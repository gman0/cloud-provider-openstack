/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package manila

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/messages"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/shares"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/sharetypes"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/snapshots"
	"google.golang.org/grpc/codes"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/manilaclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/responsebroker"
	"k8s.io/klog"
)

type (
	volumeID   string
	snapshotID string
)

const (
	bytesInGiB = 1024 * 1024 * 1024
)

type manilaError int

func (c manilaError) toRpcErrorCode() codes.Code {
	switch c {
	case manilaErrNoValidHost:
		return codes.OutOfRange
	case manilaErrUnexpectedNetwork:
		return codes.InvalidArgument
	case manilaErrAvailability:
		return codes.ResourceExhausted
	case manilaErrCapabilities:
		return codes.InvalidArgument
	case manilaErrCapacity:
		return codes.OutOfRange
	default:
		return codes.Internal
	}
}

const (
	manilaErrNoValidHost manilaError = iota + 1
	manilaErrUnexpectedNetwork
	manilaErrAvailability
	manilaErrCapabilities
	manilaErrCapacity
)

var (
	manilaErrorCodesMap = map[string]manilaError{
		"002": manilaErrNoValidHost,
		"003": manilaErrUnexpectedNetwork,
		"007": manilaErrAvailability,
		"008": manilaErrCapabilities,
		"009": manilaErrCapacity,
	}
)

type manilaErrorMessage struct {
	errCode manilaError
	message string
}

func parseGRPCEndpoint(endpoint string) (proto, addr string, err error) {
	const (
		unixScheme = "unix://"
		tcpScheme  = "tcp://"
	)

	if strings.HasPrefix(endpoint, "/") {
		return "unix", endpoint, nil
	}

	if strings.HasPrefix(endpoint, unixScheme) {
		pos := len(unixScheme)
		if endpoint[pos] != '/' {
			// endpoint seems to be "unix://absolute/path/to/somewhere"
			// we're missing one '/'...compensate by decrementing pos
			pos--
		}

		return "unix", endpoint[pos:], nil
	}

	if strings.HasPrefix(endpoint, tcpScheme) {
		return "tcp", endpoint[len(tcpScheme):], nil
	}

	return "", "", errors.New("endpoint uses unsupported scheme")
}

// Blocks until the response from previous request is available and reads it.
// If that request has finished successfully, release the handle because we're done.
func readResponse(handle responsebroker.ResponseHandle) interface{} {
	if resp, err := handle.Read(); err == nil {
		handle.Release()
		return resp
	}

	return nil
}

type requestResult struct {
	dataPtr interface{}
	err     error
}

// Writes the response.
// If this request has finished successfully, wait for others to readResponse() and dispose of the lock.
func writeResponse(handle responsebroker.ResponseHandle, rb *responsebroker.ResponseBroker, identifier string, res *requestResult) {
	handle.Write(res.dataPtr, res.err)

	if res.err == nil {
		rb.Done(identifier)
	}
}

func endpointAddress(proto, addr string) string {
	return fmt.Sprintf("%s://%s", proto, addr)
}

func fmtGrpcConnError(fwdEndpoint string, err error) string {
	return fmt.Sprintf("connecting to fwd plugin at %s failed: %v", fwdEndpoint, err)
}

func bytesToGiB(sizeInBytes int64) int {
	sizeInGiB := int(sizeInBytes / bytesInGiB)

	if int64(sizeInGiB)*bytesInGiB < sizeInBytes {
		// Round up
		return sizeInGiB + 1
	}

	return sizeInGiB
}

func lastResourceError(resourceID string, manilaClient manilaclient.Interface) (manilaErrorMessage, error) {
	msgs, err := manilaClient.GetUserMessages(&messages.ListOpts{
		ResourceID:   resourceID,
		MessageLevel: "ERROR",
		Limit:        1,
		SortDir:      "desc",
		SortKey:      "created_at",
	})

	if err != nil {
		return manilaErrorMessage{}, err
	}

	if msgs != nil && len(msgs) == 1 {
		return manilaErrorMessage{errCode: manilaErrorCodesMap[msgs[0].DetailID], message: msgs[0].UserMessage}, nil
	}

	return manilaErrorMessage{message: "unknown error"}, nil
}

func isManilaErrNotFound(err error) bool {
	if err == nil {
		return false
	}

	if _, isDefault404 := err.(gophercloud.ErrDefault404); isDefault404 {
		return true
	}

	if _, isResourceNotFound := err.(gophercloud.ErrResourceNotFound); isResourceNotFound {
		return true
	}

	return false
}

func compareProtocol(protoA, protoB string) bool {
	return strings.ToUpper(protoA) == strings.ToUpper(protoB)
}

func shareTypeGetExtraSpecs(shareType string, manilaClient manilaclient.Interface) (sharetypes.ExtraSpecs, error) {
	extraSpecs, err := manilaClient.GetExtraSpecs(shareType)

	if isManilaErrNotFound(err) {
		// Maybe shareType is share type name, try to get its ID

		id, err := manilaClient.GetShareTypeIDFromName(shareType)
		if err != nil {
			if isManilaErrNotFound(err) {
				return extraSpecs, err
			}

			return extraSpecs, fmt.Errorf("failed to get share type ID for share type %s: %v", shareType, err)
		}

		return manilaClient.GetExtraSpecs(id)
	}

	return extraSpecs, err
}

// Chooses one ExportLocation according to the below rules:
// 1. Path is not empty
// 2. IsAdminOnly == false
// 3. Preferred == true are preferred over Preferred == false
// 4. Locations with lower slice index are preferred over locations with higher slice index
func chooseExportLocation(locs []shares.ExportLocation) (shares.ExportLocation, error) {
	if len(locs) == 0 {
		return shares.ExportLocation{}, fmt.Errorf("export locations list is empty")
	}

	var (
		foundMatchingNotPreferred = false
		matchingNotPreferred      shares.ExportLocation
	)

	for _, loc := range locs {
		if loc.IsAdminOnly || strings.TrimSpace(loc.Path) == "" {
			continue
		}

		if loc.Preferred {
			return loc, nil
		}

		if !foundMatchingNotPreferred {
			matchingNotPreferred = loc
			foundMatchingNotPreferred = true
		}
	}

	if foundMatchingNotPreferred {
		return matchingNotPreferred, nil
	}

	return shares.ExportLocation{}, fmt.Errorf("cannot find any non-admin export location")
}

func getChosenExportLocation(shareID string, manilaClient manilaclient.Interface) (*shares.ExportLocation, error) {
	availableExportLocations, err := manilaClient.GetExportLocations(shareID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve export locations for share %s: %v", shareID, err)
	}

	chosenExportLocation, err := chooseExportLocation(availableExportLocations)
	if err != nil {
		return nil, fmt.Errorf("failed to choose an export location for share %s: %v", shareID, err)
	}

	return &chosenExportLocation, err
}

func getAccessRightByID(shareID, accessRightID string, manilaClient manilaclient.Interface) (*shares.AccessRight, error) {
	accessRights, err := manilaClient.GetAccessRights(shareID)
	if err != nil {
		return nil, fmt.Errorf("failed to list access rights for share %s: %v", shareID, err)
	}

	for i := range accessRights {
		if accessRights[i].ID == accessRightID {
			return &accessRights[i], nil
		}
	}

	return nil, fmt.Errorf("no access right %s for share %s found", accessRightID, shareID)
}

func tryGetCompatibilityMode(compatOpts *options.CompatibilityOptions, shareOpts *options.ControllerVolumeContext, source *csi.VolumeContentSource, shareTypeCaps manilaCapabilities) compatibilityHandler {
	if source != nil {
		if source.GetSnapshot() != nil && shareTypeCaps[manilaCapabilitySnapshot] {
			if createShareFromSnapshotEnabled, _ := strconv.ParseBool(compatOpts.CreateShareFromSnapshotEnabled); createShareFromSnapshotEnabled {
				return useCompatibilityMode(shareOpts.Protocol, manilaCapabilityShareFromSnapshot, shareTypeCaps)
			}
		}
	}

	return nil
}

//
// Controller service request validation
//

func validateCreateVolumeRequest(req *csi.CreateVolumeRequest) error {
	if req.GetName() == "" {
		return errors.New("volume name cannot be empty")
	}

	reqCaps := req.GetVolumeCapabilities()
	if reqCaps == nil {
		return errors.New("volume capabilities cannot be empty")
	}

	for _, cap := range reqCaps {
		if cap.GetBlock() != nil {
			return errors.New("block access type not allowed")
		}
	}

	if req.GetSecrets() == nil || len(req.GetSecrets()) == 0 {
		return errors.New("secrets cannot be nil or empty")
	}

	return nil
}

func validateDeleteVolumeRequest(req *csi.DeleteVolumeRequest) error {
	if req.GetVolumeId() == "" {
		return errors.New("volume ID cannot be empty")
	}

	if req.GetSecrets() == nil || len(req.GetSecrets()) == 0 {
		return errors.New("secrets cannot be nil or empty")
	}

	return nil
}

func validateCreateSnapshotRequest(req *csi.CreateSnapshotRequest) error {
	if req.GetName() == "" {
		return errors.New("snapshot name cannot be empty")
	}

	if req.GetSourceVolumeId() == "" {
		return errors.New("source volume ID cannot be empty")
	}

	if req.GetSecrets() == nil || len(req.GetSecrets()) == 0 {
		return errors.New("secrets cannot be nil or empty")
	}

	if req.GetParameters() != nil || len(req.GetParameters()) > 0 {
		klog.Info("parameters in CreateSnapshot requests are ignored")
	}

	return nil
}

func validateDeleteSnapshotRequest(req *csi.DeleteSnapshotRequest) error {
	if req.GetSnapshotId() == "" {
		return errors.New("snapshot ID cannot be empty")
	}

	if req.GetSecrets() == nil || len(req.GetSecrets()) == 0 {
		return errors.New("secrets cannot be nil or empty")
	}

	return nil
}

func verifyVolumeCompatibility(sizeInGiB int, req *csi.CreateVolumeRequest, share *shares.Share, shareOpts *options.ControllerVolumeContext, compatOpts *options.CompatibilityOptions, shareTypeCaps manilaCapabilities) error {
	coalesceValue := func(v string) string {
		if v == "" {
			return "<none>"
		}

		return v
	}

	if share.Size != sizeInGiB {
		return fmt.Errorf("size mismatch: wanted %d, got %d", share.Size, sizeInGiB)
	}

	if share.ShareProto != shareOpts.Protocol {
		return fmt.Errorf("share protocol mismatch: wanted %s, got %s", coalesceValue(share.ShareProto), coalesceValue(shareOpts.Protocol))
	}

	// FIXME shareOpts.Type may be either type name or type ID
	/*
		if share.ShareType != shareOpts.Type {
			return fmt.Errorf("share type mismatch: wanted %s, got %s", shareOpts.Type, share.ShareType)
		}
	*/

	if share.ShareNetworkID != shareOpts.ShareNetworkID {
		return fmt.Errorf("share network ID mismatch: wanted %s, got %s", coalesceValue(share.ShareNetworkID), coalesceValue(shareOpts.ShareNetworkID))
	}

	var reqSrcSnapID string
	if req.GetVolumeContentSource() != nil && req.GetVolumeContentSource().GetSnapshot() != nil {
		reqSrcSnapID = req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()
	}

	if share.SnapshotID != reqSrcSnapID {
		if tryGetCompatibilityMode(compatOpts, shareOpts, req.GetVolumeContentSource(), shareTypeCaps) == nil || share.SnapshotID != "" {
			return fmt.Errorf("source snapshot ID mismatch: wanted %s, got %s", coalesceValue(share.SnapshotID), coalesceValue(reqSrcSnapID))
		}
	}

	return nil
}

func verifySnapshotCompatibility(snapshot *snapshots.Snapshot, req *csi.CreateSnapshotRequest) error {
	if snapshot.ShareID != req.GetSourceVolumeId() {
		return fmt.Errorf("source share ID mismatch: wanted %s, got %s", snapshot.ID, req.GetSourceVolumeId())
	}

	return nil
}

func validateValidateVolumeCapabilitiesRequest(req *csi.ValidateVolumeCapabilitiesRequest) error {
	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	if req.GetVolumeCapabilities() == nil || len(req.GetVolumeCapabilities()) == 0 {
		return errors.New("volume capabilities cannot be nil or empty")
	}

	if req.GetSecrets() == nil || len(req.GetSecrets()) == 0 {
		return errors.New("stage secrets cannot be nil or empty")
	}

	return nil
}

//
// Node service request validation
//

func validateNodeStageVolumeRequest(req *csi.NodeStageVolumeRequest) error {
	if req.GetVolumeCapability() == nil {
		return errors.New("volume capability missing in request")
	}

	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	if req.GetVolumeContext() == nil || len(req.GetVolumeContext()) == 0 {
		return errors.New("volume context cannot be nil or empty")
	}

	if req.GetSecrets() == nil || len(req.GetSecrets()) == 0 {
		return errors.New("stage secrets cannot be nil or empty")
	}

	return nil
}

func validateNodeUnstageVolumeRequest(req *csi.NodeUnstageVolumeRequest) error {
	if req.GetStagingTargetPath() == "" {
		return errors.New("staging path missing in request")
	}

	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	return nil
}

func validateNodePublishVolumeRequest(req *csi.NodePublishVolumeRequest) error {
	if req.GetVolumeCapability() == nil {
		return errors.New("volume capability missing in request")
	}

	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	if req.GetVolumeContext() == nil || len(req.GetSecrets()) == 0 {
		return errors.New("volume context cannot be nil or empty")
	}

	if req.GetSecrets() == nil || len(req.GetSecrets()) == 0 {
		return errors.New("node publish secrets cannot be nil or empty")
	}

	return nil
}

func validateNodeUnpublishVolumeRequest(req *csi.NodeUnpublishVolumeRequest) error {
	if req.GetTargetPath() == "" {
		return errors.New("target path missing in request")
	}

	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	return nil
}
