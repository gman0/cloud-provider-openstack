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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/shares"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/manilaclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/shareadapters"
	"k8s.io/klog"
)

type compatibilityHandler interface {
	supplementCapability(*Driver, *shares.Share, *shares.AccessRight, *csi.CreateVolumeRequest, manilaclient.Interface) error
}

// Certain share protocols may not support certain Manila capabilities
// in a given share type. This map forms a compatibility layer which
// fills in the feature gap with in-driver functionality.
var compatHandlers = map[string]map[manilaCapability]compatibilityHandler{
	"CEPHFS": {
		manilaCapabilityShareFromSnapshot: &compatCephfsCreateShareFromSnapshot{},
	},
}

const (
	compatSnapshotTag          = "manila.csi.openstack.org/created-from-snapshot"
	compatSnapshotFailureValue = "FAILED"
)

func useCompatibilityMode(shareProto string, wantsCap manilaCapability, shareTypeCaps manilaCapabilities) compatibilityHandler {
	if handlers, ok := compatHandlers[shareProto]; ok {
		if hasCapability := shareTypeCaps[wantsCap]; !hasCapability {
			if compatCapability, ok := handlers[wantsCap]; ok {
				return compatCapability
			}
		}
	}

	return nil
}

type compatCephfsCreateShareFromSnapshot struct{}

type compatCephfsTarget struct {
	share         *shares.Share
	volumeContext map[string]string
	stageSecrets  map[string]string
}

func (c *compatCephfsCreateShareFromSnapshot) validateCompatibilityOptions(compatOpts *options.CompatibilityOptions) error {
	if compatOpts.CreateShareFromSnapshotCephFSMounts == "" {
		return errors.New("compatibility option CreateShareFromSnapshotCephFSMounts is not set")
	}

	return nil
}

func (c *compatCephfsCreateShareFromSnapshot) supplementCapability(d *Driver, dstShare *shares.Share, dstAccessRight *shares.AccessRight, req *csi.CreateVolumeRequest, manilaClient manilaclient.Interface) error {
	snapID := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()

	// Validation

	if err := c.validateCompatibilityOptions(d.compatOpts); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if val, ok := dstShare.Metadata[compatSnapshotTag]; ok {
		// The dst share has been already processed. Make sure it's healthy.

		if val == compatSnapshotFailureValue {
			return status.Errorf(codes.FailedPrecondition, "failed to restore snapshot %s into share %s, the operation is in an unrecoverable error state and the share needs manual intervention", snapID, dstShare.ID)
		}

		if val != snapID {
			return status.Errorf(codes.AlreadyExists, "source snapshot ID mismatch: wanted %s, got %s", val, snapID)
		}

		return nil
	}

	// Retrieve the source share

	srcSnapshot, err := manilaClient.GetSnapshotByID(snapID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to retrieve source snapshot %s: %v", snapID, err)
	}

	srcShare, err := manilaClient.GetShareByID(srcSnapshot.ShareID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to retrieve source share %s from snapshot %s: %v", srcSnapshot.ShareID, snapID, err)
	}

	// Retrieve an access right for the source share.
	// Read-only access is sufficient.

	srcAccessRights, err := manilaClient.GetAccessRights(srcShare.ID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to list access rights for share %s: %v", srcShare.ID, err)
	}

	if srcAccessRights == nil || len(srcAccessRights) == 0 {
		return status.Errorf(codes.Internal, "no access rights for share %s", srcShare.ID)
	}

	// Build volume contexts and staging secrets

	src := compatCephfsTarget{share: srcShare}
	dst := compatCephfsTarget{share: dstShare}

	if src.volumeContext, src.stageSecrets, err = c.buildVolumeContext(srcShare.ID, &srcAccessRights[0], manilaClient); err != nil {
		return status.Errorf(codes.Internal, "failed to build volume context for source share %s: %v", srcShare.ID, err)
	}

	if dst.volumeContext, dst.stageSecrets, err = c.buildVolumeContext(dstShare.ID, dstAccessRight, manilaClient); err != nil {
		return status.Errorf(codes.Internal, "failed to build volume context for destination share %s: %v", dstShare.ID, err)
	}

	// Establish connection with the fwd nodeplugin

	csiConn, err := grpcConnect(nil, d.fwdEndpoint)
	if err != nil {
		return status.Error(codes.Unavailable, fmtGrpcConnError(d.fwdEndpoint, err))
	}

	// With exponential back-off do: mount the shares and run rsync

	retries, _ := strconv.Atoi(d.compatOpts.CreateShareFromSnapshotRetries)
	backoffInterval, _ := strconv.Atoi(d.compatOpts.CreateShareFromSnapshotBackoffInterval)

	backoff := wait.Backoff{
		Duration: time.Second * time.Duration(backoffInterval),
		Factor:   1.5,
		Steps:    retries,
	}

	wait.ExponentialBackoff(backoff, func() (bool, error) {
		if err = c.mountAndRsync(&src, &dst, snapID, d.compatOpts, csiConn, manilaClient); err != nil {
			klog.Errorf("failed to restore snapshot %s into share %s: %v", snapID, dstShare.ID, err)
			err = status.Error(codes.Internal, err.Error())

			return false, nil
		}

		return true, nil
	})

	// We're done, set dst share metadata

	var createFromShareResult string

	if err == nil {
		createFromShareResult = snapID
	} else {
		createFromShareResult = compatSnapshotFailureValue
		klog.Errorf("failed to restore snapshot %s into share %s, retry threshold reached, marking the share as failed", snapID, srcShare.ID)
	}

	_, setMetadataErr := manilaClient.SetShareMetadata(dstShare.ID, shares.SetMetadataOpts{
		Metadata: map[string]string{
			compatSnapshotTag: createFromShareResult,
		},
	})

	if setMetadataErr != nil {
		logMsg := fmt.Sprintf("failed to set metadata on share %s: %v", dstShare.ID, setMetadataErr)
		if err == nil {
			return status.Error(codes.Internal, logMsg)
		}

		klog.Error(logMsg)
	}

	return err
}

func (c *compatCephfsCreateShareFromSnapshot) stagingPath(share *shares.Share, compatOpts *options.CompatibilityOptions) string {
	return path.Join(compatOpts.CreateShareFromSnapshotCephFSMounts, share.ID)
}

func (c *compatCephfsCreateShareFromSnapshot) buildVolumeContext(shareID string, accessRight *shares.AccessRight, manilaClient manilaclient.Interface) (volCtx, nodeStageSecret map[string]string, err error) {
	exportLocation, err := getChosenExportLocation(shareID, manilaClient)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve export location: %v", err)
	}

	shareAdapter := getShareAdapter("CEPHFS")

	volCtx, err = shareAdapter.BuildVolumeContext(&shareadapters.VolumeContextArgs{
		Location: exportLocation,
		Options: &options.NodeVolumeContext{
			ShareID:       shareID,
			ShareAccessID: accessRight.ID,
			CephfsMounter: "kernel",
		},
	})

	if err != nil {
		return nil, nil, err
	}

	nodeStageSecret, err = shareAdapter.BuildNodeStageSecret(&shareadapters.SecretArgs{
		AccessRight: accessRight,
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to build node stage secrets: %v", err)
	}

	return
}

func (c *compatCephfsCreateShareFromSnapshot) nodeStage(share *shares.Share, compatOpts *options.CompatibilityOptions, volCtx, secrets map[string]string, csiConn *grpc.ClientConn) error {
	spath := c.stagingPath(share, compatOpts)

	if err := os.Mkdir(spath, 0777); err != nil {
		return fmt.Errorf("failed to create staging directory in %s: %v", spath, err)
	}

	req := csi.NodeStageVolumeRequest{
		VolumeId:          share.ID,
		VolumeContext:     volCtx,
		Secrets:           secrets,
		StagingTargetPath: spath,
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		},
	}

	_, err := csiNodeStageVolume(context.Background(), csiConn, &req)

	return err
}

func (c *compatCephfsCreateShareFromSnapshot) nodeUnstage(share *shares.Share, compatOpts *options.CompatibilityOptions, csiConn *grpc.ClientConn) {
	spath := c.stagingPath(share, compatOpts)

	_, err := csiNodeUnstageVolume(context.Background(), csiConn, &csi.NodeUnstageVolumeRequest{
		VolumeId:          share.ID,
		StagingTargetPath: spath,
	})

	if err != nil {
		klog.Errorf("couldn't unstage a volume from %s: %v", spath, err)
		return
	}

	if err := os.Remove(spath); err != nil {
		if !os.IsNotExist(err) {
			klog.Errorf("couldn't delete %s: %v", spath, err)
		}
	}
}

func (c *compatCephfsCreateShareFromSnapshot) mountAndRsync(src, dst *compatCephfsTarget, snapID string, compatOpts *options.CompatibilityOptions, csiConn *grpc.ClientConn, manilaClient manilaclient.Interface) error {
	// Mount dst and src shares

	if err := c.nodeStage(dst.share, compatOpts, dst.volumeContext, dst.stageSecrets, csiConn); err != nil {
		return fmt.Errorf("failed to mount destination share %s: %v", dst.share.ID, err)
	}

	defer c.nodeUnstage(dst.share, compatOpts, csiConn)

	if err := c.nodeStage(src.share, compatOpts, src.volumeContext, src.stageSecrets, csiConn); err != nil {
		return fmt.Errorf("failed to mount source share %s: %v", src.share.ID, err)
	}

	defer c.nodeUnstage(src.share, compatOpts, csiConn)

	// Find the snapshot location

	var snapName string

	if dir, err := ioutil.ReadDir(path.Join(c.stagingPath(src.share, compatOpts), ".snap")); err != nil {
		return err
	} else {
		for _, ent := range dir {
			if strings.HasPrefix(ent.Name(), snapID) {
				snapName = ent.Name()
				break
			}
		}
	}

	if snapName == "" {
		return fmt.Errorf("couldn't find path for snapshot %s", snapID)
	}

	// Setup logging

	var logWriter io.Writer

	if logErrorsToFile, _ := strconv.ParseBool(compatOpts.CreateShareFromSnapshotCephFSLogErrorsToFile); logErrorsToFile {
		logPath := fmt.Sprintf("%s/rsync-errors-%s.txt", c.stagingPath(dst.share, compatOpts), snapID)

		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0655)
		if err != nil {
			return fmt.Errorf("couldn't create log file in %s: %v", logPath, err)
		}

		defer func() {
			pos, _ := f.Seek(0, io.SeekCurrent)
			f.Close()

			if pos == 0 {
				// We haven't wrote anything, error log is empty and may be deleted
				os.Remove(logPath)
			}
		}()

		logWriter = bufio.NewWriter(f)
	} else {
		buf := &bytes.Buffer{}
		logWriter = buf

		defer func() {
			if b := buf.Bytes(); b != nil {
				klog.Errorf("rsync error output (source snapshot %s, destination share %s):\n%s", snapID, dst.share.ID, b)
			}
		}()
	}

	// Run rsync

	cmd := exec.Command("rsync",
		"--recursive", "--perms", "--links", "--acls", "--partial-dir=.rsync-partial",
		fmt.Sprintf("%s/.snap/%s/", c.stagingPath(src.share, compatOpts), snapName),
		c.stagingPath(dst.share, compatOpts),
	)

	cmd.Stderr = logWriter

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("rsync failed: %v", err)
	}

	return nil
}
