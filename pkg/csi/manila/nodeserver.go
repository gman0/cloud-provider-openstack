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
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/shares"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	openstack_provider "k8s.io/cloud-provider-openstack/pkg/cloudprovider/providers/openstack"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/shareadapters"
	clouderrors "k8s.io/cloud-provider-openstack/pkg/util/errors"
)

// Stores share info of a staged volume
type nodeVolumeInfo struct {
	volumeContext map[string]string
	stageSecret   map[string]string
	publishSecret map[string]string
	adapter       shareadapters.ShareAdapter
}

// Describes a volume mount managed by the Node service.
// It's used to retrieve the mapping between a volume
// and the share adapter it's managed by when unpublishing
// or unstaging the volume from a node.
// nodeMountInfo objects are stored locally as JSON files.
//
// Note:
// The storage used to store these files must be resillient
// enough to survive redeployment of the driver on the node.
// Losing this mapping will result in volume unstage/unpublish
// failure.
// For Kubernetes, using a hostPath volume is recommended to
// store these files.
type nodeMountInfo struct {
	AdapterType shareadapters.ShareAdapterType
}

// Implements the CSI Node service
type nodeServer struct {
	d *Driver

	// In-memory cache of staged volumes
	nodeVolumeInfoCache    map[volumeID]nodeVolumeInfo
	nodeVolumeInfoCacheMtx sync.RWMutex
}

var (
	// Supported Node service capabilities - RPC types
	nodeServiceCapsTypes = []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
	}

	// Supported Node service capabilities
	nodeServiceCaps []*csi.NodeServiceCapability
)

func init() {
	// Initialize `nodeServiceCaps`

	nodeServiceCaps = make([]*csi.NodeServiceCapability, len(nodeServiceCapsTypes))
	for i := range nodeServiceCapsTypes {
		nodeServiceCaps[i] = &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: nodeServiceCapsTypes[i],
				},
			},
		}
	}
}

func newNodeServer(d *Driver) *nodeServer {
	return &nodeServer{
		d:                   d,
		nodeVolumeInfoCache: make(map[volumeID]nodeVolumeInfo),
	}
}

func (ns *nodeServer) getVolumeInfo(volID volumeID, shareOpts *options.NodeVolumeContext, osOpts *openstack_provider.AuthOpts) (*nodeVolumeInfo, error) {
	ns.nodeVolumeInfoCacheMtx.Lock()
	defer ns.nodeVolumeInfoCacheMtx.Unlock()

	// Try to retrieve volume stage info from cache

	if cacheEntry, ok := ns.nodeVolumeInfoCache[volID]; ok {
		return &cacheEntry, nil
	}

	// Cache entry not found, try to query Manila:

	manilaClient, err := ns.d.ManilaClientBuilder.New(osOpts)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to create Manila v2 client: %v", err)
	}

	// Retrieve the share by its ID or name

	var share *shares.Share

	if shareOpts.ShareID != "" {
		share, err = manilaClient.GetShareByID(shareOpts.ShareID)
		if err != nil {
			if clouderrors.IsNotFound(err) {
				return nil, status.Errorf(codes.NotFound, "share %s not found: %v", shareOpts.ShareID, err)
			}

			return nil, status.Errorf(codes.Internal, "failed to retrieve share %s: %v", shareOpts.ShareID, err)
		}
	} else {
		share, err = manilaClient.GetShareByName(shareOpts.ShareName)
		if err != nil {
			if clouderrors.IsNotFound(err) {
				return nil, status.Errorf(codes.NotFound, "no share named %s found: %v", shareOpts.ShareName, err)
			}

			return nil, status.Errorf(codes.Internal, "failed to retrieve share named %s: %v", shareOpts.ShareName, err)
		}
	}

	// Get a share adapter for this share

	adapter, err := shareadapters.Find(share.ShareProto, ns.d.enabledAdaptersWithNodePlugins)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to find an appropriate adapter for volume %s (share ID %s): %v", volID, share.ID, err)
	}

	// Check share status. Fail if it's not 'available'.

	if share.Status != shareAvailable {
		if share.Status == shareCreating {
			return nil, status.Errorf(codes.Unavailable, "share %s for volume %s is in transient creating state", share.ID, volID)
		}

		return nil, status.Errorf(codes.FailedPrecondition, "invalid share status for volume %s (share ID %s): expected 'available', got '%s'",
			volID, share.ID, share.Status)
	}

	// List access rights for this share.
	// Choose the one with matching ID from the volume parameters.

	accessRights, err := manilaClient.GetAccessRights(share.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list access rights for volume %s (share ID %s): %v",
			volID, share.ID, err)
	}

	var accessRight *shares.AccessRight

	for i := range accessRights {
		if accessRights[i].ID == shareOpts.ShareAccessID {
			accessRight = &accessRights[i]
			break
		}
	}

	if accessRight == nil {
		return nil, status.Errorf(codes.InvalidArgument, "cannot find access right %s for volume %s (share ID %s)",
			shareOpts.ShareAccessID, volID, share.ID)
	}

	// Retrieve list of all export locations for this share.
	// Share adapter will try to choose the correct one for mounting.

	availableExportLocations, err := manilaClient.GetExportLocations(share.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list export locations for volume %s: %v", volID, err)
	}

	// Build volume context

	volumeContext, err := adapter.BuildVolumeContext(&shareadapters.VolumeContextArgs{Locations: availableExportLocations, Options: shareOpts})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to build volume context for volume %s: %v", volID, err)
	}

	// Build volume stage secret

	stageSecret, err := adapter.BuildNodeStageSecret(&shareadapters.SecretArgs{AccessRight: accessRight})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to build volume stage secret for volume %s: %v", volID, err)
	}

	// Build volume publish secret

	publishSecret, err := adapter.BuildNodePublishSecret(&shareadapters.SecretArgs{AccessRight: accessRight})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to build volume publish secret for volume %s: %v", volID, err)
	}

	// Cache the results

	entry := nodeVolumeInfo{
		volumeContext: volumeContext,
		stageSecret:   stageSecret,
		publishSecret: publishSecret,
		adapter:       adapter,
	}

	ns.nodeVolumeInfoCache[volID] = entry

	return &entry, nil
}

// Writes mount info into `mountInfoDir`
func writeNodeMountInfo(volID volumeID, volInfo *nodeVolumeInfo, mountInfoDir string) error {
	if mountInfoDir == "" {
		return errors.New("path to mount info directory not specified, --mount-info-dir flag must not be empty")
	}

	jsonData, err := json.Marshal(&nodeMountInfo{
		AdapterType: volInfo.adapter.Type(),
	})

	if err != nil {
		return err
	}

	if err = ioutil.WriteFile(path.Join(mountInfoDir, string(volID)), jsonData, 0600); err != nil {
		return err
	}

	return nil
}

// Reads mount info from `mountInfoDir`.
// Returns (nil, nil) to signal that there is no mount info associated with the supplied `volID`.
func readNodeMountInfo(volID volumeID, mountInfoDir string) (*nodeMountInfo, error) {
	if mountInfoDir == "" {
		return nil, errors.New("path to mount info directory not specified, --mount-info-dir flag must not be empty")
	}

	jsonData, err := ioutil.ReadFile(path.Join(mountInfoDir, string(volID)))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var mountInfo *nodeMountInfo
	if err = json.Unmarshal(jsonData, &mountInfo); err != nil {
		return nil, err
	}

	return mountInfo, nil
}

func forgetNodeMountInfo(volID volumeID, mountInfoDir string) {
	// Deliberately ignore errors reported by os.Remove
	os.Remove(path.Join(mountInfoDir, string(volID)))
}

// Retrieves share adapter from mount info.
// Returns (nil, nil) when there is no mount info associated with the supplied `volID`.
func getShareAdapterFromNodeMountInfo(volID volumeID, mountInfoDir string, enabledAdapters map[shareadapters.ShareAdapterType]shareadapters.ShareAdapter,
) (shareadapters.ShareAdapter, error) {
	mountInfo, err := readNodeMountInfo(volID, mountInfoDir)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read mount info for volume %s: %v", volID, err)
	}

	if mountInfo == nil && err == nil {
		return nil, nil
	}

	adapter, ok := enabledAdapters[mountInfo.AdapterType]
	if !ok {
		return nil, status.Errorf(codes.Internal, "mount info contains invalid adapter type %d (name %s)",
			mountInfo.AdapterType, shareadapters.ShareAdapterTypeNameMap[mountInfo.AdapterType])
	}

	return adapter, nil
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if err := validateNodePublishVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Configuration

	shareOpts, err := options.NewNodeVolumeContext(req.GetVolumeContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume context: %v", err)
	}

	osOpts, err := options.NewOpenstackOptions(req.GetSecrets())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid OpenStack secrets: %v", err)
	}

	// Retrieve volume info

	volInfo, err := ns.getVolumeInfo(volumeID(req.GetVolumeId()), shareOpts, osOpts)
	if err != nil {
		return nil, err
	}

	// Forward the RPC

	conn, err := ns.d.CSIClientBuilder.NewConnectionWithContext(ctx, volInfo.adapter.GetCSINodePluginInfo().Endpoint)
	if err != nil {
		return nil, status.Error(codes.Unavailable, fmtGrpcConnError(volInfo.adapter.GetCSINodePluginInfo().Endpoint, err))
	}

	defer conn.Close()

	return ns.d.CSIClientBuilder.NewNodeServiceClient(conn).PublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          req.GetVolumeId(),
		StagingTargetPath: req.GetStagingTargetPath(),
		TargetPath:        req.GetTargetPath(),
		VolumeCapability:  req.GetVolumeCapability(),
		Readonly:          req.GetReadonly(),
		Secrets:           volInfo.publishSecret,
		VolumeContext:     volInfo.volumeContext,
	})
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if err := validateNodeUnpublishVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Read mount info. We need to find out what share adapter manages that mount.

	adapter, err := getShareAdapterFromNodeMountInfo(volumeID(req.GetVolumeId()), ns.d.MountInfoDir, ns.d.enabledAdaptersWithNodePlugins)
	if err != nil {
		return nil, err
	}

	if adapter == nil {
		return nil, status.Errorf(codes.Internal, "mount info for volume %s not found", req.GetVolumeId())
	}

	// Forward the RPC

	conn, err := ns.d.CSIClientBuilder.NewConnectionWithContext(ctx, adapter.GetCSINodePluginInfo().Endpoint)
	if err != nil {
		return nil, status.Error(codes.Unavailable, fmtGrpcConnError(adapter.GetCSINodePluginInfo().Endpoint, err))
	}

	defer conn.Close()

	return ns.d.CSIClientBuilder.NewNodeServiceClient(conn).UnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   req.GetVolumeId(),
		TargetPath: req.GetTargetPath(),
	})
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if err := validateNodeStageVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Configuration

	shareOpts, err := options.NewNodeVolumeContext(req.GetVolumeContext())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume context: %v", err)
	}

	osOpts, err := options.NewOpenstackOptions(req.GetSecrets())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid OpenStack secrets: %v", err)
	}

	var (
		volID = volumeID(req.GetVolumeId())
		resp  = &csi.NodeStageVolumeResponse{}
	)

	// Retrieve volume info

	volInfo, err := ns.getVolumeInfo(volID, shareOpts, osOpts)
	if err != nil {
		return nil, err
	}

	// Persist the mount info

	if err = writeNodeMountInfo(volID, volInfo, ns.d.MountInfoDir); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write node mount info file: %v", err)
	}
	defer func() {
		// Clean up the mount info we've just stored in case of an error
		if err != nil {
			forgetNodeMountInfo(volID, ns.d.MountInfoDir)
		}
	}()

	// Probe for STAGE_UNSTAGE_VOLUME capability of the partner Node Plugin

	if _, ok := volInfo.adapter.GetCSINodePluginInfo().Capabilities[csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME]; ok {
		// The Node Plugin associated with this share adapter supports STAGE_UNSTAGE_VOLUME capability.
		// Forward this RPC to the associated plugin.

		conn, err := ns.d.CSIClientBuilder.NewConnectionWithContext(ctx, volInfo.adapter.GetCSINodePluginInfo().Endpoint)
		if err != nil {
			return nil, status.Error(codes.Unavailable, fmtGrpcConnError(volInfo.adapter.GetCSINodePluginInfo().Endpoint, err))
		}

		defer conn.Close()

		resp, err = ns.d.CSIClientBuilder.NewNodeServiceClient(conn).StageVolume(ctx, &csi.NodeStageVolumeRequest{
			VolumeId:          req.GetVolumeId(),
			StagingTargetPath: req.GetStagingTargetPath(),
			VolumeCapability:  req.GetVolumeCapability(),
			VolumeContext:     volInfo.volumeContext,
			Secrets:           volInfo.stageSecret,
		})
	}

	return resp, err
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if err := validateNodeUnstageVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var (
		volID = volumeID(req.GetVolumeId())
		resp  = &csi.NodeUnstageVolumeResponse{}
	)

	// Read mount info. We need to find out what share adapter manages that mount.

	adapter, err := getShareAdapterFromNodeMountInfo(volID, ns.d.MountInfoDir, ns.d.enabledAdaptersWithNodePlugins)
	if err != nil {
		return nil, err
	}

	if adapter == nil {
		// getShareAdapterFromNodeMountInfo returned (nil, nil).
		// This means the mount info for this volume was not found (ENOENT).
		// To maintain idempotence, we'll assume the mount info has been
		// already removed by previous successful NodeUnstageVolume call.
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	// Probe for STAGE_UNSTAGE_VOLUME capability of the partner Node Plugin

	if _, ok := adapter.GetCSINodePluginInfo().Capabilities[csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME]; ok {
		// The Node Plugin associated with this share adapter supports STAGE_UNSTAGE_VOLUME capability.
		// Forward this RPC to the associated plugin.

		conn, err := ns.d.CSIClientBuilder.NewConnectionWithContext(ctx, adapter.GetCSINodePluginInfo().Endpoint)
		if err != nil {
			return nil, status.Error(codes.Unavailable, fmtGrpcConnError(adapter.GetCSINodePluginInfo().Endpoint, err))
		}

		defer conn.Close()

		resp, err = ns.d.CSIClientBuilder.NewNodeServiceClient(conn).UnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
			VolumeId:          req.GetVolumeId(),
			StagingTargetPath: req.GetStagingTargetPath(),
		})
	}

	if err == nil {
		// We can safely clean volume info and mount info caches

		ns.nodeVolumeInfoCacheMtx.Lock()
		delete(ns.nodeVolumeInfoCache, volumeID(req.VolumeId))
		ns.nodeVolumeInfoCacheMtx.Unlock()

		forgetNodeMountInfo(volumeID(req.GetVolumeId()), ns.d.MountInfoDir)
	}

	return resp, err
}

func (ns *nodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	nodeInfo := &csi.NodeGetInfoResponse{
		NodeId: ns.d.NodeID,
	}

	if ns.d.WithTopology {
		nodeInfo.AccessibleTopology = &csi.Topology{
			Segments: map[string]string{topologyKey: ns.d.NodeAZ},
		}
	}

	return nodeInfo, nil
}

func (ns *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: nodeServiceCaps,
	}, nil
}

func (ns *nodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
