/*
Copyright 2020 The Kubernetes Authors.
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

package compatibility

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/shares"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/csiclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/shareadapters"
	"k8s.io/klog/v2"
)

const (
	targetDir        = "mount"
	stagingTargetDir = "globalmount"
)

type cephfsShareInfo struct {
	volumeContext map[string]string
	stageSecret   map[string]string
	publishSecret map[string]string

	mountsPath        string
	stagingTargetPath string
	targetPath        string
}

func createShareFromSnapshotCephfs(args *CompatArgs) error {
	var (
		jobFinished bool // Used during clean up

		err      error
		srcShare *shares.Share
		srcInfo  *cephfsShareInfo
		dstInfo  *cephfsShareInfo
		dstShare = args.CreateShareFromSnapshot.DestShare
		snapID   = args.CreateShareFromSnapshot.CreateVolumeReq.GetVolumeContentSource().GetSnapshot().GetSnapshotId()
		ctx      = args.CreateShareFromSnapshot.Ctx

		adapter       = args.CreateShareFromSnapshot.Adapter
		manilaClient  = args.CreateShareFromSnapshot.ManilaClient
		csiNodeClient csiclient.Node
	)

	if adapter.GetCSINodePluginInfo().Endpoint == "" {
		return status.Errorf(codes.FailedPrecondition,
			"createShareFromSnapshotCephfs compatibility mode requires share adapter %s to have node plugin endpoint specified",
			shareadapters.ShareAdapterTypeNameMap[adapter.Type()])
	}

	if args.CreateShareFromSnapshot.CompatOpts.CreateShareFromSnapshotMountsDir == "" {
		return status.Errorf(codes.FailedPrecondition,
			"createShareFromSnapshotCephfs compatibility mode requires create-share-from-snapshot-mounts-dir to be specified",
			shareadapters.ShareAdapterTypeNameMap[adapter.Type()])
	}

	// Establish connection with the partner Node Plugin

	conn, err := args.CreateShareFromSnapshot.CsiClientBuilder.NewConnectionWithContext(ctx, adapter.GetCSINodePluginInfo().Endpoint)
	if err != nil {
		err = status.Errorf(codes.Unavailable, "failed to connect to %s: %v", adapter.GetCSINodePluginInfo().Endpoint, err)
	}
	defer conn.Close()

	csiNodeClient = args.CreateShareFromSnapshot.CsiClientBuilder.NewNodeServiceClient(conn)

	defer func() {
		// Clean up only on jobFinished

		if !jobFinished {
			return
		}

		if srcInfo != nil {
			if srcShare != nil {
				cephfsNodeUnpublishVolume(ctx, csiNodeClient, srcInfo, srcShare.ID, adapter)
				cephfsNodeUnstageVolume(ctx, csiNodeClient, srcInfo, srcShare.ID, adapter)
			}

			cephfsRmMountDirs(srcInfo)
		}

		if dstInfo != nil {
			cephfsNodeUnpublishVolume(ctx, csiNodeClient, dstInfo, dstShare.ID, adapter)
			cephfsNodeUnstageVolume(ctx, csiNodeClient, dstInfo, dstShare.ID, adapter)

			cephfsRmMountDirs(dstInfo)
		}
	}()

	// Perform share metadata validation.
	// The destination share which will be populated by the source snapshot
	// must contain manila.csi.openstack.org/created-from-snapshot metadata.
	// This supplements the `SnapshotID` field of the share and holds
	// two pieces of information:
	// (1) source snapshot ID,
	// (2) create_share_from_snapshot status: pending, failed or finished.
	// If no status is given and the metadata value contains only the snapshot ID,
	// the operation is presumed to be successfully completed, in which case we exit early.

	createdFromSnapshotMetadata, ok := dstShare.Metadata[CreatedFromSnapshotTag]
	if !ok {
		return status.Errorf(codes.FailedPrecondition, "share %s is missing %s metadata and cannot be processed with compatibility mode for create_share_from_snapshot",
			dstShare.ID, CreatedFromSnapshotTag)
	}

	if createdFromSnapshotMetadata == snapID {
		// We're done
		return nil
	}

	snapIDFromMetadata, opStatusFromMetadata := SplitCreatedFromSnapshotInfo(createdFromSnapshotMetadata)

	if snapIDFromMetadata != snapID {
		return status.Errorf(codes.AlreadyExists, "source snapshot ID mismatch: wanted %s, got %s", createdFromSnapshotMetadata, snapID)
	}

	switch opStatusFromMetadata {
	case CreatedFromSnapshotPending:
		// Continue normally and run the rsync job

	case CreatedFromSnapshotFailed:
		return status.Errorf(codes.FailedPrecondition, "failed to restore snapshot %s into share %s,"+
			"the operation is in an unrecoverable error state and the share needs manual intervention",
			snapID, dstShare.ID)

	default:
		return status.Errorf(codes.FailedPrecondition, "failed to restore snapshot %s into share %s, the operation is in an unrecoverable"+
			"error state and the share needs manual intervention: unknown status %q in share metadata %s=%s",
			snapID, dstShare.ID, opStatusFromMetadata, CreatedFromSnapshotTag, createdFromSnapshotMetadata)
	}

	// Retrieve the source share

	srcSnapshot, err := manilaClient.GetSnapshotByID(snapID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to retrieve source snapshot %s: %v", snapID, err)
	}

	srcShare, err = manilaClient.GetShareByID(srcSnapshot.ShareID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to retrieve source share %s from snapshot %s: %v", srcSnapshot.ShareID, snapID, err)
	}

	// Retrieve an access right for the source share.
	// NOTE: The first access right from the list is chosen.

	srcAccessRights, err := manilaClient.GetAccessRights(srcShare.ID)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to list access rights for share %s: %v", srcShare.ID, err)
	}

	if srcAccessRights == nil || len(srcAccessRights) == 0 {
		return status.Errorf(codes.Internal, "no access rights for share %s", srcShare.ID)
	}

	// Build volume contexts and secrets

	srcInfo, err = buildCephfsShareInfo(args, srcShare, &srcAccessRights[0])
	if err != nil {
		return err
	}

	dstInfo, err = buildCephfsShareInfo(args, dstShare, args.CreateShareFromSnapshot.DestShareAccess)
	if err != nil {
		return err
	}

	// Prepare mount dirs

	if err = cephfsMkMountDirs(srcInfo); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	if err = cephfsMkMountDirs(dstInfo); err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Mount src and dst shares

	toMount := []struct {
		info    *cephfsShareInfo
		shareID string
	}{
		{info: srcInfo, shareID: srcShare.ID},
		{info: dstInfo, shareID: dstShare.ID},
	}

	for i := range toMount {
		if err := cephfsNodeStageVolume(ctx, csiNodeClient, toMount[i].info, toMount[i].shareID, adapter); err != nil {
			return err
		}

		if err := cephfsNodePublishVolume(ctx, csiNodeClient, toMount[i].info, toMount[i].shareID, adapter); err != nil {
			return err
		}
	}

	// Get snapshot path

	snapAbsPath, err := cephfsFindSnapshotAbsPath(srcInfo.targetPath, snapID)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Run rsync job

	rsyncStderrToFile, _ := strconv.ParseBool(args.CreateShareFromSnapshot.CompatOpts.RsyncStderrToFile)
	rsyncIoTimeout, _ := strconv.Atoi(args.CreateShareFromSnapshot.CompatOpts.RsyncIoTimeout)

	jobID := dstShare.ID
	jobRes := runJob(jobID, rsyncJob, &jobArgs{
		rsyncArgs: &rsyncJobArgs{
			src:          snapAbsPath,
			dst:          dstInfo.targetPath,
			stderrToFile: rsyncStderrToFile,
			ioTimeout:    rsyncIoTimeout,
		},
	})

	select {
	case err = <-jobRes.done:
		// Job has finished

		jobFinished = true // Used during clean up
		removeJob(jobID)

	default:
		// Job is still pending, return ABORTED error code

		return status.Errorf(codes.Aborted, "share %s is still being populated with snapshot %s",
			args.CreateShareFromSnapshot.DestShare.ID,
			args.CreateShareFromSnapshot.CreateVolumeReq.GetVolumeContentSource().GetSnapshot().GetSnapshotId())
	}

	if err != nil {
		// TODO mark dst share as failed?
		return status.Errorf(codes.Internal, "rsync job failed: %v", err)
	}

	// Update manila.csi.openstack.org/created-from-snapshot
	// share metadata to mark the dest share as ready.

	_, err = manilaClient.SetShareMetadata(dstShare.ID, &shares.SetMetadataOpts{
		Metadata: map[string]string{
			CreatedFromSnapshotTag: snapID,
		},
	})

	if err != nil {
		return status.Errorf(codes.Internal, "failed to set metadata on share %s: %v", dstShare.ID, err)
	}

	return nil
}

func buildCephfsShareInfo(args *CompatArgs, share *shares.Share, accessRight *shares.AccessRight) (*cephfsShareInfo, error) {
	var (
		shareInfo cephfsShareInfo
		err       error

		adapter = args.CreateShareFromSnapshot.Adapter
	)

	// Build volume context

	exportLocations, err := args.CreateShareFromSnapshot.ManilaClient.GetExportLocations(share.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list export locations for share %s: %v", share.ID, err)
	}

	shareInfo.volumeContext, err = adapter.BuildVolumeContext(&shareadapters.VolumeContextArgs{
		Locations: exportLocations,
		Options: &options.NodeVolumeContext{
			ShareID:       share.ID,
			ShareAccessID: accessRight.ID,
			CephfsMounter: "kernel",
		},
	})

	if err != nil {
		return nil, err
	}

	// Build node stage secrets

	shareInfo.stageSecret, err = adapter.BuildNodeStageSecret(&shareadapters.SecretArgs{
		AccessRight: accessRight,
	})

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build node stage secrets for share %s: %v", share.ID, err)
	}

	// Build node publish secrets

	shareInfo.publishSecret, err = adapter.BuildNodePublishSecret(&shareadapters.SecretArgs{
		AccessRight: accessRight,
	})

	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build node publish secrets for share %s: %v", share.ID, err)
	}

	// Fill `shareInfo`

	shareInfo.mountsPath = path.Join(args.CreateShareFromSnapshot.CompatOpts.CreateShareFromSnapshotMountsDir, share.ID)
	shareInfo.stagingTargetPath = path.Join(shareInfo.mountsPath, stagingTargetDir)
	shareInfo.targetPath = path.Join(shareInfo.mountsPath, targetDir)

	return &shareInfo, nil
}

func cephfsMkMountDirs(i *cephfsShareInfo) error {
	for _, p := range []string{i.stagingTargetPath, i.targetPath} {
		if err := os.MkdirAll(p, 0777); err != nil {
			return err
		}
	}

	return nil
}

func cephfsRmMountDirs(i *cephfsShareInfo) {
	for _, p := range []string{i.stagingTargetPath, i.targetPath, i.mountsPath} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			klog.Error(err.Error())
		}
	}
}

func wrapGrpcError(err error, extraMsg string) error {
	if err == nil {
		return nil
	}

	statusErr, _ := status.FromError(err)

	return status.Errorf(statusErr.Code(), "%s: %v", extraMsg, statusErr.Message())
}

func cephfsNodeStageVolume(ctx context.Context, c csiclient.Node, i *cephfsShareInfo, shareID string, adapter shareadapters.ShareAdapter) error {
	_, err := c.StageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          shareID,
		VolumeContext:     i.volumeContext,
		Secrets:           i.stageSecret,
		StagingTargetPath: i.stagingTargetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	})

	return wrapGrpcError(err, fmt.Sprintf("NodeStageVolume from %s failed", adapter.GetCSINodePluginInfo().Endpoint))
}

func cephfsNodeUnstageVolume(ctx context.Context, c csiclient.Node, i *cephfsShareInfo, shareID string, adapter shareadapters.ShareAdapter) error {
	_, err := c.UnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
		VolumeId:          shareID,
		StagingTargetPath: i.stagingTargetPath,
	})

	return wrapGrpcError(err, fmt.Sprintf("NodeUnstageVolume from %s failed", adapter.GetCSINodePluginInfo().Endpoint))
}

func cephfsNodePublishVolume(ctx context.Context, c csiclient.Node, i *cephfsShareInfo, shareID string, adapter shareadapters.ShareAdapter) error {
	_, err := c.PublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          shareID,
		VolumeContext:     i.volumeContext,
		Secrets:           i.publishSecret,
		StagingTargetPath: i.stagingTargetPath,
		TargetPath:        i.targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	})

	return wrapGrpcError(err, fmt.Sprintf("NodePublishVolume from %s failed", adapter.GetCSINodePluginInfo().Endpoint))
}

func cephfsNodeUnpublishVolume(ctx context.Context, c csiclient.Node, i *cephfsShareInfo, shareID string, adapter shareadapters.ShareAdapter) error {
	_, err := c.UnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   shareID,
		TargetPath: i.targetPath,
	})

	return wrapGrpcError(err, fmt.Sprintf("NodeUnpublishVolume from %s failed", adapter.GetCSINodePluginInfo().Endpoint))
}

// Find the location of snapshot in srcPath.
// CephFS stores snapshots under `<subvol>/.snap`.
// We'll be searching for a directory named `<snapshot ID>*`.
func cephfsFindSnapshotAbsPath(volPath, snapID string) (string, error) {
	var (
		snapsPath   = path.Join(volPath, ".snap")
		snapDirName string
	)

	if dir, err := ioutil.ReadDir(snapsPath); err != nil {
		return "", err
	} else {
		for _, ent := range dir {
			if strings.HasPrefix(ent.Name(), snapID) {
				snapDirName = ent.Name()
				break
			}
		}
	}

	if snapDirName == "" {
		return "", fmt.Errorf("couldn't find path for snapshot %s", snapID)
	}

	return path.Join(snapsPath, snapDirName), nil
}
