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

package compatibility

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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/csiclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/manilaclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/shareadapters"
	manilautil "k8s.io/cloud-provider-openstack/pkg/csi/manila/util"
	"k8s.io/klog"
)

type cephfsTarget struct {
	share         *shares.Share
	volumeContext map[string]string
	stageSecrets  map[string]string
}

type CephfsCreateShareFromSnapshot struct{}

// Supplements create_share_from_snapshot_support capability for CephFS snapshots.
// Mounts the destination share (i.e. where the snapshot is to be restored into)
// and source share (retrieved from the snapshot). Once successfully mounted,
// the data from source is copied over to destination using rsync.
func (c CephfsCreateShareFromSnapshot) SupplementCapability(compatOpts *options.CompatibilityOptions, dstShare *shares.Share, dstAccessRight *shares.AccessRight, req *csi.CreateVolumeRequest, fwdEndpoint string, manilaClient manilaclient.Interface, csiClientBuilder csiclient.Builder) error {
	snapID := req.GetVolumeContentSource().GetSnapshot().GetSnapshotId()
	ctx := context.Background()

	// Validation

	if err := c.validateCompatibilityOptions(compatOpts); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if val, ok := dstShare.Metadata[CreatedFromSnapshotTag]; ok {
		// The dst share has been already processed. Make sure it's healthy.

		if val == snapID {
			// We're done
			return nil
		}

		if val == CreatedFromSnapshotFailed {
			return status.Errorf(codes.FailedPrecondition, "failed to restore snapshot %s into share %s, the operation is in an unrecoverable error state and the share needs manual intervention", snapID, dstShare.ID)
		}

		if val != CreatedFromSnapshotPending {
			return status.Errorf(codes.AlreadyExists, "source snapshot ID mismatch: wanted %s, got %s", val, snapID)
		}
	} else {
		// Otherwise mark it with CreatedFromSnapshotPending

		_, err := manilaClient.SetShareMetadata(dstShare.ID, &shares.SetMetadataOpts{
			Metadata: map[string]string{
				CreatedFromSnapshotTag: CreatedFromSnapshotPending,
			},
		})

		if err != nil {
			return status.Errorf(codes.Internal, "failed to set metadata on share %s: %v", dstShare.ID, err)
		}
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

	src := cephfsTarget{share: srcShare}
	dst := cephfsTarget{share: dstShare}

	if src.volumeContext, src.stageSecrets, err = c.buildVolumeContext(srcShare.ID, &srcAccessRights[0], manilaClient); err != nil {
		return status.Errorf(codes.Internal, "failed to build volume context for source share %s: %v", srcShare.ID, err)
	}

	if dst.volumeContext, dst.stageSecrets, err = c.buildVolumeContext(dstShare.ID, dstAccessRight, manilaClient); err != nil {
		return status.Errorf(codes.Internal, "failed to build volume context for destination share %s: %v", dstShare.ID, err)
	}

	// Establish connection with the fwd nodeplugin

	csiConn, err := csiClientBuilder.NewConnectionWithContext(ctx, fwdEndpoint)
	if err != nil {
		return status.Error(codes.Unavailable, fmt.Sprintf("connecting to fwd plugin at %s failed: %v", fwdEndpoint, err))
	}

	nodeClient := csiClientBuilder.NewNodeServiceClient(csiConn)

	// With exponential back-off do: mount the shares and run rsync

	retries, _ := strconv.Atoi(compatOpts.CreateShareFromSnapshotRetries)
	backoffInterval, _ := strconv.Atoi(compatOpts.CreateShareFromSnapshotBackoffInterval)

	backoff := wait.Backoff{
		Duration: time.Second * time.Duration(backoffInterval),
		Factor:   1.5,
		Steps:    retries,
	}

	wait.ExponentialBackoff(backoff, func() (bool, error) {
		if err = c.mountAndRsync(ctx, &src, &dst, snapID, compatOpts, nodeClient); err != nil {
			klog.Errorf("failed to restore snapshot %s into share %s: %v", snapID, dstShare.ID, err)
			err = status.Error(codes.Internal, err.Error())

			return false, nil
		}

		return true, nil
	})

	// We're done, set dst share metadata

	var createShareFromSnapResult string

	if err == nil {
		createShareFromSnapResult = snapID
	} else {
		createShareFromSnapResult = CreatedFromSnapshotFailed
		klog.Errorf("failed to restore snapshot %s into share %s, retry threshold reached, marking the share as failed", snapID, srcShare.ID)
	}

	_, setMetadataErr := manilaClient.SetShareMetadata(dstShare.ID, shares.SetMetadataOpts{
		Metadata: map[string]string{
			CreatedFromSnapshotTag: createShareFromSnapResult,
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

func (c CephfsCreateShareFromSnapshot) validateCompatibilityOptions(compatOpts *options.CompatibilityOptions) error {
	if compatOpts.CreateShareFromSnapshotCephFSMounts == "" {
		return errors.New("compatibility option CreateShareFromSnapshotCephFSMounts is not set")
	}

	return nil
}

func (c CephfsCreateShareFromSnapshot) stagingPath(share *shares.Share, compatOpts *options.CompatibilityOptions) string {
	return path.Join(compatOpts.CreateShareFromSnapshotCephFSMounts, share.ID)
}

func (c CephfsCreateShareFromSnapshot) buildVolumeContext(shareID string, accessRight *shares.AccessRight, manilaClient manilaclient.Interface) (volCtx, nodeStageSecret map[string]string, err error) {
	exportLocation, err := manilautil.GetChosenExportLocation(shareID, manilaClient)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve export location: %v", err)
	}

	shareAdapter := shareadapters.Cephfs{}

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

func (c CephfsCreateShareFromSnapshot) nodeStage(ctx context.Context, share *shares.Share, compatOpts *options.CompatibilityOptions, volCtx, secrets map[string]string, nodeClient csiclient.Node) error {
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

	_, err := nodeClient.StageVolume(ctx, &req)

	return err
}

func (c CephfsCreateShareFromSnapshot) nodeUnstage(ctx context.Context, share *shares.Share, compatOpts *options.CompatibilityOptions, nodeClient csiclient.Node) {
	spath := c.stagingPath(share, compatOpts)

	_, err := nodeClient.UnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
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

func (c CephfsCreateShareFromSnapshot) mountAndRsync(ctx context.Context, src, dst *cephfsTarget, snapID string, compatOpts *options.CompatibilityOptions, nodeClient csiclient.Node) error {
	// Mount dst and src shares

	if err := c.nodeStage(ctx, dst.share, compatOpts, dst.volumeContext, dst.stageSecrets, nodeClient); err != nil {
		return fmt.Errorf("failed to mount destination share %s: %v", dst.share.ID, err)
	}

	defer c.nodeUnstage(ctx, dst.share, compatOpts, nodeClient)

	if err := c.nodeStage(ctx, src.share, compatOpts, src.volumeContext, src.stageSecrets, nodeClient); err != nil {
		return fmt.Errorf("failed to mount source share %s: %v", src.share.ID, err)
	}

	defer c.nodeUnstage(ctx, src.share, compatOpts, nodeClient)

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
