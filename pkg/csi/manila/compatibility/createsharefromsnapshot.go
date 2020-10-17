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
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/gophercloud/gophercloud/openstack/sharedfilesystems/v2/shares"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/csiclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/manilaclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/shareadapters"
)

type CreateShareFromSnapshotCompatArgs struct {
	Ctx context.Context

	Adapter         shareadapters.ShareAdapter
	CreateVolumeReq *csi.CreateVolumeRequest
	CompatOpts      *options.CompatibilityOptions
	DestShare       *shares.Share
	DestShareAccess *shares.AccessRight

	ManilaClient     manilaclient.Interface
	CsiClientBuilder csiclient.Builder
}

const (
	// Manila share metadata key for shares created from snapshots.
	// This supplements share's SnapshotID field.
	CreatedFromSnapshotTag = "manila.csi.openstack.org/created-from-snapshot"

	// Labels used to denote current status of populating
	// share with snapshot contents.
	//
	// The actual metadata key-value will be in following format:
	// manila.csi.openstack.org/created-from-snapshot: <SNAPSHOT ID>/<STATUS>
	//
	// If no <STATUS> is given, the metadata is expected to look like so:
	// manila.csi.openstack.org/created-from-snapshot: <SNAPSHOT ID>
	// This means this share is successfully populated with its source snapshot,
	// and is ready to use.

	// Share is being populated with snapshot contents
	CreatedFromSnapshotPending = "pending"

	// Share is in a permanent error state and
	// no further attempts fill be made to fix it.
	// This needs manual intervention.
	CreatedFromSnapshotFailed = "failed"

	// Separator used in <SNAPSHOT ID><SEPARATOR><STATUS>
	CreatedFromSnapshotValueSeparator = "/"
)

var createShareFromSnapshotCompatMap = map[shareadapters.ShareAdapterType]Compat{
	shareadapters.CephfsType: createShareFromSnapshotCephfs,
}

func BuildCreatedFromSnapshotInfo(status, snapshotID string) string {
	return fmt.Sprintf("%s%s%s", snapshotID, CreatedFromSnapshotValueSeparator, status)
}

func SplitCreatedFromSnapshotInfo(metadata string) (snapID, status string) {
	ss := strings.SplitN(metadata, CreatedFromSnapshotValueSeparator, 2)

	if len(ss) > 0 {
		snapID = ss[0]
		if len(ss) == 2 {
			status = ss[1]
		}
	}

	return
}
