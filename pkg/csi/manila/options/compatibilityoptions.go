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

package options

import (
	"fmt"

	"k8s.io/cloud-provider-openstack/pkg/csi/manila/validator"
)

type CompatibilityOptions struct {
	Name string `name:"name"`

	CreateShareFromSnapshotEnabled   string `name:"create-share-from-snapshot-enabled" value:"default:false" matches:"^true|false$"`
	CreateShareFromSnapshotMountsDir string `name:"create-share-from-snapshot-mounts-dir" value:"optional"`

	RsyncStderrToFile string `name:"rsync-stderr-to-file" value:"default:false" matches:"^true|false$"`
	RsyncIoTimeout    string `name:"rsync-io-timeout" value:"default:false" value:"default:0" matches:"^[0-9]+$"`
}

var (
	compatOptionsValidator = validator.New(&CompatibilityOptions{})
)

func NewCompatibilityOptions(data map[string]string, validAdapterNames []string) (*CompatibilityOptions, error) {
	opts := &CompatibilityOptions{}
	err := compatOptionsValidator.Populate(data, opts)

	if err == nil {
		var found bool
		for _, name := range validAdapterNames {
			if name == opts.Name {
				found = true
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("unknown share adapter %s", opts.Name)
		}
	}

	return opts, nil
}
