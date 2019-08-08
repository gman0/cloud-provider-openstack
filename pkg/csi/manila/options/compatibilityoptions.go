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

import "k8s.io/cloud-provider-openstack/pkg/share/manila/shareoptions/validator"

type CompatibilityOptions struct {
	CreateShareFromSnapshotEnabled               string `name:"CreateShareFromSnapshotEnabled" value:"default:false" matches:"^true|false$"`
	CreateShareFromSnapshotRetries               string `name:"CreateShareFromSnapshotRetries" value:"default:10" matches:"^\d+$"`
	CreateShareFromSnapshotBackoffInterval       string `name:"CreateShareFromSnapshotBackoffInterval" value:"default:5" matches:"^\d+$"`
	CreateShareFromSnapshotCephFSMounts          string `name:"CreateShareFromSnapshotCephFSMounts" value:"optional"`
	CreateShareFromSnapshotCephFSLogErrorsToFile string `name:"CreateShareFromSnapshotCephFSLogErrorsToFile" value:"default:false" matches:"^true|false$"`
}

var (
	compatOptionsValidator = validator.New(&CompatibilityOptions{})
)

func NewCompatibilityOptions(data map[string]string) (*CompatibilityOptions, error) {
	opts := &CompatibilityOptions{}
	return opts, compatOptionsValidator.Populate(data, opts)
}
