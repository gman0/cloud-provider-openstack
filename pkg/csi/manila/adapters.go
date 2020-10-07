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
	"fmt"
	"strings"

	"k8s.io/cloud-provider-openstack/pkg/csi/manila/shareadapters"
)

func getShareAdapter(proto string, adapters map[shareadapters.ShareAdapterType]shareadapters.ShareAdapter) (shareadapters.ShareAdapter, error) {
	// Try to find a suitable adapter by share protocol
	if t, ok := shareadapters.ShareAdapterNameTypeMap[strings.ToLower(proto)]; ok {
		if adapter, ok := adapters[t]; ok {
			return adapter, nil
		}
	}

	return nil, fmt.Errorf("adapter for share protocol %s either not available or not enabled", proto)
}
