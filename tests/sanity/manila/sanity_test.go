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

package sanity

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/kubernetes-csi/csi-test/v3/pkg/sanity"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/shareadapters"
)

func TestDriver(t *testing.T) {
	basePath, err := ioutil.TempDir("", "manila.csi.openstack.org")
	if err != nil {
		t.Fatalf("failed create base path in %s: %v", basePath, err)
	}

	defer func() {
		os.RemoveAll(basePath)
		os.Remove(basePath)
	}()

	endpoint := path.Join(basePath, "csi.sock")

	validAdapters := make([]string, 0, len(shareadapters.ShareAdapterNameTypeMap))
	for name := range shareadapters.ShareAdapterNameTypeMap {
		validAdapters = append(validAdapters, name)
	}

	adapterOpts, err := options.NewAdapterOptions(map[string]string{
		"name":                 "nfs",
		"node-plugin-endpoint": "unix:///fake.sock",
	}, validAdapters)

	if err != nil {
		t.Fatalf("failed to create adapter options: %v", err)
	}

	d, err := manila.NewDriver(
		&manila.DriverOpts{
			DriverName:          "manila.csi.openstack.org",
			NodeID:              "fake-node",
			WithTopology:        true,
			EnabledAdapterOpts:  []options.AdapterOptions{*adapterOpts},
			MountInfoDir:        basePath,
			NodeAZ:              "fake-az",
			ServerCSIEndpoint:   endpoint,
			ManilaClientBuilder: &fakeManilaClientBuilder{},
			CSIClientBuilder:    &fakeCSIClientBuilder{},
			CompatOpts:          &options.CompatibilityOptions{},
		})

	if err != nil {
		t.Fatalf("failed to initialize CSI Manila driver: %v", err)
	}

	go d.Run()

	cfg := sanity.NewTestConfig()
	cfg.SecretsFile = "fake-secrets.yaml"
	cfg.Address = endpoint

	sanity.Test(t, cfg)
}
