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

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/csiclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/manilaclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/runtimeconfig"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/shareadapters"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
)

var (
	endpoint                     string
	driverName                   string
	nodeID                       string
	nodeAZ                       string
	mountInfoDir                 string
	runtimeConfigFile            string
	withTopology                 bool
	protoSelector                string // Deprecated
	fwdEndpoint                  string // Deprecated
	enabledAdapters              []string
	adapterCompatibilitySettings []string
	userAgentData                []string
)

func parseMapFromOption(s string) (map[string]string, error) {
	const (
		kvElemSeparator    = ","
		kvMappingSeparator = "="
	)

	if s == "" {
		return nil, nil
	}

	kvPairs := strings.Split(s, kvElemSeparator)
	m := make(map[string]string)

	for _, elem := range kvPairs {
		kvPair := strings.SplitN(elem, kvMappingSeparator, 2)
		if len(kvPair) != 2 || kvPair[0] == "" || kvPair[1] == "" {
			return nil, fmt.Errorf("invalid format in option %v, expected KEY=VALUE", elem)
		}

		m[kvPair[0]] = kvPair[1]
	}

	return m, nil
}

func parseAdapterOptions_v0(validAdapters []string) ([]options.AdapterOptions, error) {
	if protoSelector == "" && fwdEndpoint == "" {
		return nil, nil
	}

	opt, err := options.NewAdapterOptions(
		map[string]string{
			"name":                 protoSelector,
			"node-plugin-endpoint": fwdEndpoint,
		},
		validAdapters,
	)

	return []options.AdapterOptions{*opt}, err
}

func parseAdapterOptions_v1(validAdapters []string) ([]options.AdapterOptions, error) {
	opts := make([]options.AdapterOptions, len(enabledAdapters))

	for i := range enabledAdapters {
		m, err := parseMapFromOption(enabledAdapters[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse --enable-adapter %s: %v", enabledAdapters[i], err)
		}

		opt, err := options.NewAdapterOptions(m, validAdapters)
		if err != nil {
			return nil, fmt.Errorf("failed to validate --enable-adapter %s: %v", enabledAdapters[i], err)
		}

		opts[i] = *opt
	}

	return opts, nil
}

func parseCompatibilityOptions(validAdapters []string) ([]options.CompatibilityOptions, error) {
	opts := make([]options.CompatibilityOptions, len(adapterCompatibilitySettings))

	for i := range adapterCompatibilitySettings {
		m, err := parseMapFromOption(adapterCompatibilitySettings[i])
		if err != nil {
			return nil, fmt.Errorf("failed to parse --adapter-compatibility %s: %v", adapterCompatibilitySettings[i], err)
		}

		opt, err := options.NewCompatibilityOptions(m, validAdapters)
		if err != nil {
			return nil, fmt.Errorf("failed to validate --adapter-compatibility %s: %v", adapterCompatibilitySettings[i], err)
		}

		opts[i] = *opt
	}

	return opts, nil
}

func main() {
	validAdapters := make([]string, 0, len(shareadapters.ShareAdapterNameTypeMap))
	for name := range shareadapters.ShareAdapterNameTypeMap {
		validAdapters = append(validAdapters, name)
	}

	flag.CommandLine.Parse([]string{})

	cmd := &cobra.Command{
		Use:   os.Args[0],
		Short: "CSI Manila driver",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// Glog requires this otherwise it complains.
			flag.CommandLine.Parse(nil)

			// This is a temporary hack to enable proper logging until upstream dependencies
			// are migrated to fully utilize klog instead of glog.
			klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
			klog.InitFlags(klogFlags)

			// Sync the glog and klog flags.
			cmd.Flags().VisitAll(func(f1 *pflag.Flag) {
				f2 := klogFlags.Lookup(f1.Name)
				if f2 != nil {
					value := f1.Value.String()
					f2.Value.Set(value)
				}
			})
		},
		Run: func(cmd *cobra.Command, args []string) {
			var adapterOpts []options.AdapterOptions
			var err error

			if adapterOpts, err = parseAdapterOptions_v1(validAdapters); err != nil {
				klog.Fatal(err.Error())
			}

			if adapterOpts_v0, err := parseAdapterOptions_v0(validAdapters); err != nil {
				klog.Fatal(err.Error())
			} else {
				if adapterOpts != nil {
					if adapterOpts_v0 != nil {
						// parseAdapterOptions_v1 has returned a set of enabled adapters,
						// but so did parseAdapterOptions_v0.
						// Using v0 and v1 style of enabling share protocols at the same
						// time may be confusing to users and is therefore not allowed.
						klog.Fatal("Mixing --enable-adapter with --share-protocol-selector is not allowed. Please use --enable-adapter option only.")
					}
				} else {
					adapterOpts = adapterOpts_v0
				}
			}

			manilaClientBuilder := &manilaclient.ClientBuilder{UserAgent: "manila-csi-plugin", ExtraUserAgentData: userAgentData}
			csiClientBuilder := &csiclient.ClientBuilder{}

			d, err := manila.NewDriver(
				&manila.DriverOpts{
					DriverName:          driverName,
					NodeID:              nodeID,
					NodeAZ:              nodeAZ,
					WithTopology:        withTopology,
					EnabledAdapterOpts:  adapterOpts,
					MountInfoDir:        mountInfoDir,
					ServerCSIEndpoint:   endpoint,
					ManilaClientBuilder: manilaClientBuilder,
					CSIClientBuilder:    csiClientBuilder,
				},
			)

			if err != nil {
				klog.Fatalf("driver initialization failed: %v", err)
			}

			runtimeconfig.RuntimeConfigFilename = runtimeConfigFile

			if err = d.Run(); err != nil {
				klog.Fatalf("driver failed: %v", err)
			}
		},
	}

	cmd.Flags().AddGoFlagSet(flag.CommandLine)

	cmd.PersistentFlags().StringVar(&endpoint, "endpoint", "unix://tmp/csi.sock", "CSI endpoint")

	cmd.PersistentFlags().StringVar(&driverName, "drivername", "manila.csi.openstack.org", "name of the driver")

	cmd.PersistentFlags().StringVar(&nodeID, "nodeid", "", "this node's ID")
	cmd.MarkPersistentFlagRequired("nodeid")

	cmd.PersistentFlags().StringVar(&nodeAZ, "nodeaz", "", "this node's availability zone")

	cmd.PersistentFlags().StringVar(&mountInfoDir, "mount-info-dir", "", "path to a directory where mount info files will be stored")

	cmd.PersistentFlags().StringVar(&runtimeConfigFile, "runtime-config-file", "", "path to the runtime configuration file")

	cmd.PersistentFlags().BoolVar(&withTopology, "with-topology", false, "cluster is topology-aware")

	cmd.PersistentFlags().StringVar(&protoSelector, "share-protocol-selector", "", "specifies which Manila share protocol to use. Valid values are NFS and CEPHFS.")
	cmd.PersistentFlags().MarkDeprecated("share-protocol-selector", "This option is deprecated and will be removed completely in CPO v1.22.0. Please use --enable-adapter instead.")

	cmd.PersistentFlags().StringVar(&fwdEndpoint, "fwdendpoint", "", "CSI Node Plugin endpoint to which all Node Service RPCs are forwarded. Must be able to handle the file-system specified in share-protocol-selector")
	cmd.PersistentFlags().MarkDeprecated("fwdendpoint", "This option is deprecated and will be removed completely in CPO v1.22.0. Please use --enable-adapter instead.")

	cmd.PersistentFlags().StringArrayVar(&enabledAdapters, "enable-adapter", nil, "enables a share adapter. Repeat for each share adapter you wish to enable.")

	cmd.PersistentFlags().StringArrayVar(&adapterCompatibilitySettings, "adapter-compatibility", nil, "compatibility settings for an enabled share adapter. Repeat for each share adapter you wish to configure.")

	cmd.PersistentFlags().StringArrayVar(&userAgentData, "user-agent", nil, "extra data to add to gophercloud user-agent. Use multiple times to add more than one component.")

	logs.InitLogs()
	defer logs.FlushLogs()

	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}
