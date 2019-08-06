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
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/manilaclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/component-base/logs"
	"k8s.io/klog"
)

var (
	endpoint      string
	driverName    string
	nodeID        string
	protoSelector string
	fwdEndpoint   string
	userAgentData []string
	compatibility string
)

func parseCompatOpts() (*options.CompatibilityOptions, error) {
	data := make(map[string]string)

	if compatibility == "" {
		return options.NewCompatibilityOptions(data)
	}

	knownCompatSettings := map[string]interface{}{
		"CreateShareFromSnapshotEnabled":               nil,
		"CreateShareFromSnapshotRetries":               nil,
		"CreateShareFromSnapshotBackoffInterval":       nil,
		"CreateShareFromSnapshotCephFSMounts":          nil,
		"CreateShareFromSnapshotCephFSLogErrorsToFile": nil,
	}

	isKnown := func(v string) bool {
		_, ok := knownCompatSettings[v]
		return ok
	}

	settings := strings.Split(compatibility, ",")
	for _, elem := range settings {
		setting := strings.SplitN(elem, "=", 2)

		if len(setting) != 2 || setting[0] == "" || setting[1] == "" {
			return nil, fmt.Errorf("invalid format in option %v, expected KEY=VALUE", setting)
		}

		if !isKnown(setting[0]) {
			return nil, fmt.Errorf("unrecognized option '%s'", setting[0])
		}

		data[setting[0]] = setting[1]
	}

	return options.NewCompatibilityOptions(data)
}

func main() {
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
			compatOpts, err := parseCompatOpts()
			if err != nil {
				klog.Fatalf("failed to parse compatibility settings: %v", err)
			}

			d, err := manila.NewDriver(nodeID, driverName, endpoint, fwdEndpoint, protoSelector, &manilaclient.ClientBuilder{UserAgentData: userAgentData}, compatOpts)
			if err != nil {
				klog.Fatalf("driver initialization failed: %v", err)
			}

			d.Run()
		},
	}

	cmd.Flags().AddGoFlagSet(flag.CommandLine)

	cmd.PersistentFlags().StringVar(&endpoint, "endpoint", "unix://tmp/csi.sock", "CSI endpoint")
	cmd.MarkPersistentFlagRequired("endpoint")

	cmd.PersistentFlags().StringVar(&driverName, "drivername", "manila.csi.openstack.org", "name of the driver")
	cmd.MarkPersistentFlagRequired("drivername")

	cmd.PersistentFlags().StringVar(&nodeID, "nodeid", "", "this node's ID")
	cmd.MarkPersistentFlagRequired("nodeid")

	cmd.PersistentFlags().StringVar(&protoSelector, "share-protocol-selector", "", "specifies which Manila share protocol to use")
	cmd.MarkPersistentFlagRequired("share-protocol-selector")

	cmd.PersistentFlags().StringVar(&fwdEndpoint, "fwdendpoint", "", "CSI Node Plugin endpoint to which all Node Service RPCs are forwarded. Must be able to handle the file-system specified in share-protocol-selector")
	cmd.MarkPersistentFlagRequired("fwdendpoint")

	cmd.PersistentFlags().StringArrayVar(&userAgentData, "user-agent", nil, "extra data to add to gophercloud user-agent. Use multiple times to add more than one component.")

	cmd.PersistentFlags().StringVar(&compatibility, "compatibility-settings", "", "settings for the compatibility layer")

	logs.InitLogs()
	defer logs.FlushLogs()

	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}
