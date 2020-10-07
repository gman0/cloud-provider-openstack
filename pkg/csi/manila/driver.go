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
	"errors"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/csiclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/grpcutils"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/manilaclient"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/options"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/shareadapters"
	"k8s.io/cloud-provider-openstack/pkg/version"
	"k8s.io/klog/v2"
)

type DriverOpts struct {
	DriverName         string
	NodeID             string
	NodeAZ             string
	WithTopology       bool
	EnabledAdapterOpts []options.AdapterOptions
	MountInfoDir       string

	ServerCSIEndpoint string

	ManilaClientBuilder manilaclient.Builder
	CSIClientBuilder    csiclient.Builder

	CompatOpts *options.CompatibilityOptions
}

func (o *DriverOpts) validate() error {
	notEmpty := func(value, name string) error {
		if value == "" {
			return fmt.Errorf("%s is missing", name)
		}
		return nil
	}

	valueNameMap := map[string]string{
		o.DriverName:        "driver name",
		o.NodeID:            "node ID",
		o.ServerCSIEndpoint: "CSI driver endpoint",
	}

	for k, v := range valueNameMap {
		if err := notEmpty(k, v); err != nil {
			return err
		}
	}

	if o.EnabledAdapterOpts == nil || len(o.EnabledAdapterOpts) == 0 {
		return errors.New("no share adapters enabled")
	}

	proto, addr, err := parseGRPCEndpoint(o.ServerCSIEndpoint)
	if err != nil {
		return fmt.Errorf("failed to parse the CSI driver endpoint: %v", err)
	}
	o.ServerCSIEndpoint = endpointAddress(proto, addr)

	for i := range o.EnabledAdapterOpts {
		if o.EnabledAdapterOpts[i].NodePluginEndpoint == "" {
			continue
		}

		proto, addr, err = parseGRPCEndpoint(o.EnabledAdapterOpts[i].NodePluginEndpoint)
		if err != nil {
			return fmt.Errorf("failed to parse node plugin endpoint %s for adapter %s: %v",
				o.EnabledAdapterOpts[i].NodePluginEndpoint, o.EnabledAdapterOpts[i].Name, err)
		}

		o.EnabledAdapterOpts[i].NodePluginEndpoint = endpointAddress(proto, addr)
	}

	return nil
}

type Driver struct {
	*DriverOpts

	fqVersion string // Fully qualified version in format {driverVersion}@{CPO version}

	ids *identityServer
	cs  *controllerServer
	ns  *nodeServer

	// All enabled share adapters
	enabledAdapters map[shareadapters.ShareAdapterType]shareadapters.ShareAdapter
	// Subset of `enabledAdapters` with `NodePluginEndpoint` defined
	enabledAdaptersWithNodePlugins map[shareadapters.ShareAdapterType]shareadapters.ShareAdapter
}

const (
	specVersion   = "1.2.0"
	driverVersion = "1.0.0-preview"
	topologyKey   = "topology.manila.csi.openstack.org/zone"
)

func NewDriver(opts *DriverOpts) (*Driver, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	d := &Driver{
		DriverOpts: opts,
		fqVersion:  fmt.Sprintf("%s@%s", driverVersion, version.Version),

		enabledAdapters:                make(map[shareadapters.ShareAdapterType]shareadapters.ShareAdapter),
		enabledAdaptersWithNodePlugins: make(map[shareadapters.ShareAdapterType]shareadapters.ShareAdapter),
	}

	klog.Info("Driver: ", d.DriverName)
	klog.Info("Driver version: ", d.fqVersion)
	klog.Info("CSI spec version: ", specVersion)

	if d.WithTopology {
		klog.Infof("Topology awareness enabled, node availability zone: %s", d.NodeAZ)
	} else {
		klog.Info("Topology awareness disabled")
	}

	// Initialize share adapters

	for _, adapterOpts := range opts.EnabledAdapterOpts {
		klog.Infof("Enabling adapter %#v", adapterOpts)

		var (
			adapterType           = shareadapters.ShareAdapterNameTypeMap[adapterOpts.Name]
			adapter               shareadapters.ShareAdapter
			partnerNodePluginInfo *shareadapters.CSINodePluginInfo
			err                   error
		)

		if _, ok := d.enabledAdapters[adapterType]; ok {
			// Enabling the same share adapter twice may be a configuration error
			// and is therefore not allowed.
			return nil, fmt.Errorf("adapter %s already enabled", adapterOpts.Name)
		}

		if adapterOpts.NodePluginEndpoint != "" {
			// Adapter has a Node Plugin endpoint defined. Try to retrieve its credentials.
			partnerNodePluginInfo, err = buildCSINodePluginInfo(adapterOpts.NodePluginEndpoint, d.CSIClientBuilder)
			if err != nil {
				return nil, err
			}
		}

		// Create a share adapter of the desired type

		switch adapterType {
		case shareadapters.CephfsType:
			adapter = &shareadapters.Cephfs{
				NodePluginInfo: partnerNodePluginInfo,
			}

		case shareadapters.NfsType:
			adapter = &shareadapters.NFS{
				NodePluginInfo: partnerNodePluginInfo,
			}

		default:
			return nil, fmt.Errorf("unknown adapter %s", adapterOpts.Name)
		}

		// Store the adapter

		d.enabledAdapters[adapterType] = adapter
		if adapterOpts.NodePluginEndpoint != "" {
			d.enabledAdaptersWithNodePlugins[adapterType] = adapter
			klog.Infof("Associating %s adapter with %#v", adapterOpts.Name, adapter.GetCSINodePluginInfo())
		}
	}

	// Initialize Identity server
	d.ids = newIdentityServer(d)

	// Initialize Node server
	d.ns = newNodeServer(d)

	// Initialize Controller server
	d.cs = newControllerServer(d)

	return d, nil
}

func buildCSINodePluginInfo(endpoint string, csiClientBuilder csiclient.Builder) (*shareadapters.CSINodePluginInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	conn, err := csiClientBuilder.NewConnectionWithContext(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", endpoint, err)
	}
	defer conn.Close()

	getPluginInfoResp, err := csiClientBuilder.NewIdentityServiceClient(conn).GetPluginInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetPluginInfo failed for %s: %v", endpoint, err)
	}

	getNodeCapabilitiesResp, err := csiClientBuilder.NewNodeServiceClient(conn).GetCapabilities(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetNodeCapabilities failed for %s: %v", endpoint, err)
	}

	nodeCaps := make(map[csi.NodeServiceCapability_RPC_Type]struct{}, len(getNodeCapabilitiesResp.GetCapabilities()))
	for _, capability := range getNodeCapabilitiesResp.GetCapabilities() {
		nodeCaps[capability.GetRpc().GetType()] = struct{}{}
	}

	return &shareadapters.CSINodePluginInfo{
		Endpoint:     endpoint,
		Name:         getPluginInfoResp.GetName(),
		Version:      getPluginInfoResp.GetVendorVersion(),
		Capabilities: nodeCaps,
	}, nil
}

func (d *Driver) Run() error {
	s := grpcutils.NewServer(d.ServerCSIEndpoint, grpc.UnaryInterceptor(grpcutils.ServerLogRPC))

	idsCapTypesToStringSlice := func() []string {
		ss := make([]string, len(d.ids.caps))
		for i := range d.ids.caps {
			ss[i] = d.ids.caps[i].String()
		}
		return ss
	}

	klog.Info("Registering Indentity server with ", idsCapTypesToStringSlice())
	csi.RegisterIdentityServer(s.Server, d.ids)

	nsCapTypesToStringSlice := func() []string {
		ss := make([]string, len(nodeServiceCapsTypes))
		for i := range nodeServiceCapsTypes {
			ss[i] = nodeServiceCapsTypes[i].String()
		}
		return ss
	}

	klog.Info("Registering Node server with ", nsCapTypesToStringSlice())
	csi.RegisterNodeServer(s.Server, d.ns)

	csCapTypesToStringSlice := func() []string {
		ss := make([]string, len(controllerServiceCapsTypes))
		for i := range controllerServiceCapsTypes {
			ss[i] = controllerServiceCapsTypes[i].String()
		}
		return ss
	}

	klog.Info("Registering Controller server with ", csCapTypesToStringSlice())
	csi.RegisterControllerServer(s.Server, d.cs)

	return s.Serve()
}
