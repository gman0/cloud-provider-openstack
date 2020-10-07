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

	"github.com/container-storage-interface/spec/lib/go/csi"
	wrappers "github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Implements the CSI Identity service
type identityServer struct {
	d    *Driver
	caps []*csi.PluginCapability
}

func newIdentityServer(d *Driver) *identityServer {
	serviceCaps := []csi.PluginCapability_Service_Type{
		csi.PluginCapability_Service_CONTROLLER_SERVICE,
	}

	if d.WithTopology {
		serviceCaps = append(serviceCaps, csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS)
	}

	caps := make([]*csi.PluginCapability, len(serviceCaps))

	for i := range serviceCaps {
		caps[i] = &csi.PluginCapability{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: serviceCaps[i],
				},
			},
		}
	}

	return &identityServer{
		d:    d,
		caps: caps,
	}
}

func (ids *identityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          ids.d.DriverName,
		VendorVersion: ids.d.fqVersion,
	}, nil
}

func (ids *identityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	// Driver is ready when all its partner Node Plugins are ready

	for _, adapter := range ids.d.enabledAdaptersWithNodePlugins {
		conn, err := ids.d.CSIClientBuilder.NewConnectionWithContext(ctx, adapter.GetCSINodePluginInfo().Endpoint)
		if err != nil {
			return nil, status.Error(codes.FailedPrecondition, fmtGrpcConnError(adapter.GetCSINodePluginInfo().Endpoint, err))
		}
		defer conn.Close()

		probeResp, err := ids.d.CSIClientBuilder.NewIdentityServiceClient(conn).Probe(ctx, req)
		if err != nil {
			return nil, fmtGrpcStatusError(err, adapter.GetCSINodePluginInfo())
		}

		if probeResp.GetReady() != nil && !probeResp.GetReady().Value {
			// If a partner Node Plugin reports not-ready, that means we're not ready either.
			return &csi.ProbeResponse{Ready: &wrappers.BoolValue{Value: false}}, nil
		}
	}

	return &csi.ProbeResponse{Ready: &wrappers.BoolValue{Value: true}}, nil
}

func (ids *identityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: ids.caps,
	}, nil
}
