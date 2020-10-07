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

package csiclient

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/cloud-provider-openstack/pkg/csi/manila/grpcutils"
)

var _ Builder = &ClientBuilder{}

func NewNodeSvcClient(conn *grpc.ClientConn) *NodeSvcClient {
	return &NodeSvcClient{cl: csi.NewNodeClient(conn)}
}

func NewIdentitySvcClient(conn *grpc.ClientConn) *IdentitySvcClient {
	return &IdentitySvcClient{cl: csi.NewIdentityClient(conn)}
}

type ClientBuilder struct{}

func (b ClientBuilder) NewConnection(endpoint string) (*grpc.ClientConn, error) {
	dialOpts := append(grpcutils.DefaultDialOptions, grpc.WithUnaryInterceptor(grpcutils.ClientLogRPCWithLabel(endpoint)))
	return grpcutils.NewConnection(endpoint, dialOpts...)
}

func (b ClientBuilder) NewConnectionWithContext(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
	dialOpts := append(grpcutils.DefaultDialOptions, grpc.WithUnaryInterceptor(grpcutils.ClientLogRPCWithLabel(endpoint)))
	return grpcutils.NewConnectionWithContext(ctx, endpoint, dialOpts...)
}

func (b ClientBuilder) NewNodeServiceClient(conn *grpc.ClientConn) Node {
	return NewNodeSvcClient(conn)
}

func (b ClientBuilder) NewIdentityServiceClient(conn *grpc.ClientConn) Identity {
	return NewIdentitySvcClient(conn)
}
