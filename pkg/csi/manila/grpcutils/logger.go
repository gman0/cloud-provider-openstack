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

package grpcutils

import (
	"context"
	"sync/atomic"

	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

var (
	callCounter uint64
)

func ServerLogRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	callID := atomic.AddUint64(&callCounter, 1)

	klog.V(3).Infof("[ID:%d] GRPC call: %s", callID, info.FullMethod)
	klog.V(5).Infof("[ID:%d] GRPC request: %s", callID, protosanitizer.StripSecrets(req))
	resp, err := handler(ctx, req)

	if err != nil {
		klog.Errorf("[ID:%d] GRPC error: %v", callID, err)
	} else {
		klog.V(5).Infof("[ID:%d] GRPC response: %s", callID, protosanitizer.StripSecrets(resp))
	}

	return resp, err
}

func ClientLogRPCWithLabel(label string) func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		callID := atomic.AddUint64(&callCounter, 1)

		klog.V(3).Infof("[ID:%d] GRPC call %s: %s", callID, label, method)
		klog.V(5).Infof("[ID:%d] GRPC request %s: %s", callID, label, protosanitizer.StripSecrets(req))

		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil {
			klog.Errorf("[ID:%d] GRPC error %s: %v", callID, label, err)
		} else {
			klog.V(5).Infof("[ID:%d] GRPC response %s: %s", callID, label, protosanitizer.StripSecrets(reply))
		}

		return err
	}
}
