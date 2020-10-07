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
	"time"

	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

var (
	DefaultDialOptions = []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithBlock(),
	}
)

func NewConnection(endpoint string, dialOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	var (
		conn *grpc.ClientConn
		err  error
	)

	dialFinished := make(chan bool)
	go func() {
		conn, err = grpc.Dial(endpoint, dialOptions...)
		close(dialFinished)
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			klog.Warningf("still connecting to %s", endpoint)
		case <-dialFinished:
			return conn, err
		}
	}
}

func NewConnectionWithContext(ctx context.Context, endpoint string, dialOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, endpoint, dialOptions...)
}
