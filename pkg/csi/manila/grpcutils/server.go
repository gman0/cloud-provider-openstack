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
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
)

type GrpcServer struct {
	Server   *grpc.Server
	endpoint string
}

func NewServer(endpoint string, opts ...grpc.ServerOption) *GrpcServer {
	return &GrpcServer{
		Server:   grpc.NewServer(opts...),
		endpoint: endpoint,
	}
}

func (s *GrpcServer) Serve() error {
	proto, addr, err := parseGRPCEndpoint(s.endpoint)
	if err != nil {
		return fmt.Errorf("couldn't parse GRPC server endpoint address %s: %v", s.endpoint, err)
	}

	if proto == "unix" {
		if err = os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove an existing socket file %s: %v", addr, err)
		}
	}

	listener, err := net.Listen(proto, addr)
	if err != nil {
		return fmt.Errorf("listen failed for GRPC server: %v", err)
	}

	log.Printf("Listening for connections on %#v", listener.Addr())

	return s.Server.Serve(listener)
}

func parseGRPCEndpoint(endpoint string) (proto, addr string, err error) {
	const (
		unixScheme = "unix://"
		tcpScheme  = "tcp://"
	)

	if strings.HasPrefix(endpoint, "/") {
		return "unix", endpoint, nil
	}

	if strings.HasPrefix(endpoint, unixScheme) {
		pos := len(unixScheme)
		if endpoint[pos] != '/' {
			// endpoint seems to be "unix://absolute/path/to/somewhere"
			// we're missing one '/'...compensate by decrementing pos
			pos--
		}

		return "unix", endpoint[pos:], nil
	}

	if strings.HasPrefix(endpoint, tcpScheme) {
		return "tcp", endpoint[len(tcpScheme):], nil
	}

	return "", "", errors.New("endpoint uses unsupported scheme")
}
