//go:build !dev

package main

import "google.golang.org/grpc"

func registerReflection(_ *grpc.Server) {}
