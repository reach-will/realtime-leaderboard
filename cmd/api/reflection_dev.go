//go:build dev

package main

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func registerReflection(s *grpc.Server) {
	reflection.Register(s)
}
