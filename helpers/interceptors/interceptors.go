// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package interceptors

import (
	"errors"

	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// ErrorHandlingInterceptor is a server interceptor for handling errors
func UnaryServerErrorInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Call the handler to process the request
	h, err := handler(ctx, req)
	// Check for GRPCError and convert it
	if err != nil {
		var grpcErr fferr.Error
		if errors.As(err, &grpcErr) {
			logging.GlobalLogger.Errorw("GRPCError", "error", grpcErr, "method", info.FullMethod, "request", req, "response", h, "stack_trace", grpcErr.Stack())
			return h, grpcErr.ToErr()
		}
	}

	return h, err
}

func StreamServerErrorInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// Call the handler to process the request
	err := handler(srv, ss)
	// Check for GRPCError and convert it
	if err != nil {
		var grpcErr fferr.Error
		if errors.As(err, &grpcErr) {
			logging.GlobalLogger.Errorw("GRPCError", "error", grpcErr, "method", info.FullMethod, "stackTrace", grpcErr.Stack())
			return grpcErr.ToErr()
		}
	}

	return err
}
