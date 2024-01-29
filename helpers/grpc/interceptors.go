package grpc

import (
	"errors"
	"github.com/featureform/fferr"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// ErrorHandlingInterceptor is a server interceptor for handling errors
func ErrorHandlingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Call the handler to process the request
	h, err := handler(ctx, req)

	// Check for fferr.GRPCError and convert it
	if err != nil {
		var grpcErr fferr.GRPCError
		if errors.As(err, &grpcErr) {
			return h, grpcErr.ToStatusErr()
		}
	}

	return h, err
}
