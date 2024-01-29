package fferr

import (
	"errors"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// ErrorHandlingInterceptor is a server interceptor for handling errors
func UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Call the handler to process the request
	h, err := handler(ctx, req)
	// Check for GRPCError and convert it
	if err != nil {
		var grpcErr GRPCError
		if errors.As(err, &grpcErr) {
			return h, grpcErr.ToErr()
		}
	}

	return h, err
}

func StreamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// Call the handler to process the request
	err := handler(srv, ss)
	// Check for GRPCError and convert it
	if err != nil {
		var grpcErr GRPCError
		if errors.As(err, &grpcErr) {
			return grpcErr.ToErr()
		}
	}

	return err
}

func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Call the invoker to execute the RPC
		err := invoker(ctx, method, req, reply, cc, opts...)
		// Convert to GRPCError implementation
		grpcErr := FromErr(err)
		return grpcErr
	}
}

func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Call the streamer to execute the RPC
		stream, err := streamer(ctx, desc, cc, method, opts...)
		// Convert to GRPCError implementation
		grpcErr := FromErr(err)
		return stream, grpcErr
	}
}
