package fferr

import (
	"encoding/json"
	"errors"
	"io"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var logger *zap.SugaredLogger

func init() {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zap.DebugLevel),
		Development:      true,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			NewReflectedEncoder: func(w io.Writer) zapcore.ReflectedEncoder {
				enc := json.NewEncoder(w)
				enc.SetEscapeHTML(false)
				enc.SetIndent("", "    ")
				return enc
			},
		},
	}
	lg, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	logger = lg.Sugar().Named("fferr")
}

// ErrorHandlingInterceptor is a server interceptor for handling errors
func UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Call the handler to process the request
	h, err := handler(ctx, req)
	// Check for GRPCError and convert it
	if err != nil {
		var grpcErr GRPCError
		if errors.As(err, &grpcErr) {
			logger.Errorw("GRPCError", "error", grpcErr, "method", info.FullMethod, "request", req, "response", h, "stackTrace", grpcErr.Stack())
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
			logger.Errorw("GRPCError", "error", grpcErr, "method", info.FullMethod, "stackTrace", grpcErr.Stack())
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
