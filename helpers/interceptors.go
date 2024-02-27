package helpers

import (
	"errors"
	"github.com/featureform/fferr"
	"github.com/featureform/logging"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var logger *zap.SugaredLogger

func init() {
	if shouldUseStackTraceLogger := GetEnvBool("FEATUREFORM_DEBUG", false); shouldUseStackTraceLogger {
		logger = logging.NewStackTraceLogger("fferr")
	} else {
		logger = logging.NewLogger("fferr")
	}
}

// ErrorHandlingInterceptor is a server interceptor for handling errors
func UnaryServerErrorInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Call the handler to process the request
	h, err := handler(ctx, req)
	// Check for GRPCError and convert it
	if err != nil {
		var grpcErr fferr.GRPCError
		if errors.As(err, &grpcErr) {
			logger.Errorw("GRPCError", "error", grpcErr, "method", info.FullMethod, "request", req, "response", h, "stack_trace", grpcErr.Stack())
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
		var grpcErr fferr.GRPCError
		if errors.As(err, &grpcErr) {
			logger.Errorw("GRPCError", "error", grpcErr, "method", info.FullMethod, "stackTrace", grpcErr.Stack())
			return grpcErr.ToErr()
		}
	}

	return err
}
