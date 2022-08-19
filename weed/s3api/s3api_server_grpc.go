package s3api

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/rpc"
)

func (s3a *S3ApiServer) Configure(ctx context.Context, request *rpc.S3ConfigureRequest) (*rpc.S3ConfigureResponse, error) {

	if err := s3a.iam.LoadS3ApiConfigurationFromBytes(request.S3ConfigurationFileContent); err != nil {
		return nil, err
	}

	return &rpc.S3ConfigureResponse{}, nil

}
