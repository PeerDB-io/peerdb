package aws_common

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

func LoadSdkConfig(ctx context.Context, region *string) (*aws.Config, error) {
	sdkConfig, err := config.LoadDefaultConfig(ctx, func(options *config.LoadOptions) error {
		if region != nil {
			options.Region = *region
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &sdkConfig, nil

}
