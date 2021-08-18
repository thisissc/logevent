package aliyunsls

import (
	sls "github.com/aliyun/aliyun-log-go-sdk"
)

var (
	SlsClient sls.ClientInterface
)

type AliyunConfig struct {
	AccessKeyID     string
	AccessKeySecret string
	Endpoint        string
}

func (c *AliyunConfig) Init() error {
	SlsClient = sls.CreateNormalInterface(c.Endpoint, c.AccessKeyID, c.AccessKeySecret, "")
	return nil
}
