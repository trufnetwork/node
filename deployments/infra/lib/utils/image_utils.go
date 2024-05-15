package utils

import (
	"github.com/aws/jsii-runtime-go"
	"os"
	"strings"
)

type DockerCacheOptions struct {
	CacheType       string
	CacheFromParams string
	CacheToParams   string
}

func GetBuildxCacheOpts() DockerCacheOptions {
	cacheType := "local"
	cacheFromParams := "src=/tmp/buildx-cache/#IMAGE_NAME"
	cacheToParams := "dest=/tmp/buildx-cache-new/#IMAGE_NAME"

	if os.Getenv("CI") == "true" {
		cacheType = "gha"
		cacheFromParams = "scope=truflation/tsn/#IMAGE_NAME"
		cacheToParams = "mode=max,scope=truflation/tsn/#IMAGE_NAME"
	}

	return DockerCacheOptions{
		CacheType:       cacheType,
		CacheFromParams: cacheFromParams,
		CacheToParams:   cacheToParams,
	}
}

// ConvertParamsToMap converts a string of comma-separated key-value pairs to a map.
// e.g.: "key1=value1,key2=value2" -> {"key1": "value1", "key2": "value2"}
func ConvertParamsToMap(paramsStr string) *map[string]*string {
	params := strings.Split(paramsStr, ",")
	paramsMap := make(map[string]*string)
	for _, param := range params {
		kv := strings.Split(param, "=")
		paramsMap[kv[0]] = jsii.String(kv[1])
	}
	return &paramsMap
}

// UpdateMapValues in every param, it replaces the target string with the value string.
func UpdateMapValues(params *map[string]*string, target string, value string) {
	for k, v := range *params {
		(*params)[k] = jsii.String(strings.Replace(*v, target, value, -1))
	}
}

func UpdateParamsWithImageName(paramsStr string, imageName string) *map[string]*string {
	params := ConvertParamsToMap(paramsStr)
	UpdateMapValues(params, "#IMAGE_NAME", imageName)
	return params
}
