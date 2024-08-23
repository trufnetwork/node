package contracts

import (
	_ "embed"
)

//go:embed system_contract.kf
var SystemContractContent string

//go:embed composed_stream_template.kf
var ComposedStreamContent []byte

//go:embed primitive_stream_template.kf
var PrimitiveStreamContent []byte
