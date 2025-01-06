package contracts

import (
	_ "embed"
)

//go:embed system_contract.kf
var SystemContractStringContent string

//go:embed system_contract.kf
var SystemContractContent []byte

//go:embed composed_stream_template.kf
var ComposedStreamContent []byte

//go:embed primitive_stream_template.kf
var PrimitiveStreamContent []byte

//go:embed composed_stream_template_unix.kf
var ComposedStreamUnixContent []byte

//go:embed primitive_stream_unix.kf
var PrimitiveStreamUnixContent []byte
