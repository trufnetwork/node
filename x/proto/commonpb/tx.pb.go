// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.7
// source: kwil/common/tx.proto

package commonpb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type PayloadType int32

const (
	PayloadType_INVALID_PAYLOAD_TYPE PayloadType = 0
	PayloadType_DEPLOY_DATABASE      PayloadType = 1
	PayloadType_MODIFY_DATABASE      PayloadType = 2
	PayloadType_DELETE_DATABASE      PayloadType = 3
	PayloadType_EXECUTE_QUERY        PayloadType = 4
	PayloadType_WITHDRAW             PayloadType = 5
)

// Enum value maps for PayloadType.
var (
	PayloadType_name = map[int32]string{
		0: "INVALID_PAYLOAD_TYPE",
		1: "DEPLOY_DATABASE",
		2: "MODIFY_DATABASE",
		3: "DELETE_DATABASE",
		4: "EXECUTE_QUERY",
		5: "WITHDRAW",
	}
	PayloadType_value = map[string]int32{
		"INVALID_PAYLOAD_TYPE": 0,
		"DEPLOY_DATABASE":      1,
		"MODIFY_DATABASE":      2,
		"DELETE_DATABASE":      3,
		"EXECUTE_QUERY":        4,
		"WITHDRAW":             5,
	}
)

func (x PayloadType) Enum() *PayloadType {
	p := new(PayloadType)
	*p = x
	return p
}

func (x PayloadType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (PayloadType) Descriptor() protoreflect.EnumDescriptor {
	return file_kwil_common_tx_proto_enumTypes[0].Descriptor()
}

func (PayloadType) Type() protoreflect.EnumType {
	return &file_kwil_common_tx_proto_enumTypes[0]
}

func (x PayloadType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use PayloadType.Descriptor instead.
func (PayloadType) EnumDescriptor() ([]byte, []int) {
	return file_kwil_common_tx_proto_rawDescGZIP(), []int{0}
}

type SignatureType int32

const (
	SignatureType_INVALID_SINATURE_TYPE          SignatureType = 0
	SignatureType_PK_SECP256K1_UNCOMPRESSED      SignatureType = 1
	SignatureType_ACCOUNT_SECP256K1_UNCOMPRESSED SignatureType = 2
	SignatureType_END_SIGNATURE_TYPE             SignatureType = 3
)

// Enum value maps for SignatureType.
var (
	SignatureType_name = map[int32]string{
		0: "INVALID_SINATURE_TYPE",
		1: "PK_SECP256K1_UNCOMPRESSED",
		2: "ACCOUNT_SECP256K1_UNCOMPRESSED",
		3: "END_SIGNATURE_TYPE",
	}
	SignatureType_value = map[string]int32{
		"INVALID_SINATURE_TYPE":          0,
		"PK_SECP256K1_UNCOMPRESSED":      1,
		"ACCOUNT_SECP256K1_UNCOMPRESSED": 2,
		"END_SIGNATURE_TYPE":             3,
	}
)

func (x SignatureType) Enum() *SignatureType {
	p := new(SignatureType)
	*p = x
	return p
}

func (x SignatureType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (SignatureType) Descriptor() protoreflect.EnumDescriptor {
	return file_kwil_common_tx_proto_enumTypes[1].Descriptor()
}

func (SignatureType) Type() protoreflect.EnumType {
	return &file_kwil_common_tx_proto_enumTypes[1]
}

func (x SignatureType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use SignatureType.Descriptor instead.
func (SignatureType) EnumDescriptor() ([]byte, []int) {
	return file_kwil_common_tx_proto_rawDescGZIP(), []int{1}
}

type Tx struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Hash        []byte      `protobuf:"bytes,1,opt,name=hash,proto3" json:"hash,omitempty"`
	PayloadType PayloadType `protobuf:"varint,2,opt,name=payload_type,json=payloadType,proto3,enum=common.PayloadType" json:"payload_type,omitempty"`
	Payload     []byte      `protobuf:"bytes,3,opt,name=payload,proto3" json:"payload,omitempty"`
	Nonce       int64       `protobuf:"varint,4,opt,name=nonce,proto3" json:"nonce,omitempty"`
	Signature   *Signature  `protobuf:"bytes,5,opt,name=signature,proto3" json:"signature,omitempty"`
	Fee         string      `protobuf:"bytes,6,opt,name=fee,proto3" json:"fee,omitempty"`
	Sender      string      `protobuf:"bytes,7,opt,name=sender,proto3" json:"sender,omitempty"`
}

func (x *Tx) Reset() {
	*x = Tx{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kwil_common_tx_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Tx) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Tx) ProtoMessage() {}

func (x *Tx) ProtoReflect() protoreflect.Message {
	mi := &file_kwil_common_tx_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Tx.ProtoReflect.Descriptor instead.
func (*Tx) Descriptor() ([]byte, []int) {
	return file_kwil_common_tx_proto_rawDescGZIP(), []int{0}
}

func (x *Tx) GetHash() []byte {
	if x != nil {
		return x.Hash
	}
	return nil
}

func (x *Tx) GetPayloadType() PayloadType {
	if x != nil {
		return x.PayloadType
	}
	return PayloadType_INVALID_PAYLOAD_TYPE
}

func (x *Tx) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

func (x *Tx) GetNonce() int64 {
	if x != nil {
		return x.Nonce
	}
	return 0
}

func (x *Tx) GetSignature() *Signature {
	if x != nil {
		return x.Signature
	}
	return nil
}

func (x *Tx) GetFee() string {
	if x != nil {
		return x.Fee
	}
	return ""
}

func (x *Tx) GetSender() string {
	if x != nil {
		return x.Sender
	}
	return ""
}

type Signature struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SignatureBytes []byte        `protobuf:"bytes,1,opt,name=signature_bytes,json=signatureBytes,proto3" json:"signature_bytes,omitempty"`
	SignatureType  SignatureType `protobuf:"varint,2,opt,name=signature_type,json=signatureType,proto3,enum=common.SignatureType" json:"signature_type,omitempty"`
}

func (x *Signature) Reset() {
	*x = Signature{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kwil_common_tx_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Signature) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Signature) ProtoMessage() {}

func (x *Signature) ProtoReflect() protoreflect.Message {
	mi := &file_kwil_common_tx_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Signature.ProtoReflect.Descriptor instead.
func (*Signature) Descriptor() ([]byte, []int) {
	return file_kwil_common_tx_proto_rawDescGZIP(), []int{1}
}

func (x *Signature) GetSignatureBytes() []byte {
	if x != nil {
		return x.SignatureBytes
	}
	return nil
}

func (x *Signature) GetSignatureType() SignatureType {
	if x != nil {
		return x.SignatureType
	}
	return SignatureType_INVALID_SINATURE_TYPE
}

var File_kwil_common_tx_proto protoreflect.FileDescriptor

var file_kwil_common_tx_proto_rawDesc = []byte{
	0x0a, 0x14, 0x6b, 0x77, 0x69, 0x6c, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x74, 0x78,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x22, 0xdb,
	0x01, 0x0a, 0x02, 0x54, 0x78, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61, 0x73, 0x68, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x12, 0x36, 0x0a, 0x0c, 0x70, 0x61, 0x79,
	0x6c, 0x6f, 0x61, 0x64, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32,
	0x13, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64,
	0x54, 0x79, 0x70, 0x65, 0x52, 0x0b, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x54, 0x79, 0x70,
	0x65, 0x12, 0x18, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x14, 0x0a, 0x05, 0x6e,
	0x6f, 0x6e, 0x63, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x6e, 0x6f, 0x6e, 0x63,
	0x65, 0x12, 0x2f, 0x0a, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x53, 0x69,
	0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x52, 0x09, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75,
	0x72, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x66, 0x65, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x03, 0x66, 0x65, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x65, 0x6e, 0x64, 0x65, 0x72, 0x18, 0x07,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x65, 0x6e, 0x64, 0x65, 0x72, 0x22, 0x72, 0x0a, 0x09,
	0x53, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x12, 0x27, 0x0a, 0x0f, 0x73, 0x69, 0x67,
	0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x0e, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x42, 0x79, 0x74,
	0x65, 0x73, 0x12, 0x3c, 0x0a, 0x0e, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x5f,
	0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x15, 0x2e, 0x63, 0x6f, 0x6d,
	0x6d, 0x6f, 0x6e, 0x2e, 0x53, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x54, 0x79, 0x70,
	0x65, 0x52, 0x0d, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x54, 0x79, 0x70, 0x65,
	0x2a, 0x87, 0x01, 0x0a, 0x0b, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x54, 0x79, 0x70, 0x65,
	0x12, 0x18, 0x0a, 0x14, 0x49, 0x4e, 0x56, 0x41, 0x4c, 0x49, 0x44, 0x5f, 0x50, 0x41, 0x59, 0x4c,
	0x4f, 0x41, 0x44, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x10, 0x00, 0x12, 0x13, 0x0a, 0x0f, 0x44, 0x45,
	0x50, 0x4c, 0x4f, 0x59, 0x5f, 0x44, 0x41, 0x54, 0x41, 0x42, 0x41, 0x53, 0x45, 0x10, 0x01, 0x12,
	0x13, 0x0a, 0x0f, 0x4d, 0x4f, 0x44, 0x49, 0x46, 0x59, 0x5f, 0x44, 0x41, 0x54, 0x41, 0x42, 0x41,
	0x53, 0x45, 0x10, 0x02, 0x12, 0x13, 0x0a, 0x0f, 0x44, 0x45, 0x4c, 0x45, 0x54, 0x45, 0x5f, 0x44,
	0x41, 0x54, 0x41, 0x42, 0x41, 0x53, 0x45, 0x10, 0x03, 0x12, 0x11, 0x0a, 0x0d, 0x45, 0x58, 0x45,
	0x43, 0x55, 0x54, 0x45, 0x5f, 0x51, 0x55, 0x45, 0x52, 0x59, 0x10, 0x04, 0x12, 0x0c, 0x0a, 0x08,
	0x57, 0x49, 0x54, 0x48, 0x44, 0x52, 0x41, 0x57, 0x10, 0x05, 0x2a, 0x85, 0x01, 0x0a, 0x0d, 0x53,
	0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x19, 0x0a, 0x15,
	0x49, 0x4e, 0x56, 0x41, 0x4c, 0x49, 0x44, 0x5f, 0x53, 0x49, 0x4e, 0x41, 0x54, 0x55, 0x52, 0x45,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x10, 0x00, 0x12, 0x1d, 0x0a, 0x19, 0x50, 0x4b, 0x5f, 0x53, 0x45,
	0x43, 0x50, 0x32, 0x35, 0x36, 0x4b, 0x31, 0x5f, 0x55, 0x4e, 0x43, 0x4f, 0x4d, 0x50, 0x52, 0x45,
	0x53, 0x53, 0x45, 0x44, 0x10, 0x01, 0x12, 0x22, 0x0a, 0x1e, 0x41, 0x43, 0x43, 0x4f, 0x55, 0x4e,
	0x54, 0x5f, 0x53, 0x45, 0x43, 0x50, 0x32, 0x35, 0x36, 0x4b, 0x31, 0x5f, 0x55, 0x4e, 0x43, 0x4f,
	0x4d, 0x50, 0x52, 0x45, 0x53, 0x53, 0x45, 0x44, 0x10, 0x02, 0x12, 0x16, 0x0a, 0x12, 0x45, 0x4e,
	0x44, 0x5f, 0x53, 0x49, 0x47, 0x4e, 0x41, 0x54, 0x55, 0x52, 0x45, 0x5f, 0x54, 0x59, 0x50, 0x45,
	0x10, 0x03, 0x42, 0x17, 0x5a, 0x15, 0x6b, 0x77, 0x69, 0x6c, 0x2f, 0x78, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_kwil_common_tx_proto_rawDescOnce sync.Once
	file_kwil_common_tx_proto_rawDescData = file_kwil_common_tx_proto_rawDesc
)

func file_kwil_common_tx_proto_rawDescGZIP() []byte {
	file_kwil_common_tx_proto_rawDescOnce.Do(func() {
		file_kwil_common_tx_proto_rawDescData = protoimpl.X.CompressGZIP(file_kwil_common_tx_proto_rawDescData)
	})
	return file_kwil_common_tx_proto_rawDescData
}

var file_kwil_common_tx_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_kwil_common_tx_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_kwil_common_tx_proto_goTypes = []interface{}{
	(PayloadType)(0),   // 0: common.PayloadType
	(SignatureType)(0), // 1: common.SignatureType
	(*Tx)(nil),         // 2: common.Tx
	(*Signature)(nil),  // 3: common.Signature
}
var file_kwil_common_tx_proto_depIdxs = []int32{
	0, // 0: common.Tx.payload_type:type_name -> common.PayloadType
	3, // 1: common.Tx.signature:type_name -> common.Signature
	1, // 2: common.Signature.signature_type:type_name -> common.SignatureType
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_kwil_common_tx_proto_init() }
func file_kwil_common_tx_proto_init() {
	if File_kwil_common_tx_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_kwil_common_tx_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Tx); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_kwil_common_tx_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Signature); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_kwil_common_tx_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_kwil_common_tx_proto_goTypes,
		DependencyIndexes: file_kwil_common_tx_proto_depIdxs,
		EnumInfos:         file_kwil_common_tx_proto_enumTypes,
		MessageInfos:      file_kwil_common_tx_proto_msgTypes,
	}.Build()
	File_kwil_common_tx_proto = out.File
	file_kwil_common_tx_proto_rawDesc = nil
	file_kwil_common_tx_proto_goTypes = nil
	file_kwil_common_tx_proto_depIdxs = nil
}
