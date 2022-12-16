// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.7
// source: kwil/pricingsvc/pricing.proto

package entity

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

type EstimateRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Caller string `protobuf:"bytes,1,opt,name=caller,proto3" json:"caller,omitempty"`
	// Types that are assignable to Request:
	//	*EstimateRequest_Deploy
	//	*EstimateRequest_Delete
	//	*EstimateRequest_Query
	Request isEstimateRequest_Request `protobuf_oneof:"request"`
}

func (x *EstimateRequest) Reset() {
	*x = EstimateRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EstimateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EstimateRequest) ProtoMessage() {}

func (x *EstimateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EstimateRequest.ProtoReflect.Descriptor instead.
func (*EstimateRequest) Descriptor() ([]byte, []int) {
	return file_kwil_pricingsvc_pricing_proto_rawDescGZIP(), []int{0}
}

func (x *EstimateRequest) GetCaller() string {
	if x != nil {
		return x.Caller
	}
	return ""
}

func (m *EstimateRequest) GetRequest() isEstimateRequest_Request {
	if m != nil {
		return m.Request
	}
	return nil
}

func (x *EstimateRequest) GetDeploy() *EstimateDeployRequest {
	if x, ok := x.GetRequest().(*EstimateRequest_Deploy); ok {
		return x.Deploy
	}
	return nil
}

func (x *EstimateRequest) GetDelete() *EstimateDeleteRequest {
	if x, ok := x.GetRequest().(*EstimateRequest_Delete); ok {
		return x.Delete
	}
	return nil
}

func (x *EstimateRequest) GetQuery() *EstimateQueryRequest {
	if x, ok := x.GetRequest().(*EstimateRequest_Query); ok {
		return x.Query
	}
	return nil
}

type isEstimateRequest_Request interface {
	isEstimateRequest_Request()
}

type EstimateRequest_Deploy struct {
	Deploy *EstimateDeployRequest `protobuf:"bytes,2,opt,name=deploy,proto3,oneof"`
}

type EstimateRequest_Delete struct {
	Delete *EstimateDeleteRequest `protobuf:"bytes,3,opt,name=delete,proto3,oneof"`
}

type EstimateRequest_Query struct {
	Query *EstimateQueryRequest `protobuf:"bytes,4,opt,name=query,proto3,oneof"`
}

func (*EstimateRequest_Deploy) isEstimateRequest_Request() {}

func (*EstimateRequest_Delete) isEstimateRequest_Request() {}

func (*EstimateRequest_Query) isEstimateRequest_Request() {}

type CRUDInput struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name  string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *CRUDInput) Reset() {
	*x = CRUDInput{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CRUDInput) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CRUDInput) ProtoMessage() {}

func (x *CRUDInput) ProtoReflect() protoreflect.Message {
	mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CRUDInput.ProtoReflect.Descriptor instead.
func (*CRUDInput) Descriptor() ([]byte, []int) {
	return file_kwil_pricingsvc_pricing_proto_rawDescGZIP(), []int{1}
}

func (x *CRUDInput) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *CRUDInput) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

type EstimateDeployRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Database string `protobuf:"bytes,1,opt,name=database,proto3" json:"database,omitempty"`
	Owner    string `protobuf:"bytes,2,opt,name=owner,proto3" json:"owner,omitempty"`
}

func (x *EstimateDeployRequest) Reset() {
	*x = EstimateDeployRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EstimateDeployRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EstimateDeployRequest) ProtoMessage() {}

func (x *EstimateDeployRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EstimateDeployRequest.ProtoReflect.Descriptor instead.
func (*EstimateDeployRequest) Descriptor() ([]byte, []int) {
	return file_kwil_pricingsvc_pricing_proto_rawDescGZIP(), []int{2}
}

func (x *EstimateDeployRequest) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

func (x *EstimateDeployRequest) GetOwner() string {
	if x != nil {
		return x.Owner
	}
	return ""
}

type EstimateDeleteRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Database string `protobuf:"bytes,1,opt,name=database,proto3" json:"database,omitempty"`
	Owner    string `protobuf:"bytes,2,opt,name=owner,proto3" json:"owner,omitempty"`
}

func (x *EstimateDeleteRequest) Reset() {
	*x = EstimateDeleteRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EstimateDeleteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EstimateDeleteRequest) ProtoMessage() {}

func (x *EstimateDeleteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EstimateDeleteRequest.ProtoReflect.Descriptor instead.
func (*EstimateDeleteRequest) Descriptor() ([]byte, []int) {
	return file_kwil_pricingsvc_pricing_proto_rawDescGZIP(), []int{3}
}

func (x *EstimateDeleteRequest) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

func (x *EstimateDeleteRequest) GetOwner() string {
	if x != nil {
		return x.Owner
	}
	return ""
}

type EstimateQueryRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Database string       `protobuf:"bytes,1,opt,name=database,proto3" json:"database,omitempty"`
	Owner    string       `protobuf:"bytes,2,opt,name=owner,proto3" json:"owner,omitempty"`
	Query    string       `protobuf:"bytes,3,opt,name=query,proto3" json:"query,omitempty"`
	Crud     []*CRUDInput `protobuf:"bytes,4,rep,name=crud,proto3" json:"crud,omitempty"`
}

func (x *EstimateQueryRequest) Reset() {
	*x = EstimateQueryRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EstimateQueryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EstimateQueryRequest) ProtoMessage() {}

func (x *EstimateQueryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EstimateQueryRequest.ProtoReflect.Descriptor instead.
func (*EstimateQueryRequest) Descriptor() ([]byte, []int) {
	return file_kwil_pricingsvc_pricing_proto_rawDescGZIP(), []int{4}
}

func (x *EstimateQueryRequest) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

func (x *EstimateQueryRequest) GetOwner() string {
	if x != nil {
		return x.Owner
	}
	return ""
}

func (x *EstimateQueryRequest) GetQuery() string {
	if x != nil {
		return x.Query
	}
	return ""
}

func (x *EstimateQueryRequest) GetCrud() []*CRUDInput {
	if x != nil {
		return x.Crud
	}
	return nil
}

type EstimateResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Price string `protobuf:"bytes,1,opt,name=price,proto3" json:"price,omitempty"`
}

func (x *EstimateResponse) Reset() {
	*x = EstimateResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EstimateResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EstimateResponse) ProtoMessage() {}

func (x *EstimateResponse) ProtoReflect() protoreflect.Message {
	mi := &file_kwil_pricingsvc_pricing_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EstimateResponse.ProtoReflect.Descriptor instead.
func (*EstimateResponse) Descriptor() ([]byte, []int) {
	return file_kwil_pricingsvc_pricing_proto_rawDescGZIP(), []int{5}
}

func (x *EstimateResponse) GetPrice() string {
	if x != nil {
		return x.Price
	}
	return ""
}

var File_kwil_pricingsvc_pricing_proto protoreflect.FileDescriptor

var file_kwil_pricingsvc_pricing_proto_rawDesc = []byte{
	0x0a, 0x1d, 0x6b, 0x77, 0x69, 0x6c, 0x2f, 0x70, 0x72, 0x69, 0x63, 0x69, 0x6e, 0x67, 0x73, 0x76,
	0x63, 0x2f, 0x70, 0x72, 0x69, 0x63, 0x69, 0x6e, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x06, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x22, 0xdc, 0x01, 0x0a, 0x0f, 0x45, 0x73, 0x74, 0x69,
	0x6d, 0x61, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x63,
	0x61, 0x6c, 0x6c, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x63, 0x61, 0x6c,
	0x6c, 0x65, 0x72, 0x12, 0x37, 0x0a, 0x06, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x2e, 0x45, 0x73, 0x74,
	0x69, 0x6d, 0x61, 0x74, 0x65, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x48, 0x00, 0x52, 0x06, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x12, 0x37, 0x0a, 0x06,
	0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x65,
	0x6e, 0x74, 0x69, 0x74, 0x79, 0x2e, 0x45, 0x73, 0x74, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x44, 0x65,
	0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x00, 0x52, 0x06, 0x64,
	0x65, 0x6c, 0x65, 0x74, 0x65, 0x12, 0x34, 0x0a, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x2e, 0x45, 0x73,
	0x74, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x48, 0x00, 0x52, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x42, 0x09, 0x0a, 0x07, 0x72,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x35, 0x0a, 0x09, 0x43, 0x52, 0x55, 0x44, 0x49, 0x6e,
	0x70, 0x75, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x49, 0x0a,
	0x15, 0x45, 0x73, 0x74, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x44, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61,
	0x73, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61,
	0x73, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x6f, 0x77, 0x6e, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x6f, 0x77, 0x6e, 0x65, 0x72, 0x22, 0x49, 0x0a, 0x15, 0x45, 0x73, 0x74, 0x69,
	0x6d, 0x61, 0x74, 0x65, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x12, 0x14, 0x0a,
	0x05, 0x6f, 0x77, 0x6e, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x6f, 0x77,
	0x6e, 0x65, 0x72, 0x22, 0x85, 0x01, 0x0a, 0x14, 0x45, 0x73, 0x74, 0x69, 0x6d, 0x61, 0x74, 0x65,
	0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1a, 0x0a, 0x08,
	0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08,
	0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x6f, 0x77, 0x6e, 0x65,
	0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x6f, 0x77, 0x6e, 0x65, 0x72, 0x12, 0x14,
	0x0a, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x71,
	0x75, 0x65, 0x72, 0x79, 0x12, 0x25, 0x0a, 0x04, 0x63, 0x72, 0x75, 0x64, 0x18, 0x04, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x11, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x2e, 0x43, 0x52, 0x55, 0x44,
	0x49, 0x6e, 0x70, 0x75, 0x74, 0x52, 0x04, 0x63, 0x72, 0x75, 0x64, 0x22, 0x28, 0x0a, 0x10, 0x45,
	0x73, 0x74, 0x69, 0x6d, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x14, 0x0a, 0x05, 0x70, 0x72, 0x69, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05,
	0x70, 0x72, 0x69, 0x63, 0x65, 0x42, 0x17, 0x5a, 0x15, 0x6b, 0x77, 0x69, 0x6c, 0x2f, 0x78, 0x2f,
	0x70, 0x72, 0x69, 0x63, 0x69, 0x6e, 0x67, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_kwil_pricingsvc_pricing_proto_rawDescOnce sync.Once
	file_kwil_pricingsvc_pricing_proto_rawDescData = file_kwil_pricingsvc_pricing_proto_rawDesc
)

func file_kwil_pricingsvc_pricing_proto_rawDescGZIP() []byte {
	file_kwil_pricingsvc_pricing_proto_rawDescOnce.Do(func() {
		file_kwil_pricingsvc_pricing_proto_rawDescData = protoimpl.X.CompressGZIP(file_kwil_pricingsvc_pricing_proto_rawDescData)
	})
	return file_kwil_pricingsvc_pricing_proto_rawDescData
}

var file_kwil_pricingsvc_pricing_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_kwil_pricingsvc_pricing_proto_goTypes = []interface{}{
	(*EstimateRequest)(nil),       // 0: entity.EstimateRequest
	(*CRUDInput)(nil),             // 1: entity.CRUDInput
	(*EstimateDeployRequest)(nil), // 2: entity.EstimateDeployRequest
	(*EstimateDeleteRequest)(nil), // 3: entity.EstimateDeleteRequest
	(*EstimateQueryRequest)(nil),  // 4: entity.EstimateQueryRequest
	(*EstimateResponse)(nil),      // 5: entity.EstimateResponse
}
var file_kwil_pricingsvc_pricing_proto_depIdxs = []int32{
	2, // 0: entity.EstimateRequest.deploy:type_name -> entity.EstimateDeployRequest
	3, // 1: entity.EstimateRequest.delete:type_name -> entity.EstimateDeleteRequest
	4, // 2: entity.EstimateRequest.query:type_name -> entity.EstimateQueryRequest
	1, // 3: entity.EstimateQueryRequest.crud:type_name -> entity.CRUDInput
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_kwil_pricingsvc_pricing_proto_init() }
func file_kwil_pricingsvc_pricing_proto_init() {
	if File_kwil_pricingsvc_pricing_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_kwil_pricingsvc_pricing_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EstimateRequest); i {
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
		file_kwil_pricingsvc_pricing_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CRUDInput); i {
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
		file_kwil_pricingsvc_pricing_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EstimateDeployRequest); i {
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
		file_kwil_pricingsvc_pricing_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EstimateDeleteRequest); i {
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
		file_kwil_pricingsvc_pricing_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EstimateQueryRequest); i {
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
		file_kwil_pricingsvc_pricing_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EstimateResponse); i {
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
	file_kwil_pricingsvc_pricing_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*EstimateRequest_Deploy)(nil),
		(*EstimateRequest_Delete)(nil),
		(*EstimateRequest_Query)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_kwil_pricingsvc_pricing_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_kwil_pricingsvc_pricing_proto_goTypes,
		DependencyIndexes: file_kwil_pricingsvc_pricing_proto_depIdxs,
		MessageInfos:      file_kwil_pricingsvc_pricing_proto_msgTypes,
	}.Build()
	File_kwil_pricingsvc_pricing_proto = out.File
	file_kwil_pricingsvc_pricing_proto_rawDesc = nil
	file_kwil_pricingsvc_pricing_proto_goTypes = nil
	file_kwil_pricingsvc_pricing_proto_depIdxs = nil
}
