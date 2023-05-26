// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v3.21.12
// source: catalog/src/model.proto

package protos

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

type DBType int32

const (
	DBType_BIGQUERY  DBType = 0
	DBType_SNOWFLAKE DBType = 1
	DBType_MONGO     DBType = 2
	DBType_POSTGRES  DBType = 3
)

// Enum value maps for DBType.
var (
	DBType_name = map[int32]string{
		0: "BIGQUERY",
		1: "SNOWFLAKE",
		2: "MONGO",
		3: "POSTGRES",
	}
	DBType_value = map[string]int32{
		"BIGQUERY":  0,
		"SNOWFLAKE": 1,
		"MONGO":     2,
		"POSTGRES":  3,
	}
)

func (x DBType) Enum() *DBType {
	p := new(DBType)
	*p = x
	return p
}

func (x DBType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (DBType) Descriptor() protoreflect.EnumDescriptor {
	return file_catalog_src_model_proto_enumTypes[0].Descriptor()
}

func (DBType) Type() protoreflect.EnumType {
	return &file_catalog_src_model_proto_enumTypes[0]
}

func (x DBType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use DBType.Descriptor instead.
func (DBType) EnumDescriptor() ([]byte, []int) {
	return file_catalog_src_model_proto_rawDescGZIP(), []int{0}
}

type SnowflakeConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AccountId    string `protobuf:"bytes,1,opt,name=account_id,json=accountId,proto3" json:"account_id,omitempty"`
	Username     string `protobuf:"bytes,2,opt,name=username,proto3" json:"username,omitempty"`
	PrivateKey   string `protobuf:"bytes,3,opt,name=private_key,json=privateKey,proto3" json:"private_key,omitempty"`
	Database     string `protobuf:"bytes,4,opt,name=database,proto3" json:"database,omitempty"`
	Warehouse    string `protobuf:"bytes,6,opt,name=warehouse,proto3" json:"warehouse,omitempty"`
	Role         string `protobuf:"bytes,7,opt,name=role,proto3" json:"role,omitempty"`
	QueryTimeout uint64 `protobuf:"varint,8,opt,name=query_timeout,json=queryTimeout,proto3" json:"query_timeout,omitempty"`
}

func (x *SnowflakeConfig) Reset() {
	*x = SnowflakeConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_catalog_src_model_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SnowflakeConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SnowflakeConfig) ProtoMessage() {}

func (x *SnowflakeConfig) ProtoReflect() protoreflect.Message {
	mi := &file_catalog_src_model_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SnowflakeConfig.ProtoReflect.Descriptor instead.
func (*SnowflakeConfig) Descriptor() ([]byte, []int) {
	return file_catalog_src_model_proto_rawDescGZIP(), []int{0}
}

func (x *SnowflakeConfig) GetAccountId() string {
	if x != nil {
		return x.AccountId
	}
	return ""
}

func (x *SnowflakeConfig) GetUsername() string {
	if x != nil {
		return x.Username
	}
	return ""
}

func (x *SnowflakeConfig) GetPrivateKey() string {
	if x != nil {
		return x.PrivateKey
	}
	return ""
}

func (x *SnowflakeConfig) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

func (x *SnowflakeConfig) GetWarehouse() string {
	if x != nil {
		return x.Warehouse
	}
	return ""
}

func (x *SnowflakeConfig) GetRole() string {
	if x != nil {
		return x.Role
	}
	return ""
}

func (x *SnowflakeConfig) GetQueryTimeout() uint64 {
	if x != nil {
		return x.QueryTimeout
	}
	return 0
}

type BigqueryConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AuthType                string `protobuf:"bytes,1,opt,name=auth_type,json=authType,proto3" json:"auth_type,omitempty"`
	ProjectId               string `protobuf:"bytes,2,opt,name=project_id,json=projectId,proto3" json:"project_id,omitempty"`
	PrivateKeyId            string `protobuf:"bytes,3,opt,name=private_key_id,json=privateKeyId,proto3" json:"private_key_id,omitempty"`
	PrivateKey              string `protobuf:"bytes,4,opt,name=private_key,json=privateKey,proto3" json:"private_key,omitempty"`
	ClientEmail             string `protobuf:"bytes,5,opt,name=client_email,json=clientEmail,proto3" json:"client_email,omitempty"`
	ClientId                string `protobuf:"bytes,6,opt,name=client_id,json=clientId,proto3" json:"client_id,omitempty"`
	AuthUri                 string `protobuf:"bytes,7,opt,name=auth_uri,json=authUri,proto3" json:"auth_uri,omitempty"`
	TokenUri                string `protobuf:"bytes,8,opt,name=token_uri,json=tokenUri,proto3" json:"token_uri,omitempty"`
	AuthProviderX509CertUrl string `protobuf:"bytes,9,opt,name=auth_provider_x509_cert_url,json=authProviderX509CertUrl,proto3" json:"auth_provider_x509_cert_url,omitempty"`
	ClientX509CertUrl       string `protobuf:"bytes,10,opt,name=client_x509_cert_url,json=clientX509CertUrl,proto3" json:"client_x509_cert_url,omitempty"`
	DatasetId               string `protobuf:"bytes,11,opt,name=dataset_id,json=datasetId,proto3" json:"dataset_id,omitempty"`
}

func (x *BigqueryConfig) Reset() {
	*x = BigqueryConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_catalog_src_model_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BigqueryConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BigqueryConfig) ProtoMessage() {}

func (x *BigqueryConfig) ProtoReflect() protoreflect.Message {
	mi := &file_catalog_src_model_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BigqueryConfig.ProtoReflect.Descriptor instead.
func (*BigqueryConfig) Descriptor() ([]byte, []int) {
	return file_catalog_src_model_proto_rawDescGZIP(), []int{1}
}

func (x *BigqueryConfig) GetAuthType() string {
	if x != nil {
		return x.AuthType
	}
	return ""
}

func (x *BigqueryConfig) GetProjectId() string {
	if x != nil {
		return x.ProjectId
	}
	return ""
}

func (x *BigqueryConfig) GetPrivateKeyId() string {
	if x != nil {
		return x.PrivateKeyId
	}
	return ""
}

func (x *BigqueryConfig) GetPrivateKey() string {
	if x != nil {
		return x.PrivateKey
	}
	return ""
}

func (x *BigqueryConfig) GetClientEmail() string {
	if x != nil {
		return x.ClientEmail
	}
	return ""
}

func (x *BigqueryConfig) GetClientId() string {
	if x != nil {
		return x.ClientId
	}
	return ""
}

func (x *BigqueryConfig) GetAuthUri() string {
	if x != nil {
		return x.AuthUri
	}
	return ""
}

func (x *BigqueryConfig) GetTokenUri() string {
	if x != nil {
		return x.TokenUri
	}
	return ""
}

func (x *BigqueryConfig) GetAuthProviderX509CertUrl() string {
	if x != nil {
		return x.AuthProviderX509CertUrl
	}
	return ""
}

func (x *BigqueryConfig) GetClientX509CertUrl() string {
	if x != nil {
		return x.ClientX509CertUrl
	}
	return ""
}

func (x *BigqueryConfig) GetDatasetId() string {
	if x != nil {
		return x.DatasetId
	}
	return ""
}

type MongoConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Username    string `protobuf:"bytes,1,opt,name=username,proto3" json:"username,omitempty"`
	Password    string `protobuf:"bytes,2,opt,name=password,proto3" json:"password,omitempty"`
	Clusterurl  string `protobuf:"bytes,3,opt,name=clusterurl,proto3" json:"clusterurl,omitempty"`
	Clusterport int32  `protobuf:"varint,4,opt,name=clusterport,proto3" json:"clusterport,omitempty"`
	Database    string `protobuf:"bytes,5,opt,name=database,proto3" json:"database,omitempty"`
}

func (x *MongoConfig) Reset() {
	*x = MongoConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_catalog_src_model_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MongoConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MongoConfig) ProtoMessage() {}

func (x *MongoConfig) ProtoReflect() protoreflect.Message {
	mi := &file_catalog_src_model_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MongoConfig.ProtoReflect.Descriptor instead.
func (*MongoConfig) Descriptor() ([]byte, []int) {
	return file_catalog_src_model_proto_rawDescGZIP(), []int{2}
}

func (x *MongoConfig) GetUsername() string {
	if x != nil {
		return x.Username
	}
	return ""
}

func (x *MongoConfig) GetPassword() string {
	if x != nil {
		return x.Password
	}
	return ""
}

func (x *MongoConfig) GetClusterurl() string {
	if x != nil {
		return x.Clusterurl
	}
	return ""
}

func (x *MongoConfig) GetClusterport() int32 {
	if x != nil {
		return x.Clusterport
	}
	return 0
}

func (x *MongoConfig) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

type PostgresConfig struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Host     string `protobuf:"bytes,1,opt,name=host,proto3" json:"host,omitempty"`
	Port     uint32 `protobuf:"varint,2,opt,name=port,proto3" json:"port,omitempty"`
	User     string `protobuf:"bytes,3,opt,name=user,proto3" json:"user,omitempty"`
	Password string `protobuf:"bytes,4,opt,name=password,proto3" json:"password,omitempty"`
	Database string `protobuf:"bytes,5,opt,name=database,proto3" json:"database,omitempty"`
}

func (x *PostgresConfig) Reset() {
	*x = PostgresConfig{}
	if protoimpl.UnsafeEnabled {
		mi := &file_catalog_src_model_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PostgresConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PostgresConfig) ProtoMessage() {}

func (x *PostgresConfig) ProtoReflect() protoreflect.Message {
	mi := &file_catalog_src_model_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PostgresConfig.ProtoReflect.Descriptor instead.
func (*PostgresConfig) Descriptor() ([]byte, []int) {
	return file_catalog_src_model_proto_rawDescGZIP(), []int{3}
}

func (x *PostgresConfig) GetHost() string {
	if x != nil {
		return x.Host
	}
	return ""
}

func (x *PostgresConfig) GetPort() uint32 {
	if x != nil {
		return x.Port
	}
	return 0
}

func (x *PostgresConfig) GetUser() string {
	if x != nil {
		return x.User
	}
	return ""
}

func (x *PostgresConfig) GetPassword() string {
	if x != nil {
		return x.Password
	}
	return ""
}

func (x *PostgresConfig) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

type Peer struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Type DBType `protobuf:"varint,2,opt,name=type,proto3,enum=peerdb.peers.DBType" json:"type,omitempty"`
	// Types that are assignable to Config:
	//
	//	*Peer_SnowflakeConfig
	//	*Peer_BigqueryConfig
	//	*Peer_MongoConfig
	//	*Peer_PostgresConfig
	Config isPeer_Config `protobuf_oneof:"config"`
}

func (x *Peer) Reset() {
	*x = Peer{}
	if protoimpl.UnsafeEnabled {
		mi := &file_catalog_src_model_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Peer) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Peer) ProtoMessage() {}

func (x *Peer) ProtoReflect() protoreflect.Message {
	mi := &file_catalog_src_model_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Peer.ProtoReflect.Descriptor instead.
func (*Peer) Descriptor() ([]byte, []int) {
	return file_catalog_src_model_proto_rawDescGZIP(), []int{4}
}

func (x *Peer) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Peer) GetType() DBType {
	if x != nil {
		return x.Type
	}
	return DBType_BIGQUERY
}

func (m *Peer) GetConfig() isPeer_Config {
	if m != nil {
		return m.Config
	}
	return nil
}

func (x *Peer) GetSnowflakeConfig() *SnowflakeConfig {
	if x, ok := x.GetConfig().(*Peer_SnowflakeConfig); ok {
		return x.SnowflakeConfig
	}
	return nil
}

func (x *Peer) GetBigqueryConfig() *BigqueryConfig {
	if x, ok := x.GetConfig().(*Peer_BigqueryConfig); ok {
		return x.BigqueryConfig
	}
	return nil
}

func (x *Peer) GetMongoConfig() *MongoConfig {
	if x, ok := x.GetConfig().(*Peer_MongoConfig); ok {
		return x.MongoConfig
	}
	return nil
}

func (x *Peer) GetPostgresConfig() *PostgresConfig {
	if x, ok := x.GetConfig().(*Peer_PostgresConfig); ok {
		return x.PostgresConfig
	}
	return nil
}

type isPeer_Config interface {
	isPeer_Config()
}

type Peer_SnowflakeConfig struct {
	SnowflakeConfig *SnowflakeConfig `protobuf:"bytes,3,opt,name=snowflake_config,json=snowflakeConfig,proto3,oneof"`
}

type Peer_BigqueryConfig struct {
	BigqueryConfig *BigqueryConfig `protobuf:"bytes,4,opt,name=bigquery_config,json=bigqueryConfig,proto3,oneof"`
}

type Peer_MongoConfig struct {
	MongoConfig *MongoConfig `protobuf:"bytes,5,opt,name=mongo_config,json=mongoConfig,proto3,oneof"`
}

type Peer_PostgresConfig struct {
	PostgresConfig *PostgresConfig `protobuf:"bytes,6,opt,name=postgres_config,json=postgresConfig,proto3,oneof"`
}

func (*Peer_SnowflakeConfig) isPeer_Config() {}

func (*Peer_BigqueryConfig) isPeer_Config() {}

func (*Peer_MongoConfig) isPeer_Config() {}

func (*Peer_PostgresConfig) isPeer_Config() {}

var File_catalog_src_model_proto protoreflect.FileDescriptor

var file_catalog_src_model_proto_rawDesc = []byte{
	0x0a, 0x17, 0x63, 0x61, 0x74, 0x61, 0x6c, 0x6f, 0x67, 0x2f, 0x73, 0x72, 0x63, 0x2f, 0x6d, 0x6f,
	0x64, 0x65, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0c, 0x70, 0x65, 0x65, 0x72, 0x64,
	0x62, 0x2e, 0x70, 0x65, 0x65, 0x72, 0x73, 0x22, 0xe0, 0x01, 0x0a, 0x0f, 0x53, 0x6e, 0x6f, 0x77,
	0x66, 0x6c, 0x61, 0x6b, 0x65, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12, 0x1d, 0x0a, 0x0a, 0x61,
	0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x09, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x75, 0x73,
	0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x75, 0x73,
	0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x70, 0x72, 0x69, 0x76, 0x61, 0x74,
	0x65, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x70, 0x72, 0x69,
	0x76, 0x61, 0x74, 0x65, 0x4b, 0x65, 0x79, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62,
	0x61, 0x73, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62,
	0x61, 0x73, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x77, 0x61, 0x72, 0x65, 0x68, 0x6f, 0x75, 0x73, 0x65,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x77, 0x61, 0x72, 0x65, 0x68, 0x6f, 0x75, 0x73,
	0x65, 0x12, 0x12, 0x0a, 0x04, 0x72, 0x6f, 0x6c, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x72, 0x6f, 0x6c, 0x65, 0x12, 0x23, 0x0a, 0x0d, 0x71, 0x75, 0x65, 0x72, 0x79, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0x08, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0c, 0x71, 0x75,
	0x65, 0x72, 0x79, 0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x22, 0x99, 0x03, 0x0a, 0x0e, 0x42,
	0x69, 0x67, 0x71, 0x75, 0x65, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12, 0x1b, 0x0a,
	0x09, 0x61, 0x75, 0x74, 0x68, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x08, 0x61, 0x75, 0x74, 0x68, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x70, 0x72,
	0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09,
	0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x49, 0x64, 0x12, 0x24, 0x0a, 0x0e, 0x70, 0x72, 0x69,
	0x76, 0x61, 0x74, 0x65, 0x5f, 0x6b, 0x65, 0x79, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0c, 0x70, 0x72, 0x69, 0x76, 0x61, 0x74, 0x65, 0x4b, 0x65, 0x79, 0x49, 0x64, 0x12,
	0x1f, 0x0a, 0x0b, 0x70, 0x72, 0x69, 0x76, 0x61, 0x74, 0x65, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x70, 0x72, 0x69, 0x76, 0x61, 0x74, 0x65, 0x4b, 0x65, 0x79,
	0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x65, 0x6d, 0x61, 0x69, 0x6c,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x45, 0x6d,
	0x61, 0x69, 0x6c, 0x12, 0x1b, 0x0a, 0x09, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x49, 0x64,
	0x12, 0x19, 0x0a, 0x08, 0x61, 0x75, 0x74, 0x68, 0x5f, 0x75, 0x72, 0x69, 0x18, 0x07, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x07, 0x61, 0x75, 0x74, 0x68, 0x55, 0x72, 0x69, 0x12, 0x1b, 0x0a, 0x09, 0x74,
	0x6f, 0x6b, 0x65, 0x6e, 0x5f, 0x75, 0x72, 0x69, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08,
	0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x55, 0x72, 0x69, 0x12, 0x3c, 0x0a, 0x1b, 0x61, 0x75, 0x74, 0x68,
	0x5f, 0x70, 0x72, 0x6f, 0x76, 0x69, 0x64, 0x65, 0x72, 0x5f, 0x78, 0x35, 0x30, 0x39, 0x5f, 0x63,
	0x65, 0x72, 0x74, 0x5f, 0x75, 0x72, 0x6c, 0x18, 0x09, 0x20, 0x01, 0x28, 0x09, 0x52, 0x17, 0x61,
	0x75, 0x74, 0x68, 0x50, 0x72, 0x6f, 0x76, 0x69, 0x64, 0x65, 0x72, 0x58, 0x35, 0x30, 0x39, 0x43,
	0x65, 0x72, 0x74, 0x55, 0x72, 0x6c, 0x12, 0x2f, 0x0a, 0x14, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74,
	0x5f, 0x78, 0x35, 0x30, 0x39, 0x5f, 0x63, 0x65, 0x72, 0x74, 0x5f, 0x75, 0x72, 0x6c, 0x18, 0x0a,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x11, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x58, 0x35, 0x30, 0x39,
	0x43, 0x65, 0x72, 0x74, 0x55, 0x72, 0x6c, 0x12, 0x1d, 0x0a, 0x0a, 0x64, 0x61, 0x74, 0x61, 0x73,
	0x65, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x64, 0x61, 0x74,
	0x61, 0x73, 0x65, 0x74, 0x49, 0x64, 0x22, 0xa3, 0x01, 0x0a, 0x0b, 0x4d, 0x6f, 0x6e, 0x67, 0x6f,
	0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12, 0x1a, 0x0a, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61,
	0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x75, 0x73, 0x65, 0x72, 0x6e, 0x61,
	0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x12, 0x1e,
	0x0a, 0x0a, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x75, 0x72, 0x6c, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0a, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x75, 0x72, 0x6c, 0x12, 0x20,
	0x0a, 0x0b, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x70, 0x6f, 0x72, 0x74, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x0b, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x70, 0x6f, 0x72, 0x74,
	0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x18, 0x05, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x22, 0x84, 0x01, 0x0a,
	0x0e, 0x50, 0x6f, 0x73, 0x74, 0x67, 0x72, 0x65, 0x73, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12,
	0x12, 0x0a, 0x04, 0x68, 0x6f, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x68,
	0x6f, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0d, 0x52, 0x04, 0x70, 0x6f, 0x72, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x75, 0x73, 0x65, 0x72, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75, 0x73, 0x65, 0x72, 0x12, 0x1a, 0x0a, 0x08, 0x70,
	0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x70,
	0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62,
	0x61, 0x73, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62,
	0x61, 0x73, 0x65, 0x22, 0xec, 0x02, 0x0a, 0x04, 0x50, 0x65, 0x65, 0x72, 0x12, 0x12, 0x0a, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65,
	0x12, 0x28, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x14,
	0x2e, 0x70, 0x65, 0x65, 0x72, 0x64, 0x62, 0x2e, 0x70, 0x65, 0x65, 0x72, 0x73, 0x2e, 0x44, 0x42,
	0x54, 0x79, 0x70, 0x65, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x4a, 0x0a, 0x10, 0x73, 0x6e,
	0x6f, 0x77, 0x66, 0x6c, 0x61, 0x6b, 0x65, 0x5f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x70, 0x65, 0x65, 0x72, 0x64, 0x62, 0x2e, 0x70, 0x65,
	0x65, 0x72, 0x73, 0x2e, 0x53, 0x6e, 0x6f, 0x77, 0x66, 0x6c, 0x61, 0x6b, 0x65, 0x43, 0x6f, 0x6e,
	0x66, 0x69, 0x67, 0x48, 0x00, 0x52, 0x0f, 0x73, 0x6e, 0x6f, 0x77, 0x66, 0x6c, 0x61, 0x6b, 0x65,
	0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12, 0x47, 0x0a, 0x0f, 0x62, 0x69, 0x67, 0x71, 0x75, 0x65,
	0x72, 0x79, 0x5f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1c, 0x2e, 0x70, 0x65, 0x65, 0x72, 0x64, 0x62, 0x2e, 0x70, 0x65, 0x65, 0x72, 0x73, 0x2e, 0x42,
	0x69, 0x67, 0x71, 0x75, 0x65, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x48, 0x00, 0x52,
	0x0e, 0x62, 0x69, 0x67, 0x71, 0x75, 0x65, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12,
	0x3e, 0x0a, 0x0c, 0x6d, 0x6f, 0x6e, 0x67, 0x6f, 0x5f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x70, 0x65, 0x65, 0x72, 0x64, 0x62, 0x2e, 0x70,
	0x65, 0x65, 0x72, 0x73, 0x2e, 0x4d, 0x6f, 0x6e, 0x67, 0x6f, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67,
	0x48, 0x00, 0x52, 0x0b, 0x6d, 0x6f, 0x6e, 0x67, 0x6f, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12,
	0x47, 0x0a, 0x0f, 0x70, 0x6f, 0x73, 0x74, 0x67, 0x72, 0x65, 0x73, 0x5f, 0x63, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x70, 0x65, 0x65, 0x72, 0x64,
	0x62, 0x2e, 0x70, 0x65, 0x65, 0x72, 0x73, 0x2e, 0x50, 0x6f, 0x73, 0x74, 0x67, 0x72, 0x65, 0x73,
	0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x48, 0x00, 0x52, 0x0e, 0x70, 0x6f, 0x73, 0x74, 0x67, 0x72,
	0x65, 0x73, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x42, 0x08, 0x0a, 0x06, 0x63, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x2a, 0x3e, 0x0a, 0x06, 0x44, 0x42, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0c, 0x0a, 0x08,
	0x42, 0x49, 0x47, 0x51, 0x55, 0x45, 0x52, 0x59, 0x10, 0x00, 0x12, 0x0d, 0x0a, 0x09, 0x53, 0x4e,
	0x4f, 0x57, 0x46, 0x4c, 0x41, 0x4b, 0x45, 0x10, 0x01, 0x12, 0x09, 0x0a, 0x05, 0x4d, 0x4f, 0x4e,
	0x47, 0x4f, 0x10, 0x02, 0x12, 0x0c, 0x0a, 0x08, 0x50, 0x4f, 0x53, 0x54, 0x47, 0x52, 0x45, 0x53,
	0x10, 0x03, 0x42, 0x12, 0x5a, 0x10, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x64, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_catalog_src_model_proto_rawDescOnce sync.Once
	file_catalog_src_model_proto_rawDescData = file_catalog_src_model_proto_rawDesc
)

func file_catalog_src_model_proto_rawDescGZIP() []byte {
	file_catalog_src_model_proto_rawDescOnce.Do(func() {
		file_catalog_src_model_proto_rawDescData = protoimpl.X.CompressGZIP(file_catalog_src_model_proto_rawDescData)
	})
	return file_catalog_src_model_proto_rawDescData
}

var file_catalog_src_model_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_catalog_src_model_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_catalog_src_model_proto_goTypes = []interface{}{
	(DBType)(0),             // 0: peerdb.peers.DBType
	(*SnowflakeConfig)(nil), // 1: peerdb.peers.SnowflakeConfig
	(*BigqueryConfig)(nil),  // 2: peerdb.peers.BigqueryConfig
	(*MongoConfig)(nil),     // 3: peerdb.peers.MongoConfig
	(*PostgresConfig)(nil),  // 4: peerdb.peers.PostgresConfig
	(*Peer)(nil),            // 5: peerdb.peers.Peer
}
var file_catalog_src_model_proto_depIdxs = []int32{
	0, // 0: peerdb.peers.Peer.type:type_name -> peerdb.peers.DBType
	1, // 1: peerdb.peers.Peer.snowflake_config:type_name -> peerdb.peers.SnowflakeConfig
	2, // 2: peerdb.peers.Peer.bigquery_config:type_name -> peerdb.peers.BigqueryConfig
	3, // 3: peerdb.peers.Peer.mongo_config:type_name -> peerdb.peers.MongoConfig
	4, // 4: peerdb.peers.Peer.postgres_config:type_name -> peerdb.peers.PostgresConfig
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_catalog_src_model_proto_init() }
func file_catalog_src_model_proto_init() {
	if File_catalog_src_model_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_catalog_src_model_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SnowflakeConfig); i {
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
		file_catalog_src_model_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BigqueryConfig); i {
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
		file_catalog_src_model_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MongoConfig); i {
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
		file_catalog_src_model_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PostgresConfig); i {
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
		file_catalog_src_model_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Peer); i {
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
	file_catalog_src_model_proto_msgTypes[4].OneofWrappers = []interface{}{
		(*Peer_SnowflakeConfig)(nil),
		(*Peer_BigqueryConfig)(nil),
		(*Peer_MongoConfig)(nil),
		(*Peer_PostgresConfig)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_catalog_src_model_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_catalog_src_model_proto_goTypes,
		DependencyIndexes: file_catalog_src_model_proto_depIdxs,
		EnumInfos:         file_catalog_src_model_proto_enumTypes,
		MessageInfos:      file_catalog_src_model_proto_msgTypes,
	}.Build()
	File_catalog_src_model_proto = out.File
	file_catalog_src_model_proto_rawDesc = nil
	file_catalog_src_model_proto_goTypes = nil
	file_catalog_src_model_proto_depIdxs = nil
}
