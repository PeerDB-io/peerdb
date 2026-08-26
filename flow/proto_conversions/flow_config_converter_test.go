package proto_conversions

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

// Field numbers deliberately present in only one of the two messages. Anything
// else missing is a forgotten field, not a design decision.
var (
	removedFromCore = map[protoreflect.FieldNumber]string{}
	coreOnly        = map[protoreflect.FieldNumber]string{}
)

// TestFlowConnectionConfigsEquivalence enforces what codegen used to: that
// FlowConnectionConfigs and FlowConnectionConfigsCore stay in sync. Since
// copyFieldsByNumber copies by field number, shared numbers must agree on name,
// type, cardinality and oneof membership.
func TestFlowConnectionConfigsEquivalence(t *testing.T) {
	apiFields := (&protos.FlowConnectionConfigs{}).ProtoReflect().Descriptor().Fields()
	coreFields := (&protos.FlowConnectionConfigsCore{}).ProtoReflect().Descriptor().Fields()

	for i := range apiFields.Len() {
		api := apiFields.Get(i)
		core := coreFields.ByNumber(api.Number())
		if core == nil {
			require.Containsf(t, removedFromCore, api.Number(),
				"field %s (%d) is missing from FlowConnectionConfigsCore: add it there, or record it in removedFromCore",
				api.Name(), api.Number())
			continue
		}
		requireSameShape(t, api, core)
	}

	for i := range coreFields.Len() {
		core := coreFields.Get(i)
		if apiFields.ByNumber(core.Number()) == nil {
			require.Containsf(t, coreOnly, core.Number(),
				"field %s (%d) is missing from FlowConnectionConfigs: add it there, or record it in coreOnly",
				core.Name(), core.Number())
		}
	}
}

func TestFlowConnectionConfigsRoundTrip(t *testing.T) {
	skipValidation := true
	api := &protos.FlowConnectionConfigs{
		FlowJobName:        "round_trip_test",
		TableMappings:      []*protos.TableMapping{{SourceTableIdentifier: "src", DestinationTableIdentifier: "dst"}},
		MaxBatchSize:       100,
		IdleTimeoutSeconds: 30,
		DoInitialSnapshot:  true,
		System:             protos.TypeSystem_PG,
		SourceName:         "source_peer",
		DestinationName:    "destination_peer",
		Env:                map[string]string{"key": "value"},
		Version:            7,
		Flags:              []string{"flag_a", "flag_b"},
		SkipValidation:     &skipValidation,
	}

	core := FlowConnectionConfigsToCore(api)
	require.NotNil(t, core)
	require.Equal(t, api.FlowJobName, core.FlowJobName)
	require.Equal(t, api.Env, core.Env)
	require.Equal(t, api.Flags, core.Flags)
	require.Equal(t, api.SkipValidation, core.SkipValidation)

	roundTripped := &protos.FlowConnectionConfigs{}
	copyFieldsByNumber(core.ProtoReflect(), roundTripped.ProtoReflect())
	require.Truef(t, proto.Equal(api, roundTripped), "round trip mismatch:\nbefore: %v\nafter:  %v", api, roundTripped)
}

func TestFlowConnectionConfigsNil(t *testing.T) {
	require.Nil(t, FlowConnectionConfigsToCore(nil))
}

func requireSameShape(t *testing.T, api, core protoreflect.FieldDescriptor) {
	t.Helper()

	field := fmt.Sprintf("field %s (%d)", api.Name(), api.Number())
	require.Equalf(t, api.Name(), core.Name(), "%s has a different name in Core: %s", field, core.Name())
	require.Equalf(t, api.Kind(), core.Kind(), "%s has a different type in Core", field)
	require.Equalf(t, api.Cardinality(), core.Cardinality(), "%s has a different cardinality in Core", field)
	require.Equalf(t, api.HasOptionalKeyword(), core.HasOptionalKeyword(), "%s differs on the optional keyword in Core", field)
	require.Equalf(t, oneofName(api), oneofName(core), "%s belongs to a different oneof in Core", field)
	require.Equalf(t, api.IsMap(), core.IsMap(), "%s differs on being a map in Core", field)

	switch {
	case api.IsMap():
		// a map field's entry message is synthesized per parent, so its name
		// always differs - compare the key and value shapes instead
		requireSameShape(t, api.MapKey(), core.MapKey())
		requireSameShape(t, api.MapValue(), core.MapValue())
	case api.Kind() == protoreflect.MessageKind, api.Kind() == protoreflect.GroupKind:
		require.Equalf(t, api.Message().FullName(), core.Message().FullName(), "%s has a different message type in Core", field)
	case api.Kind() == protoreflect.EnumKind:
		require.Equalf(t, api.Enum().FullName(), core.Enum().FullName(), "%s has a different enum type in Core", field)
	}
}

// oneofName ignores the synthetic oneofs protoc wraps `optional` fields in,
// which HasOptionalKeyword already covers.
func oneofName(fd protoreflect.FieldDescriptor) protoreflect.Name {
	if oneof := fd.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() {
		return oneof.Name()
	}
	return ""
}
