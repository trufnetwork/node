package hcl

import (
	"reflect"
	"testing"

	"kwil/x/schemadef/sqlschema"

	"github.com/stretchr/testify/require"
)

func TestTypePrint(t *testing.T) {
	intSpec := &TypeSpec{
		Name: "int",
		T:    "int",
		Attributes: []*TypeAttr{
			unsignedTypeAttr(),
		},
	}
	for _, tt := range []struct {
		spec     *TypeSpec
		typ      *Type
		expected string
	}{
		{
			spec:     intSpec,
			typ:      &Type{T: "int"},
			expected: "int",
		},
		{
			spec:     intSpec,
			typ:      &Type{T: "int", Attrs: []*Attr{LitAttr("unsigned", "true")}},
			expected: "int unsigned",
		},
		{
			spec: &TypeSpec{
				Name:       "float",
				T:          "float",
				Attributes: []*TypeAttr{unsignedTypeAttr()},
			},
			typ:      &Type{T: "float", Attrs: []*Attr{LitAttr("unsigned", "true")}},
			expected: "float unsigned",
		},
		{
			spec: &TypeSpec{
				T:    "varchar",
				Name: "varchar",
				Attributes: []*TypeAttr{
					{Name: "size", Kind: reflect.Int, Required: true},
				},
			},
			typ:      &Type{T: "varchar", Attrs: []*Attr{LitAttr("size", "255")}},
			expected: "varchar(255)",
		},
	} {
		t.Run(tt.expected, func(t *testing.T) {
			r := &TypeRegistry{}
			err := r.Register(tt.spec)
			require.NoError(t, err)
			s, err := r.PrintType(tt.typ)
			require.NoError(t, err)
			require.EqualValues(t, tt.expected, s)
		})
	}
}

func TestRegistry(t *testing.T) {
	r := &TypeRegistry{}
	text := &TypeSpec{Name: "text", T: "text"}
	err := r.Register(text)
	require.NoError(t, err)
	err = r.Register(text)
	require.EqualError(t, err, `spec: type with T of "text" already registered`)
	spec, ok := r.findName("text")
	require.True(t, ok)
	require.EqualValues(t, spec, text)
}

func TestValidSpec(t *testing.T) {
	registry := &TypeRegistry{}
	err := registry.Register(&TypeSpec{
		Name: "X",
		T:    "X",
		Attributes: []*TypeAttr{
			{Name: "a", Required: false, Kind: reflect.Slice},
			{Name: "b", Required: true},
		},
	})
	require.EqualError(t, err, `spec: invalid typespec "X": attr "a" is of kind slice but not last`)
	err = registry.Register(&TypeSpec{
		Name: "Z",
		T:    "Z",
		Attributes: []*TypeAttr{
			{Name: "b", Required: true},
			{Name: "a", Required: false, Kind: reflect.Slice},
		},
	})
	require.NoError(t, err)
	err = registry.Register(&TypeSpec{
		Name: "Z2",
		T:    "Z2",
		Attributes: []*TypeAttr{
			{Name: "a", Required: false, Kind: reflect.Slice},
		},
	})
	require.NoError(t, err)
	err = registry.Register(&TypeSpec{
		Name: "X",
		T:    "X",
		Attributes: []*TypeAttr{
			{Name: "a", Required: false},
			{Name: "b", Required: true},
		},
	})
	require.EqualError(t, err, `spec: invalid typespec "X": attr "b" required after optional attr`)
	err = registry.Register(&TypeSpec{
		Name: "X",
		T:    "X",
		Attributes: []*TypeAttr{
			{Name: "a", Required: true},
			{Name: "b", Required: false},
		},
	})
	require.NoError(t, err)
	err = registry.Register(&TypeSpec{
		Name: "Y",
		T:    "Y",
		Attributes: []*TypeAttr{
			{Name: "a", Required: false},
			{Name: "b", Required: false},
		},
	})
	require.NoError(t, err)
}

func TestRegistryConvert(t *testing.T) {
	r := &TypeRegistry{}
	err := r.Register(
		NewTypeSpec("varchar", WithAttributes(SizeTypeAttr(true))),
		NewTypeSpec("int", WithAttributes(unsignedTypeAttr())),
		NewTypeSpec(
			"decimal",
			WithAttributes(
				&TypeAttr{
					Name:     "precision",
					Kind:     reflect.Int,
					Required: false,
				},
				&TypeAttr{
					Name:     "scale",
					Kind:     reflect.Int,
					Required: false,
				},
			),
		),
		NewTypeSpec("enum", WithAttributes(&TypeAttr{
			Name:     "values",
			Kind:     reflect.Slice,
			Required: true,
		})),
	)
	require.NoError(t, err)
	for _, tt := range []struct {
		typ         sqlschema.Type
		expected    *Type
		expectedErr string
	}{
		{
			typ:      &sqlschema.StringType{T: "varchar", Size: 255},
			expected: &Type{T: "varchar", Attrs: []*Attr{LitAttr("size", "255")}},
		},
		{
			typ:      &sqlschema.IntegerType{T: "int", Unsigned: true},
			expected: &Type{T: "int", Attrs: []*Attr{LitAttr("unsigned", "true")}},
		},
		{
			typ:      &sqlschema.IntegerType{T: "int", Unsigned: true},
			expected: &Type{T: "int", Attrs: []*Attr{LitAttr("unsigned", "true")}},
		},
		{
			typ: &sqlschema.EnumType{T: "enum", Values: []string{"on", "off"}},
			expected: &Type{T: "enum", Attrs: []*Attr{
				ListAttr("values", `"on"`, `"off"`),
			}},
		},
		{
			typ:         nil,
			expected:    &Type{},
			expectedErr: "spec: invalid sqlschema.Type on Convert",
		},
	} {
		t.Run(tt.expected.T, func(t *testing.T) {
			convert, err := r.Convert(tt.typ)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, tt.expected, convert)
		})
	}
}

func unsignedTypeAttr() *TypeAttr {
	return &TypeAttr{
		Name: "unsigned",
		Kind: reflect.Bool,
	}
}