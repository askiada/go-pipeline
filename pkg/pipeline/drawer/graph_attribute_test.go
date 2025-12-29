package drawer_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/askiada/go-pipeline/pkg/pipeline/drawer"
)

func TestGraphAttributeSetsAttributes(t *testing.T) {
	t.Parallel()

	opt := drawer.GraphAttribute("rankdir", "LR")
	optValue := reflect.ValueOf(opt)

	argType := optValue.Type().In(0)
	argValue := reflect.New(argType.Elem())
	attrField := argValue.Elem().FieldByName("Attributes")
	require.True(t, attrField.IsValid())
	require.True(t, attrField.CanSet())

	attrField.Set(reflect.MakeMap(attrField.Type()))
	optValue.Call([]reflect.Value{argValue})

	attrs, ok := attrField.Interface().(map[string]string)
	require.True(t, ok)
	require.Equal(t, "LR", attrs["rankdir"])
}
