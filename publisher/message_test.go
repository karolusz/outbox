//go:build testing

package publisher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONBMap_Value(t *testing.T) {
	var j JSONBMap
	val, err := j.Value()
	require.NoError(t, err)
	require.Equal(t, []byte("{}"), val)

	j = JSONBMap{"foo": "bar", "baz": "qux"}
	val, err = j.Value()
	require.NoError(t, err)
	require.Contains(t, string(val.([]byte)), `"foo":"bar"`)
	require.Contains(t, string(val.([]byte)), `"baz":"qux"`)
}

func TestJSONBMap_ScanByteMap(t *testing.T) {
	var j JSONBMap
	err := j.Scan([]byte(`{"foo":"bar"}`))
	require.NoError(t, err)
	require.Equal(t, JSONBMap{"foo": "bar"}, j)
}

func TestJSONBMap_ScanFromString(t *testing.T) {
	var j JSONBMap
	err := j.Scan(`{"baz":"qux"}`)
	require.NoError(t, err)
	require.Equal(t, JSONBMap{"baz": "qux"}, j)
}

func TestJSONBMap_ScanFromNil(t *testing.T) {
	var j JSONBMap
	err := j.Scan(nil)
	require.NoError(t, err)
	require.Equal(t, JSONBMap{}, j)
}

func TestJSONBMap_ScanUnsupported(t *testing.T) {
	var j JSONBMap
	err := j.Scan(123)
	require.Error(t, err)
}
