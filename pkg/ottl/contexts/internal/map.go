// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/internal"

import (
	"fmt"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func GetMapValue(m pcommon.Map, keys []ottl.Key) (interface{}, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("cannot get map value without key")
	}
	if keys[0].String == nil {
		return nil, fmt.Errorf("non-string indexing is not supported")
	}

	val, ok := m.Get(*keys[0].String)
	if !ok {
		return nil, nil
	}
	return getIndexableValue(val, keys[1:])
}

func GetCacheMapValue(m map[string]any, keys []ottl.Key) (interface{}, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("cannot get map value without key")
	}
	if keys[0].String == nil {
		return nil, fmt.Errorf("non-string indexing is not supported")
	}

	val, ok := m[*keys[0].String]
	if !ok {
		return nil, nil
	}
	//return getIndexableValue(val, keys[1:])
	
}

func SetMapValue(m pcommon.Map, keys []ottl.Key, val interface{}) error {
	if len(keys) == 0 {
		return fmt.Errorf("cannot set map value without key")
	}
	if keys[0].String == nil {
		return fmt.Errorf("non-string indexing is not supported")
	}

	currentValue, ok := m.Get(*keys[0].String)
	if !ok {
		currentValue = m.PutEmpty(*keys[0].String)
	}

	return setIndexableValue(currentValue, val, keys[1:])
}

func SetCacheMapValue(m map[string]any, keys []ottl.Key, val interface{}) error {
	if len(keys) == 0 {
		return fmt.Errorf("cannot set map value without key")
	}
	if keys[0].String == nil {
		return fmt.Errorf("non-string indexing is not supported")
	}
	if len(keys) == 1 {
		m[*keys[0].String] = val
		return nil
	}

	currentValue, ok := m[*keys[0].String]
	if !ok {
		currentValue = struct{}{}
		m[*keys[0].String] = currentValue
	}

	return setIndexableMap(m, currentValue, val, keys[1:])
}

func setIndexableMap(m map[string]any, currentValue any, val any, keys []ottl.Key) error {
	for i := 0; i < len(keys); i++ {
		switch v := currentValue.(type) {
		case pcommon.Map:
		case pcommon.Value:
			return setIndexableValue(v, val, keys[i+1:])
		case map[string]any:
		default:
			return fmt.Errorf("type %T does not support string indexing", v)

		}
	}
	return nil
}
