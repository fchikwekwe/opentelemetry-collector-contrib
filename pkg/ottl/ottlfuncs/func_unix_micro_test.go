// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ottlfuncs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

func Test_TimeUnixMicro(t *testing.T) {
	tests := []struct {
		name     string
		time     ottl.TimeGetter[interface{}]
		expected int64
	}{
		{
			name: "January 1, 2023",
			time: &ottl.StandardTimeGetter[interface{}]{
				Getter: func(ctx context.Context, tCtx interface{}) (interface{}, error) {
					return time.Date(2023, 1, 1, 0, 0, 0, 0, time.Local), nil
				},
			},
			expected: 1672549200000000,
		},
		{
			name: "April 30, 2001, 3pm",
			time: &ottl.StandardTimeGetter[interface{}]{
				Getter: func(ctx context.Context, tCtx interface{}) (interface{}, error) {
					return time.Date(2001, 4, 30, 15, 0, 0, 0, time.Local), nil
				},
			},
			expected: 988657200000000,
		},
		{
			name: "November 12, 1980, 4:35:01am",
			time: &ottl.StandardTimeGetter[interface{}]{
				Getter: func(ctx context.Context, tCtx interface{}) (interface{}, error) {
					return time.Date(1980, 11, 12, 4, 35, 1, 0, time.Local), nil
				},
			},
			expected: 342869701000000,
		},
		{
			name: "October 4, 2020, 5:05 5 microseconds 5 nanosecs",
			time: &ottl.StandardTimeGetter[interface{}]{
				Getter: func(ctx context.Context, tCtx interface{}) (interface{}, error) {
					return time.Date(2020, 10, 4, 5, 5, 5, 5, time.Local), nil
				},
			},
			expected: 1601802305000000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exprFunc, err := UnixMicro(tt.time)
			assert.NoError(t, err)
			result, err := exprFunc(nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}