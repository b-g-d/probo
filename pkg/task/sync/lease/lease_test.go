// Copyright (c) 2026 Probo Inc <hello@probo.com>.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package lease

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.probo.inc/probo/pkg/coredata"
)

func TestOutboundLeaseHeartbeatInterval(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 30*time.Second, OutboundLeaseHeartbeatInterval(5*time.Minute, 30*time.Second))
	assert.Equal(t, time.Minute, OutboundLeaseHeartbeatInterval(2*time.Minute, 2*time.Minute))
	assert.Equal(t, time.Millisecond, OutboundLeaseHeartbeatInterval(time.Millisecond, time.Second))
}

func TestProcessingLeaseLost(t *testing.T) {
	t.Parallel()

	t.Run(
		"direct lease error",
		func(t *testing.T) {
			t.Parallel()

			assert.True(t, ProcessingLeaseLost(t.Context(), coredata.ErrProcessingLeaseLost))
		},
	)

	t.Run(
		"cancelled cause",
		func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(coredata.ErrProcessingLeaseLost)

			assert.True(t, ProcessingLeaseLost(ctx, context.Canceled))
		},
	)

	t.Run(
		"unrelated error",
		func(t *testing.T) {
			t.Parallel()

			assert.False(t, ProcessingLeaseLost(t.Context(), assert.AnError))
		},
	)
}
