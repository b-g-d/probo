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

package dates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeadlineToDate_UsesLocation(t *testing.T) {
	t.Parallel()

	assert.Nil(t, DeadlineToDate(nil))

	utc := time.Date(2026, 9, 14, 15, 4, 5, 0, time.UTC)
	got := DeadlineToDate(&utc)
	require.NotNil(t, got)
	assert.Equal(t, "2026-09-14", *got)

	// Local midnight in UTC+2 is the previous calendar day in UTC.
	loc := time.FixedZone("UTC+2", 2*60*60)
	local := time.Date(2026, 9, 14, 0, 30, 0, 0, loc)
	got = DeadlineToDate(&local)
	require.NotNil(t, got)
	assert.Equal(t, "2026-09-14", *got)
}

func TestDateToDeadline(t *testing.T) {
	t.Parallel()

	empty, err := DateToDeadline("  ")
	require.NoError(t, err)
	assert.Nil(t, empty)

	parsed, err := DateToDeadline(" 2026-09-14 ")
	require.NoError(t, err)
	require.NotNil(t, parsed)
	assert.Equal(t, 2026, parsed.Year())
	assert.Equal(t, time.September, parsed.Month())
	assert.Equal(t, 14, parsed.Day())

	instant, err := DateToDeadline("2026-09-14T00:30:00+02:00")
	require.NoError(t, err)
	require.NotNil(t, instant)
	date := DeadlineToDate(instant)
	require.NotNil(t, date)
	assert.Equal(t, "2026-09-14", *date)

	broken, err := DateToDeadline("not-a-date")
	require.Error(t, err)
	assert.Nil(t, broken)
	assert.ErrorContains(t, err, "cannot parse deadline")
}
