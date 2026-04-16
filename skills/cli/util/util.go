package util

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// SparkTimeLayout is the timestamp format used by the Spark History Server REST API.
// The "GMT" suffix is a hardcoded literal and the calendar is always UTC.
// See: core/src/main/scala/org/apache/spark/status/api/v1/JacksonMessageWriter.scala
const SparkTimeLayout = "2006-01-02T15:04:05.000GMT"

func ParseSparkTime(s string) (time.Time, error) {
	return time.Parse(SparkTimeLayout, s)
}

// FormatBytes formats a byte count as a human-readable string.
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func Deref[T any](p *T) T {
	if p != nil {
		return *p
	}
	var zero T
	return zero
}

func Ptr[T any](v T) *T { return &v }

// FormatIntSlice formats an optional int slice as a comma-separated string.
func FormatIntSlice(p *[]int) string {
	if p == nil || len(Deref(p)) == 0 {
		return ""
	}
	s := make([]string, len(*p))
	for i, v := range Deref(p) {
		s[i] = strconv.Itoa(v)
	}
	return strings.Join(s, ",")
}

// DerefBytes dereferences a pointer and formats the value as human-readable bytes.
func DerefBytes(p *int64) string { return FormatBytes(Deref(p)) }

// ApplyLimit truncates a slice to limit items, returning the original total count.
func ApplyLimit[T any](items []T, limit int) ([]T, int) {
	total := len(items)
	if limit > 0 && total > limit {
		return items[:limit], total
	}
	return items, total
}

// PrintLimitFooter prints a truncation notice if the result set was limited.
func PrintLimitFooter(w io.Writer, limit, total int, noun string) {
	if limit > 0 && total > limit {
		_, _ = fmt.Fprintf(w, "\nShowing %d of %d %s. Use --limit 0 to list all.\n", limit, total, noun)
	}
}

// FormatMs dereferences a pointer to milliseconds and formats as a duration string.
func FormatMs(p *int64) string { return FormatMsVal(Deref(p)) }

// FormatMsVal formats a millisecond count as a duration string.
func FormatMsVal(ms int64) string {
	return (time.Duration(ms) * time.Millisecond).Truncate(time.Millisecond).String()
}

// CheckResponse returns body if non-nil, or an error citing the HTTP status.
func CheckResponse[T any](body *T, status string) (*T, error) {
	if body == nil {
		return nil, fmt.Errorf("unexpected status: %s", status)
	}
	return body, nil
}

// SparkDuration computes the duration between two optional Spark timestamps.
func SparkDuration(start, end *string) time.Duration {
	if start == nil || end == nil {
		return 0
	}
	s, err1 := ParseSparkTime(*start)
	e, err2 := ParseSparkTime(*end)
	if err1 != nil || err2 != nil {
		return 0
	}
	return e.Sub(s)
}
