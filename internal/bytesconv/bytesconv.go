// Package bytesconv provides zero-allocation string↔[]byte conversions.
//
// The returned values share backing memory with the input:
//   - StringToBytes: treat the slice as read-only; never mutate it.
//   - BytesToString: the []byte must outlive the string and must not be mutated afterward.
package bytesconv

import "unsafe"

// IsEmpty reports whether s has length 0.
func IsEmpty(s string) bool {
	return s == ""
}

// StringToBytes returns a read-only view of s as a []byte without copying.
func StringToBytes(s string) []byte {
	if IsEmpty(s) {
		return nil
	}

	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// BytesToString returns a string view of b without copying.
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}

	return unsafe.String(unsafe.SliceData(b), len(b))
}
