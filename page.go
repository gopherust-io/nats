package nats

// pageSlice returns items[offset:offset+limit] and the total length.
// When offset is past the end, it returns an empty slice and the total.
func pageSlice[T any](items []T, offset, limit int) ([]T, int) {
	total := len(items)
	if offset < 0 {
		offset = 0
	}

	if limit < 0 {
		limit = 0
	}

	if offset >= total {
		return items[:0:0], total
	}

	end := offset + limit
	if end > total {
		end = total
	}

	return items[offset:end], total
}
