package nats

// pageSlice returns items[offset:offset+limit] and the total length.
// When limit < 0, all items from offset to the end are returned (same as ListStreamsPage).
// When offset is past the end, it returns an empty slice and the total.
func pageSlice[T any](items []T, offset, limit int) ([]T, int) {
	total := len(items)
	if offset < 0 {
		offset = 0
	}

	if offset >= total {
		return items[:0:0], total
	}

	if limit < 0 {
		return items[offset:], total
	}

	end := offset + limit
	if end > total {
		end = total
	}

	return items[offset:end], total
}

// pageInfos pages names, fetches each info, and skips not-found races.
func pageInfos[T any](
	names []string,
	offset, limit int,
	fetch func(name string) (T, error),
	isNotFound func(error) bool,
) ([]T, int, error) {
	total := len(names)
	pageNames, _ := pageSlice(names, offset, limit)
	infos := make([]T, 0, len(pageNames))
	for _, name := range pageNames {
		info, err := fetch(name)
		if err != nil {
			if isNotFound(err) {
				continue
			}

			return nil, total, err
		}
		infos = append(infos, info)
	}

	return infos, total, nil
}
