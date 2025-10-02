package bplus

func binarySearch(keys [][]byte, target []byte, cmp func(a, b []byte) int) int {
	low := 0
	high := len(keys) - 1
	for low <= high {
		mid := low + (high-low)/2
		if cmp(keys[mid], target) == 0 {
			return mid
		} else if cmp(keys[mid], target) < 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return -1
}

func binarySearchInsert(keys [][]byte, target []byte, cmp func(a, b []byte) int) int {
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if cmp(keys[mid], target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func lowerBound(keys [][]byte, target []byte, cmp func(a, b []byte) int) int {
	lo, hi := 0, len(keys)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if cmp(keys[mid], target) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
