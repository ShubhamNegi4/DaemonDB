package bplus

// binary search for treesearch.go
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

// // binary search for treeinsertion.go
// func binarySearchInsert(keys [][]byte, target []byte, cmp func(a, b []byte) int) int {
// 	lo, hi := 0, len(keys)
// 	for lo < hi {
// 		mid := lo + (hi-lo)/2
// 		if cmp(keys[mid], target) < 0 {
// 			lo = mid + 1
// 		} else {
// 			hi = mid
// 		}
// 	}
// 	return lo
// }

// binary search for findleaf.go
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

// insert inserts elem at index i in slice.
func insert[T any](slice []T, i int, elem T) []T {
	slice = append(slice, elem) // grow by 1
	copy(slice[i+1:], slice[i:])
	slice[i] = elem
	return slice
}

// remove removes element at index i from slice.
func remove[T any](slice []T, i int) []T {
	return append(slice[:i], slice[i+1:]...)
}
