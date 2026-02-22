package storageengine

import (
	"DaemonDB/types"
	"fmt"
	"sort"
	"strings"
)

/*
This file contains JOIN implementation using merge sort algo
*/

func (se *StorageEngine) sortRowsByColumn(rows []map[string]interface{}, colName string) {
	sort.Slice(rows, func(i, j int) bool {
		return types.CompareValues(rows[i][colName], rows[j][colName]) < 0
	})
}

func (se *StorageEngine) mergeSortInnerJoin(left, right []map[string]interface{}, leftCol, rightCol string) []map[string]interface{} {
	result := []map[string]interface{}{}
	i, j := 0, 0
	lenL, lenR := len(left), len(right)
	for i < lenL && j < lenR {
		leftVal := left[i][leftCol]
		rightVal := right[j][rightCol]

		if leftVal == nil || rightVal == nil {
			if leftVal == nil {
				i++
			}
			if rightVal == nil {
				j++
			}
			continue
		}

		cmp := types.CompareValues(leftVal, rightVal)

		if cmp < 0 {
			i++
		} else if cmp > 0 {
			j++
		} else {
			target := left[i][leftCol]
			leftStart := i
			for i < len(left) && types.CompareValues(left[i][leftCol], target) == 0 {
				i++
			}

			rightStart := j
			for j < len(right) && types.CompareValues(right[j][rightCol], target) == 0 {
				j++
			}

			for li := leftStart; li < i; li++ {
				for ri := rightStart; ri < j; ri++ {
					merged := make(map[string]interface{})
					for k, v := range left[li] {
						merged[k] = v
					}
					for k, v := range right[ri] {
						merged[k] = v
					}
					result = append(result, merged)
				}
			}
		}
	}
	return result
}

func (se *StorageEngine) mergeSortOuterJoin(left, right []map[string]interface{}, leftCol, rightCol string) []map[string]interface{} {
	result := []map[string]interface{}{}
	i, j := 0, 0

	for i < len(left) {
		valL := left[i][leftCol]

		if valL == nil || j >= len(right) {
			result = append(result, se.copyRowWithNulls(left[i]))
			i++
			continue
		}

		valR := right[j][rightCol]
		if valR == nil {
			j++
			continue
		}

		cmp := types.CompareValues(valL, valR)

		if cmp < 0 {
			result = append(result, se.copyRowWithNulls(left[i]))
			i++
		} else if cmp > 0 {
			j++
		} else {
			matchVal := valL
			leftStart, rightStart := i, j

			for i < len(left) && types.CompareValues(left[i][leftCol], matchVal) == 0 {
				i++
			}
			for j < len(right) && types.CompareValues(right[j][rightCol], matchVal) == 0 {
				j++
			}

			for li := leftStart; li < i; li++ {
				for ri := rightStart; ri < j; ri++ {
					merged := make(map[string]interface{})
					for k, v := range left[li] {
						merged[k] = v
					}
					for k, v := range right[ri] {
						merged[k] = v
					}
					result = append(result, merged)
				}
			}
		}
	}
	return result
}

func (se *StorageEngine) mergeSortFullJoin(left, right []map[string]interface{}, leftCol, rightCol string) []map[string]interface{} {
	result := []map[string]interface{}{}
	i, j := 0, 0

	for i < len(left) || j < len(right) {
		if j >= len(right) {
			result = append(result, se.copyRowWithNulls(left[i]))
			i++
			continue
		}

		if i >= len(left) {
			result = append(result, se.copyRowWithNulls(right[j]))
			j++
			continue
		}

		valL := left[i][leftCol]
		valR := right[j][rightCol]

		if valL == nil {
			result = append(result, se.copyRowWithNulls(left[i]))
			i++
			continue
		}
		if valR == nil {
			result = append(result, se.copyRowWithNulls(right[j]))
			j++
			continue
		}

		cmp := types.CompareValues(valL, valR)

		if cmp < 0 {
			result = append(result, se.copyRowWithNulls(left[i]))
			i++
		} else if cmp > 0 {
			result = append(result, se.copyRowWithNulls(right[j]))
			j++
		} else {
			matchVal := valL
			leftStart, rightStart := i, j

			for i < len(left) && types.CompareValues(left[i][leftCol], matchVal) == 0 {
				i++
			}
			for j < len(right) && types.CompareValues(right[j][rightCol], matchVal) == 0 {
				j++
			}

			for li := leftStart; li < i; li++ {
				for ri := rightStart; ri < j; ri++ {
					merged := make(map[string]interface{})
					for k, v := range left[li] {
						merged[k] = v
					}
					for k, v := range right[ri] {
						merged[k] = v
					}
					result = append(result, merged)
				}
			}
		}
	}
	return result
}

func (se *StorageEngine) filterJoinedRows(rows []map[string]interface{}, whereCol, whereVal string) []map[string]interface{} {
	filtered := []map[string]interface{}{}

	for _, row := range rows {
		val := row[whereCol]

		if strings.ToUpper(whereVal) == "NULL" || whereVal == "" {
			if val == nil {
				filtered = append(filtered, row)
			}
			continue
		}

		if val != nil && fmt.Sprintf("%v", val) == whereVal {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func (se *StorageEngine) copyRowWithNulls(rows map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})
	for k, v := range rows {
		merged[k] = v
	}
	return merged
}
