package bufferpool

func (pq gdsfPQ) Len() int { return len(pq) }
func (pq gdsfPQ) Less(i, j int) bool {
	// Min-heap by key.
	return pq[i].key < pq[j].key
}
func (pq gdsfPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *gdsfPQ) Push(x any) {
	item := x.(*gdsfItem)
	item.index = len(*pq)
	*pq = append(*pq, item)
}
func (pq *gdsfPQ) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[:n-1]
	return item
}

