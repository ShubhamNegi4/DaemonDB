package main


import (
	"bytes"
	"fmt"
)


func main(){
	pager:= NewInMemoryPager()
	cache:= NewBufferPool(10)
	tree := NewBPlusTree(pager, cache, bytes.Compare)


	tree.Insertion([]byte("1"), []byte("100"))
	tree.Insertion([]byte("2"), []byte("200"))

	result, err := tree.Search([]byte("2"))
	if err != nil {
    	fmt.Printf("Error: %v\n", err)
    	return
	}
	if result == nil {
    	fmt.Printf("Key not found\n")
	} else {
    	fmt.Printf("Found: %s\n", string(result))
	}
}