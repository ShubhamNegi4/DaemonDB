package executor

import (
	"fmt"
	"strings"
)

func (vm *VM) PrintLine(cells []string) {
	for i, cell := range cells {
		fmt.Printf("%-20s", cell)
		if i < len(cells)-1 {
			fmt.Print("| ")
		}
	}
	fmt.Println()
}

func (vm *VM) PrintSeparator(count int) {
	if count > 0 {
		fmt.Println(strings.Repeat("-", (22*count)-2))
	}
}

func (vm *VM) formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	s, err := toString(val)
	if err != nil {
		return fmt.Sprintf("%v", val)
	}
	return s
}
