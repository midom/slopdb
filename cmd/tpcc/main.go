package main

import (
	"fmt"
	"os"
	"slopdb/internal/tpcc"
)

func main() {
	if err := tpcc.Run(tpcc.OpenSlopDB); err != nil {
		fmt.Fprintln(os.Stderr, "tpcc:", err)
		os.Exit(1)
	}
}
