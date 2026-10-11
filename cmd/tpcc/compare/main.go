package main

import (
	"fmt"
	"os"
	"slopdb/internal/tpcc"
)

func main() {
	if err := tpcc.Run(openEngine); err != nil {
		fmt.Fprintln(os.Stderr, "tpcc-compare:", err)
		os.Exit(1)
	}
}
