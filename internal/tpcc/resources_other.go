//go:build !linux

package tpcc

func processResources() (float64, int64, bool) { return 0, 0, false }
