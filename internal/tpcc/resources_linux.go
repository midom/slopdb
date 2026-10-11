//go:build linux

package tpcc

import "syscall"

func processResources() (cpuSeconds float64, peakRSSKiB int64, available bool) {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0, 0, false
	}
	cpu := float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	return cpu, usage.Maxrss, true
}
