package tpcc

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
)

// CPU, mutex and block profiles cover workload and drain only. The allocation
// profile is cumulative through drain and also includes process setup.
func startWorkloadProfiles(dir string) (func() error, error) {
	if dir == "" {
		return func() error { return nil }, nil
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	f, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		return nil, err
	}
	if err = pprof.StartCPUProfile(f); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	previous := runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	var once sync.Once
	var result error
	return func() error {
		once.Do(func() {
			pprof.StopCPUProfile()
			result = f.Close()
			runtime.SetMutexProfileFraction(previous)
			runtime.SetBlockProfileRate(0)
			for _, name := range []string{"mutex", "block", "allocs"} {
				out, e := os.Create(filepath.Join(dir, name+".pprof"))
				if e != nil {
					result = errors.Join(result, e)
					continue
				}
				result = errors.Join(result, pprof.Lookup(name).WriteTo(out, 0), out.Close())
			}
		})
		return result
	}, nil
}
