package main

import "fmt"

func validateSQLitePageSize(n int64) error {
	if n != 0 && (n < 512 || n > 65536 || n&(n-1) != 0) {
		return fmt.Errorf("unsupported SQLite page size %d: powers of two 512..65536 required", n)
	}
	return nil
}
func validateDuckPageSize(n int64) error {
	if n != 0 && (n < 16384 || n > 262144 || n&(n-1) != 0) {
		return fmt.Errorf("unsupported DuckDB page size %d: powers of two 16384..262144 required", n)
	}
	return nil
}
func duckPageSize(n int64) int64 {
	if n == 0 {
		return 262144
	}
	return n
}
