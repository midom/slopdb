package main

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
static long long skipped_wal_syncs() {
 unsigned long long (*fn)(void)=dlsym(RTLD_DEFAULT,"slopdb_tpcc_wal_sync_skipped");
 return fn ? (long long)fn() : -1;
}
*/
import "C"

func skippedWALSyncs() int64 { return int64(C.skipped_wal_syncs()) }
