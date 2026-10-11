#!/usr/bin/env bash
set -euo pipefail
# This shim is loaded only in the benchmark process and matches its exact WAL.
comparison_root=$(cd -- "$(dirname -- "$0")" && pwd)
benchmark_dir=$(mktemp -d /tmp/slopdb-tpcc-duckdb-XXXXXX)
# The runner creates its database directory itself, so use a fresh child path.
database_dir="$benchmark_dir/database"
shim="$benchmark_dir/wal-no-fsync.so"
cc -shared -fPIC -O2 -Wall -Wextra -o "$shim" "$comparison_root/nofsync/wal_sync.c" -ldl
binary="$benchmark_dir/tpcc-compare"
(cd "$comparison_root" && go build -o "$binary" .)
# Prevent options that could change the exact WAL path or request real sync.
for option in "$@"; do
 case "$option" in
  -dir|--dir|-dir=*|--dir=*|-engine|--engine|-engine=*|--engine=*|-wal-fsync|--wal-fsync|-wal-fsync=*|--wal-fsync=*)
   printf '%s\n' 'run-duckdb.sh owns -dir, -engine and -wal-fsync; use the binary directly for other modes.' >&2
   exit 2;;
 esac
done
exec env LD_PRELOAD="$shim" SLOPDB_TPCC_WAL_FILE="$database_dir/benchmark.duckdb.wal" "$binary" -engine duckdb -dir "$database_dir" "$@"
