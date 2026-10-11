# Q6 physical access regression

Profiling the integrated SF=0.2 TPC-H Q6 run found a physical-access regression
despite the faster typed expression kernel. The database contains 1,199,969
lineitem rows with identical schema/input files across the before/after loads.

The old EXPLAIN text reported a table rewind, but its CPU/allocation profiles
showed the actual selective shipdate-index walk and batched point lookups.
The integrated distribution cost model charged up to twelve sequential-row
units per noncovering secondary lookup. That rejected Q6's approximately 15%
date range in favor of scanning and decompressing the entire wide table.
Over 90% of the resulting allocations came from SSTable reading, block
decompression and iterator keys rather than typed expression evaluation.

Selecting the index exposed another issue: a single variable-width index range
uses several normalized, disjoint delimiter bands to preserve shorter-prefix
TEXT/BLOB ordering. The physical reader treated multiple bands as overlapping
origins. Its unnecessary deduplication set exceeded the 16 MiB budget share and
spilled into a temporary LSM engine. The read-query profile consequently
contained memtable writes and flushes, adding approximately 217 MB of allocation
per warm execution.

The reader now deduplicates across separate OR access paths or actually
overlapping intervals, preserving the bounded disk fallback when needed.
A sorted disjoint interval union within one index visits each physical entry
once, and each table row has one entry in that index, so it needs no origin set.
The noncovering lookup weight now rises from three to six with estimated
working-set size. Useful approximately 15% ranges remain eligible; broad ranges
such as 30% still prefer sequential access. Costs remain estimates rather than
execution constraints, and residual SQL predicates remain intact.

Three prepared Q6 executions per case, with a GC before each sample, measured:

| Build/access | Warm sample times | Warm allocated bytes |
| --- | --- | --- |
| Baseline, selective index | 2.030–2.036 s | 336.1–336.3 MB |
| Integrated, full scan | 2.804–2.813 s | 492.3–492.6 MB |
| Index with unnecessary interval deduplication | 2.571–2.614 s | 523.9–524.1 MB |
| Corrected index selection and disjoint bands | 1.954–1.969 s | 306.6–306.7 MB |

The corrected measurements use the same physical integrated database as the
full-scan and unnecessary-deduplication cases. CPU/allocation profiles and
scratch binaries were kept outside the repository. Revenue matched the baseline
index execution; the full scan's different accumulation order differed only in
floating-point rounding. These diagnostic samples explain this regression;
controlled runs of all 22 queries should check the cost adjustment's wider
effects.

Permanent tests check selective-versus-broad distribution decisions, disjoint,
touching and overlapping interval shapes, correct shorter-prefix results,
zero spills for disjoint bands under a 512-byte operator budget, retained OR
deduplication spills, cleanup and fresh/reopened databases.
