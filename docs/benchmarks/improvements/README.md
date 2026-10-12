# Paired SlopDB improvement measurements

The twenty-two measured TPC-H queries totaled **60.686 → 39.590 seconds**, a 34.8% reduction (1.53×). The largest improvement was Q5, **29.953 → 6.700 seconds**. Eleven queries were slower in the after run; Q13 and Q20 regressed most. TPC-C throughput was **2,139.5 → 2,125.8 transactions/second** (-0.64%). These single paired runs do not establish a statistically significant TPC-C change.

These are historical measurements of the [integrated improvements](../../project-improvements.md), before the later configurable-page-size changes. Both revisions ran serially without concurrent heavy jobs on the same Linux/ARM64 host with four CPUs and Go 1.22.2. OS caches were not reset. These development workloads are not certified TPC scores.

## Revisions and settings

| Workload | Before revision | After revision |
| --- | --- | --- |
| TPC-H | `50b6a0b1cb0208650853dfb0c839e64ca4eeb4a4` | `91ea37023b6e02cebf45b80ad25ecfb3ce2c2fd5` |
| TPC-C | `50b6a0b1cb0208650853dfb0c839e64ca4eeb4a4` | `04a5ad91db6a959e979d32d675c57f7e633a0cd2` |

TPC-H used the same hash-verified DBGEN SF=0.2 inputs: 1,731,999 rows across eight tables, fourteen secondary indexes, ANALYZE and the Q15 view. Each prepared query had one warmup and one measured execution with a 180-second deadline. The reported median is therefore a single sample. Loading, reopening and preparation were outside query timing.

TPC-C used fresh seeded databases, ten workers for sixty seconds, two warehouses, ten districts per warehouse, one hundred customers per district, one thousand items and seed 42. Both used prepared statements, five maximum retries, a 250 ms lock timeout and a five-second transaction timeout. Loading, preparation, validation and close/reopen were outside throughput timing.

Both used WAL recording with WAL fsync disabled, 4 KiB SSTable blocks, a 4 MiB memtable, a 128 MiB decoded-block cache and default auto compression. The after implementation also introduced the default 64 MiB soft operator working budget and spill paths; this comparison measures the integrated implementation, not one isolated optimization. The [manifest](manifest.json) records complete configurations, hashes and revision provenance.

## All twenty-two TPC-H queries

Positive elapsed-time changes indicate a slower after sample.

| Query | Before ms | After ms | Elapsed change |
| --- | ---: | ---: | ---: |
| Q01: Pricing summary | 4,350.394 | 5,017.305 | +15.3% |
| Q02: Minimum cost supplier | 56.979 | 52.279 | -8.2% |
| Q03: Shipping priority | 2,182.020 | 2,134.963 | -2.2% |
| Q04: Order priority | 422.477 | 446.507 | +5.7% |
| Q05: Local supplier volume | 29,953.137 | 6,700.068 | -77.6% |
| Q06: Forecasting revenue | 2,023.559 | 1,935.971 | -4.3% |
| Q07: Volume shipping | 3,174.912 | 3,119.242 | -1.8% |
| Q08: National market share | 270.948 | 210.076 | -22.5% |
| Q09: Product type profit | 4,065.259 | 5,056.333 | +24.4% |
| Q10: Returned item reporting | 622.756 | 619.856 | -0.5% |
| Q11: Important stock identification | 94.660 | 104.765 | +10.7% |
| Q12: Shipping modes | 3,134.051 | 2,603.392 | -16.9% |
| Q13: Customer distribution | 1,622.683 | 3,095.858 | +90.8% |
| Q14: Promotion effect | 211.001 | 226.496 | +7.3% |
| Q15: Top supplier | 529.630 | 507.252 | -4.2% |
| Q16: Parts supplier relationship | 239.544 | 262.252 | +9.5% |
| Q17: Small quantity order revenue | 52.492 | 56.895 | +8.4% |
| Q18: Large volume customer | 3,322.164 | 2,723.056 | -18.0% |
| Q19: Discounted revenue | 118.808 | 91.078 | -23.3% |
| Q20: Potential part promotion | 205.254 | 331.562 | +61.5% |
| Q21: Suppliers who kept orders waiting | 3,823.404 | 4,073.148 | +6.5% |
| Q22: Global sales opportunity | 210.038 | 222.043 | +5.7% |

Cumulative allocations across measured TPC-H queries fell from 13.97 to 8.42 GiB (39.7%). These are allocation totals, not retained heap or peak memory. Whole-process high-water RSS was 2.80 → 2.54 GiB, including loading and warmups. Q9, Q13 and Q20 increased both elapsed time and allocations; aggregate improvements do not eliminate these regressions.

## TPC-C and correctness

| Measurement | Before | After |
| --- | ---: | ---: |
| Committed transactions | 128,373 | 127,548 |
| Total p95 latency, ms | 15.118 | 15.210 |
| Total p99 latency, ms | 24.368 | 24.480 |
| Expected rollbacks | 559 | 557 |
| Shutdown cancellations | 8 | 10 |
| Unexpected failures / retries | 0 / 0 | 0 / 0 |

All fifteen TPC-C consistency assertions passed immediately after each run and again after reopening. All twenty-two TPC-H queries completed without errors or timeouts; complete answers matched independently recomputed SQLite results and the archived DuckDB report. SQLite verification timings were separate correctness checks and are not part of this performance comparison.

The after TPC-C report adds CPU, allocation and storage counters that the baseline runner did not collect. Its resource window includes workload execution plus shutdown drain, excluding loading, preparation, validation and close. Do not infer before/after resource improvements from missing baseline counters.

Raw reports: [TPC-H before](tpch-before.json), [TPC-H after](tpch-complete.json), [TPC-C before](tpcc-before.json), [TPC-C after](tpcc-after.json), [answer verification before](verification-before.json), and [answer verification after](verification-after.json). Reports preserve full query answers, samples, plans and the original external artifact paths. No unsuccessful intermediate TPC-H attempts are substituted for the complete after run.
