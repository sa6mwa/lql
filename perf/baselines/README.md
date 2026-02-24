# LQL Baselines

These files capture reproducible benchmark baselines for the token-stream refactor.

## Benchmark command

```bash
go test ./cmd/lql -run '^$' -bench '^BenchmarkLQLSelectionBaseline$' -benchmem -benchtime=1x -count=3
```

Mutate stream benchmark command:

```bash
LQL_BENCH_MUTATE_NDJSON_COUNT=5000 \
LQL_BENCH_MUTATE_ARRAY_COUNT=5000 \
LQL_BENCH_MUTATE_SINGLE_COUNT=4000 \
go test . -run '^$' -bench '^BenchmarkMutateStreamSynthetic$' -benchmem -benchtime=1x
```

Lockd fixture benchmark command:

```bash
go test . -run '^$' -bench '^BenchmarkLockdFixtures$' -benchmem -benchtime=1x
```

Save lockd fixture baseline:

```bash
make benchmark-lockd-save
```

Compare two lockd baselines:

```bash
make benchmark-lockd-compare OLD=perf/baselines/old.txt NEW=perf/baselines/new.txt
```

## Synthetic dataset sizing

Defaults are defined in `cmd/lql/benchmark_selection_test.go`:

- `LQL_BENCH_NDJSON_COUNT` default `30000`
- `LQL_BENCH_ARRAY_COUNT` default `30000`
- `LQL_BENCH_SINGLE_COUNT` default `20000`

Override via environment variables when needed.

## Captured runs

- `perf/baselines/2026-02-21-pre-token-stream-baseline.txt`
- `perf/baselines/2026-02-21-post-token-stream-baseline.txt`
- `perf/baselines/2026-02-21-stream-sdk-baseline.txt`
- `perf/baselines/2026-02-21-stream-sdk-baseline-legacy-sizes.txt`
- `perf/baselines/2026-02-21-stream-sdk-custom-scanner-legacy-sizes.txt`
- `perf/baselines/2026-02-21-mutate-stream-rewrite.txt`
