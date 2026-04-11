# lql

LQL (Lockd Query Language) provides a compact selector and mutation syntax for
JSON documents. This module includes:

- A Go library for parsing selectors and mutations, plus selector evaluation.
- A CLI (`cmd/lql`) for selecting JSON documents, applying mutations, and
  formatting output via [pkt.systems/prettyx](https://pkg.go.dev/pkt.systems/prettyx).

## Install

```bash
go get pkt.systems/lql@latest
```

## Development Targets

The repository provides smoke (fast) and full (expensive) validation targets:

```bash
make test                 # fast test pass
make benchmark            # smoke benchmarks
make benchmark-lockd      # lockd fixture benchmarks
make benchmark-lockd-save # save lockd baseline under perf/baselines
make fuzz                 # smoke fuzzers
make test-all             # check + smoke fuzz + smoke bench

make test-all-full        # full coverage + full fuzz + full bench
make ci-lockd             # lockd CI profile (contracts + lockd benches)
```

For CPU-constrained systems, cap Go runtime parallelism for any target:

```bash
CPU_LIMIT=2 make test-all
```

## Library usage

### Selectors

Selectors are declarative predicates built over JSON Pointer fields.

```go
sel, err := lql.ParseSelectorString(`
  and.eq{field=/status,value=open},
  and.range{field=/progress,gte=50}
`)
if err != nil {
  log.Fatal(err)
}

doc := map[string]any{
  "status":   "open",
  "progress": 72,
}

if lql.Matches(sel, doc) {
  fmt.Println("match")
}
```

If you already have selectors split into a slice, you can combine them with AND
or OR without rejoining them yourself:

```go
sel, err := lql.ParseSelectorStrings([]string{`/status="ok"`, `/msg="done"`})
sel, err := lql.ParseSelectorStringsOr([]string{`/status="ok"`, `/msg="done"`})
```

Shorthand forms are supported:

```go
sel, _ := lql.ParseSelectorString(`/status="open",/progress>=50`)
```

Datetime literals are also supported in shorthand range comparisons:

```go
sel, _ := lql.ParseSelectorString(`/timestamp>=2026-03-05T10:28:21Z`)
sel, _ = lql.ParseSelectorString(`/timestamp>=2026-03-11T01:11:28`)
sel, _ = lql.ParseSelectorString(`/timestamp<"2026-03-05T11:29:41.265+01:00"`)
```

`eq` shorthand is datetime-aware when both sides parse as temporal values:

```go
sel, _ := lql.ParseSelectorString(`/timestamp="2025-01-01"`)
// Matches values such as "2025-01-01T15:00:00Z" (date-only intersection).
```

Explicit date selectors are available for intent and macro support:

```go
sel, _ := lql.ParseSelectorString(`date{field=/timestamp,after=2025-01-01,before=2025-02-01}`)
sel, _ = lql.ParseSelectorString(`date{f=/timestamp,a=2025-01-01,b=2025-01-03}`)
sel, _ = lql.ParseSelectorString(`date{f=/timestamp,since=yesterday}`)
```

Programmatic range-term construction can use exported bound helpers:

```go
sel := lql.Selector{
  Range: &lql.RangeTerm{
    Field: "/timestamp",
    GTE:   lql.NewDatetimeRangeBound("2026-03-05T10:28:21Z"),
    LT:    lql.NewDatetimeRangeBound("2026-03-05T10:30:00Z"),
  },
}
```

Temporal behavior summary:

- Supported temporal literals: `YYYY-MM-DD`, RFC3339, RFC3339Nano, and naive
  UTC datetimes like `2026-03-11T01:11:28` or `2026-03-11T01:11:28.123456789`.
- Timezones are normalized to the same instant for datetime comparison.
- `range{...}` supports numeric or datetime bounds (`gt/gte/lt/lte`) but cannot mix numeric and datetime bounds in one clause.
- Relative macros are only supported by `date{...,since=...}` (`now`, `today`, `yesterday`).
- Shorthand/range comparators require explicit numeric or datetime literals (no macros).

Temporal selector performance note (measured on March 5, 2026, synthetic benchmark, `go test` on Intel i7-1355U):

```bash
go test . -run '^$' \
  -bench '^BenchmarkQueryStreamSynthetic/(large_ndjson$|large_ndjson_datetime_range$|large_ndjson_date_selector$)/decision_only_selector/steady_state$' \
  -benchmem -benchtime=500ms -count=5
```

- `large_ndjson/decision_only_selector/steady_state`: ~`10.25 ms/op`, `0 B/op`, `0 allocs/op`
- `large_ndjson_datetime_range/decision_only_selector/steady_state`: ~`16.33 ms/op`, `0 B/op`, `0 allocs/op` (~`1.59x`)
- `large_ndjson_date_selector/decision_only_selector/steady_state`: ~`16.29 ms/op`, `0 B/op`, `0 allocs/op` (~`1.59x`)

Interpretation: temporal selectors keep zero steady-state allocations, but runtime is higher than plain string equality because candidate values are parsed as datetimes during matching.

Fuzz-style alloc regression guard:

- `TestQueryStreamSelectorAllocBudgetFuzzReplayTemporal` replays deterministic
  parity-fuzz payloads and enforces per-candidate allocation ceilings for
  temporal shorthand/range/date selectors.
- This runs in normal `go test` (and therefore `make test`, `make test-all`,
  `make test-all-full`) to catch alloc regressions without fuzz nondeterminism.

If you want implicit OR semantics across a single string, use
`ParseSelectorStringOr`:

```go
sel, _ := lql.ParseSelectorStringOr(`/status="open",/status="queued"`)
```

Array element selection is supported via JSON Pointer indices:

```go
sel, _ := lql.ParseSelectorString(`/devices/0/status="online"`)
```

Wildcard selection follows explicit semantics:

- `*` matches any child value of an object (objects only; arrays do not match)
- `[]` matches any element of an array (arrays only; objects do not match)
- `**` matches any child (object value or array element)
- `...` matches any descendant at any depth (objects or arrays)
- Type mismatches do not match (e.g. `[]` on an object)
- Bracket sugar: `/items[]/sku` is the same as `/items/[]/sku`

```go
sel, _ := lql.ParseSelectorString(`/labels/*="production"`)
sel, _ = lql.ParseSelectorString(`/items[]/sku="ABC-123"`)
sel, _ = lql.ParseSelectorString(`/items/**/sku="ABC-123"`)
sel, _ = lql.ParseSelectorString(`/items/.../sku="ABC-123"`)
sel, _ = lql.ParseSelectorString(`icontains{field=/message,value=timeout}`)
sel, _ = lql.ParseSelectorString(`contains{field=/message,any=timeout|degraded}`)
sel, _ = lql.ParseSelectorString(`prefix{field=/service,value=auth,ignoreCase=t}`)
```

### Mutations

Mutations modify JSON objects in-place.

```go
doc := map[string]any{
  "state": map[string]any{
    "status":  "queued",
    "retries": 1,
  },
}

if err := lql.Mutate(doc,
  "/state/status=running",
  "/state/retries=+2",
  "rm:/state/legacy",
); err != nil {
  log.Fatal(err)
}
```

Brace shorthand applies multiple mutations under a prefix:

```go
_ = lql.Mutate(doc, `/state{/owner="alice",/note="hi"}`)
```

Time-prefixed mutations normalize timestamps to RFC3339Nano:

```go
_ = lql.MutateWithTime(doc, time.Now(), `time:/state/updated=NOW`)
```

Mutations support the same wildcard semantics as selectors; missing paths under
wildcards are skipped.

File-backed mutation values are available for streaming mutation paths via
`ParseMutationsWithOptions` and the `file:`, `textfile:`, and `base64file:`
prefixes. They are disabled by default, produce JSON string values, and are not
supported by `ApplyMutations`.

Create a new JSON document from `{}` while streaming file content into a field:

```bash
printf '{}\n' | lql -F \
  -m '/filename=notes.txt' \
  -m '/tags/kind=document' \
  -m '/tags/source=local' \
  -m 'textfile:/content=notes.txt'
```

Auto mode chooses escaped text for UTF-8 files without NUL bytes and base64 for
binary-looking input:

```bash
printf '{}\n' | lql -F \
  -m '/filename=photo.jpg' \
  -m '/tags/media=image' \
  -m 'file:/content=photo.jpg'
```

Library callers can do the same thing with `MutateStream`:

```go
muts, _ := lql.ParseMutationsWithOptions([]string{
  `/filename=notes.txt`,
  `/tags/kind=document`,
  `/tags/source=local`,
  `textfile:/content=notes.txt`,
}, time.Now(), lql.ParseMutationsOptions{
  EnableFileValues: true,
  FileValueBaseDir: ".",
})

_ = lql.MutateStream(lql.MutateStreamRequest{
  Reader: strings.NewReader(`{}`),
  Writer: os.Stdout,
  Mutations: muts,
})
```

Stream mutations over large inputs without loading the whole stream:

```go
muts, _ := lql.ParseMutations([]string{`/state/status=running`}, time.Now())
_ = lql.MutateStream(lql.MutateStreamRequest{
  Ctx: context.Background(),
  Reader: strings.NewReader(`{"state":{"status":"queued"}}`),
  Writer: io.Discard, // optional compact NDJSON sink
  Mutations: muts,
  OnValue: func(v lql.MutateStreamValue) error {
    fmt.Printf("%s\n", v.JSON)
    return nil
  },
})
```

`QueryStream` supports `QueryDecisionOnly` and `QueryDecisionPlusValue` modes,
and both stream APIs return typed `*StreamError` values for contract failures
with machine-usable codes (`invalid_selector`, `invalid_body`,
`document_too_large`, `context_canceled`, `internal`).
Helper predicates are available:
`IsStreamInvalidSelector`, `IsStreamInvalidBody`,
`IsStreamDocumentTooLarge`, `IsStreamContextCanceled`, `IsStreamInternal`.

Selector capability routing helpers are available via
`InspectSelectorCapabilities` and `InspectSelectorExecutionTraits`.

For deterministic stream accounting and early-stop contracts, use
`QueryStreamWithResult` and `MutateStreamWithResult`.
`QueryStreamRequest` supports additive stop limits:
`MaxMatches`, `MaxCandidates`, `MaxBytesRead`.
`QueryStreamWithResult` reports:
`CandidatesSeen`, `CandidatesMatched`, `BytesRead`, `BytesCaptured`,
`SpillCount`, `SpillBytes`, `StoppedEarly`, `StopReason`, `LastOffset`.
`MutateStreamWithResult` reports:
`CandidatesSeen`, `CandidatesWritten`, `BytesRead`, `BytesWritten`,
`BytesCaptured`, `SpillCount`, `SpillBytes`,
`StoppedEarly`, `StopReason`, `LastOffset`.

`QueryStreamRequest.OnDecision` runs once per candidate before `OnValue` and
still fires when `MatchedOnly=true` and value callbacks are skipped.
Return `ErrStreamStop` (or wrapping it) from stream callbacks for graceful
callback-driven stop (`StoppedEarly=true`, stop reason `callback_stop`,
`nil` error return).

In `QueryDecisionPlusValue`/`IncludeJSON` mode, candidate payloads spool in
memory up to 3 MiB by default, then spill to temp files (`/tmp` by default).
Configure with `SpoolMemoryBytes`, `SpoolTempDir`, and `SpoolFilePattern`.
Set `MatchedOnly` to invoke callbacks only for matched candidates.
Tune capture behavior with `CapturePolicy`:
`QueryCaptureAllCandidates` (default) or
`QueryCaptureMatchesOnlyBestEffort` for lower spool pressure on low-hit scans.

For caller-managed payload storage, set `DisableInternalSpool=true` and provide
`PayloadSinkFactory` with a custom `QueryStreamPayloadSink`.
For low-churn spill reuse across many candidates, use
`NewReusableQueryPayloadSinkFactory(...)` and pass `factory.Factory()` as the
payload sink factory, then call `factory.Close()` when done.

`MutateStream` callback payload capture also supports caller-managed sinks via
`DisableInternalSpool` and `PayloadSinkFactory`. For reusable spill behavior,
use `NewReusableMutatePayloadSinkFactory(...)` and pass `factory.Factory()`,
then call `factory.Close()` when done.

`MutateStream` supports strict framing/root modes:
`MutateSingleValueOnly`, `MutateObjectRootOnly`, and
`MutateSingleObjectOnly`.

Candidate size accounting contract:
- `QueryStream MaxCandidateBytes` and `QueryStreamValue.Size` count bytes from
  the first non-whitespace byte of each candidate to its closing JSON token.
- Top-level separators and surrounding whitespace are excluded.

## CLI usage

```
usage: lql [-m mutator...] [-f field...] selector... [data.json]
   or: lql selector... < data.json
   or: cat data.json | lql selector...
```

By default, multiple selector arguments are combined with AND. Use `-O` (or
`--or`) to combine them with OR.

Selectors determine which JSON documents to output. The matching documents are
printed in full by default. If the input is a JSON array, each element is
treated as a candidate document.

Mutations apply to each JSON object in the input stream (NDJSON or JSON arrays).
When selectors are provided alongside `-m`, mutations are applied only to
matching objects. With `-m`, selectors no longer filter output; they only control
which objects are mutated (output still includes all objects, subject to `-f`).
Use `-M`/`--matches-only` to keep selectors acting as output filters even when
`-m` is provided. Output always contains the full (possibly mutated) object
unless `-f` is used.

Local file-backed mutation values are disabled by default in the CLI. Enable
them with `-F` or `--enable-file-mutations` to use `file:/field=path`,
`textfile:/field=path`, or `base64file:/field=path`. Leading `~/` expands to
the current user's home directory.

### Selection examples

Select documents matching a status and region:

```bash
lql '/status="open",/region="us-west"' data.json
```

### Full LQL examples

CLI (full LQL expressions):

```bash
lql 'and.eq{field=/status,value=open},and.range{field=/progress,gte=50}' data.json
```

```bash
lql 'or.eq{field=/region,value=us},or.eq{field=/region,value=eu}' data.json
```

```bash
lql 'not.eq{field=/state,value=disabled}' data.json
```

```bash
lql 'exists{/metadata/etag}' data.json
```

```bash
lql 'in{field=/env,any=prod|stage|dev}' data.json
```

```bash
lql 'and.eq{field=/items[]/sku,value=ABC-123},and.range{field=/items[]/price,lt=20}' data.json
```

```bash
lql 'contains{field=/msg,value=timeout,ic=t}' data.json
```

```bash
lql 'contains{field=/msg,any=timeout|degraded},icontains{field=/service,a=AUTH|EDGE}' data.json
```

```bash
lql 'icontains{field=/msg,value=timeout},iprefix{field=/service,value=auth}' data.json
```

```bash
lql '/timestamp>=2026-03-05T10:28:21Z' data.json
```

```bash
lql 'date{field=/timestamp,after=2025-01-01,before=2025-02-01}' data.json
```

```bash
lql 'date{f=/timestamp,a=2025-01-01,b=2025-01-03}' data.json
```

```bash
lql 'date{f=/timestamp,since=yesterday}' data.json
```

Note: full LQL expressions use `{}` and should be quoted (or the braces
escaped) to avoid shell brace expansion.

Note: inside `{}`, you can use `f=`/`v=` as aliases for `field=`/`value=`, `a=`
as an alias for `any=` (`in`, `contains`, and `icontains`), and `ic=` as an
alias for `ignoreCase=` in string terms (`contains`/`prefix`).
For `date`, `a=` is an alias for `after=` and `b=` is an alias for `before=`.
`ignoreCase` accepts `true/false` or shorthand `t/f`.

Note: omitted string values stay field-scoped. For example
`contains{field=/msg}`, `icontains{field=/msg}`, `prefix{field=/name}`, and
`iprefix{field=/name}` act as path assertions and require those paths to exist
(regardless of the terminal value type).
Only root/wildcard-any forms (such as `field=/`, `field=/*`, `field=/...`)
collapse to match-all for empty string terms.

SDK (parse full LQL expressions):

```go
sel, err := lql.ParseSelectorString(
  "and.eq{field=/status,value=open},and.range{field=/progress,gte=50}",
)
```

```go
sel, err := lql.ParseSelectorString(
  "or.eq{field=/region,value=us},or.eq{field=/region,value=eu}",
)
```

```go
sel, err := lql.ParseSelectorString("not.eq{field=/state,value=disabled}")
```

```go
sel, err := lql.ParseSelectorString("exists{/metadata/etag}")
```

```go
sel, err := lql.ParseSelectorString("in{field=/env,any=prod|stage|dev}")
```

```go
sel, err := lql.ParseSelectorString(
  "and.eq{field=/items[]/sku,value=ABC-123},and.range{field=/items[]/price,lt=20}",
)
```

```go
sel, err := lql.ParseSelectorString(
  "icontains{field=/msg,value=timeout},iprefix{field=/service,value=auth}",
)
```

```go
sel, err := lql.ParseSelectorString(
  "contains{field=/msg,any=timeout|degraded},icontains{field=/service,a=AUTH|EDGE}",
)
```

```go
sel, err := lql.ParseSelectorString(
  "date{field=/timestamp,after=2025-01-01,before=2025-02-01}",
)
```

```go
sel, err := lql.ParseSelectorString("date{f=/timestamp,since=yesterday}")
```

Select only a few fields from matching documents:

```bash
lql '/status="open"' -f /id -f /status -f /region data.json
```

Filter array input (each element is evaluated):

```bash
cat devices.json | lql '/telemetry/battery_mv<3600'
```

Match on array elements inside each document:

```bash
lql '/devices/0/status="online"' data.json
```

Match on any array element using wildcards:

```bash
lql '/items[]/sku="ABC-123"' data.json
```

Match on any descendant using recursive descent:

```bash
lql '/items/.../sku="ABC-123"' data.json
```

### Mutation examples

Apply mutations conditionally:

```bash
lql -m '/state/retries++' -m '/state/status=running' '/state/status="queued"' state.json
```

Apply mutations and emit only selected fields:

```bash
lql -m '/state/retries=+3' -f /state/retries -f /state/status state.json
```

Apply mutations across array elements:

```bash
lql -m '/items[]/status=ready' data.json
```

Apply mutations using recursive descent:

```bash
lql -m '/groups/.../status=ready' data.json
```

Note: mutations apply to a JSON object root, but paths may traverse arrays using
wildcards or numeric indices.

Write mutations inline:

```bash
lql -m '/state/status=done' -i state.json
```

### Output formatting

By default, output is pretty-printed using prettyx. Use `-c` for compact
one-line JSON documents and `-t` to select a prettyx theme (see `lql -h`).

## Selector grammar overview

- Logical: `and`, `or`, `not`
- Clauses: `eq`, `contains`, `icontains`, `prefix`, `iprefix`, `range`, `date`, `in`, `exists`
- `contains`/`icontains` support `value=` (single term) or `any=`/`a=` (pipe-delimited terms)
- JSON Pointer fields: `/path/to/field`
- Shorthand: `/field=value`, `/field!=value`, `/field>=10`, `/field<5`
- Datetime shorthand: `/timestamp>=2026-03-05T10:28:21Z`, `/timestamp>=2026-03-11T01:11:28`, `/timestamp<"2026-03-05T11:29:41.265+01:00"`
- `range` bounds: `gt`, `gte`, `lt`, `lte` with numeric or datetime literals (single-mode per clause)
- `date` keys: `value`, `after`/`a`, `before`/`b`, `gt`, `gte`, `lt`, `lte`, `since`
- `date.since` macros: `now`, `today`, `yesterday`
- Arrays: `/items/0/sku="ABC-123"`
- Wildcards: `*` (object values), `[]` (array elements), `**` (any child), `...` (recursive descent)

## Mutation grammar overview

- Set: `/path=value`
- Increment: `/path++`, `/path--`, `/path=+3`, `/path=-2`
- Remove: `rm:/path`, `delete:/path`
- Time: `time:/path=NOW` or RFC3339Nano timestamp (`RFC3339` also accepted)
- Brace: `/path{/a=1,/b=2}`
- Wildcards: `*`, `[]`, `**`, `...` in path segments
