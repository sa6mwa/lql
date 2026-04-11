// Package lql implements the Lockd Query Language (LQL) selector and mutation
// syntax for JSON documents.
//
// # Selectors
//
// LQL selectors are declarative predicates over JSON documents. They compile
// to a Selector AST and can be evaluated with Matches.
//
// Basic forms:
//
//	eq{field=/status,value=open}
//	contains{field=/message,value=timeout}
//	contains{field=/message,any=timeout|degraded}
//	icontains{field=/message,value=timeout}
//	icontains{field=/service,a=AUTH|EDGE}
//	prefix{field=/owner,value=team-}
//	iprefix{field=/owner,value=team-}
//	range{field=/progress,gt=10}
//	range{field=/timestamp,gte="2026-03-05T10:28:21Z"}
//	date{field=/timestamp,after=2025-01-01,before=2025-02-01}
//	date{f=/timestamp,a=2025-01-01,b=2025-01-03}
//	date{f=/timestamp,since=yesterday}
//	in{field=/state,any=["queued","running"]}
//	exists{/metadata/etag}
//
// String terms (`contains`/`prefix`) support `ignoreCase=true|false`
// (or shorthand `ic=t|f`) for per-clause case handling.
// `contains`/`icontains` also support `any=`/`a=` for pipe-delimited multi-term
// matching.
//
// Omitted values for `contains`/`icontains`/`prefix`/`iprefix` remain
// field-scoped path assertions (for example `contains{field=/msg}` requires
// `/msg` to resolve, regardless of terminal value type). Only root/wildcard-any
// fields such as `/`, `/*`, and `/...` collapse to match-all for empty string
// terms.
//
// Logical composition:
//
//	and.eq{field=/status,value=open},and.range{field=/progress,gte=50}
//	or.eq{field=/region,value=us},or.eq{field=/region,value=eu}
//	not.eq{field=/state,value=disabled}
//
// Shorthand:
//
//	/status="open"
//	/status!=closed
//	/progress>=50
//	/timestamp>=2026-03-05T10:28:21Z
//	/timestamp>=2026-03-11T01:11:28
//	/timestamp<"2026-03-05T11:29:41.265+01:00"
//
// Temporal selector semantics:
//
//   - Supported temporal literals: YYYY-MM-DD, RFC3339, RFC3339Nano, and
//     naive UTC datetimes like YYYY-MM-DDTHH:MM:SS or
//     YYYY-MM-DDTHH:MM:SS.fffffffff.
//   - eq is datetime-aware when both query value and field value parse as
//     temporal values.
//   - Date-only equality intersects timestamps by calendar date
//     (for example "2025-01-01" matches "2025-01-01T15:00:00Z").
//   - range supports numeric or datetime bounds (gt/gte/lt/lte), but does not
//     allow mixed numeric + datetime bounds in one clause.
//   - Programmatic range construction can use NewNumericRangeBound and
//     NewDatetimeRangeBound.
//   - date.since supports relative macros now/today/yesterday.
//
// Arrays:
//
//	/devices/0/status="online"
//	/items/2/sku="ABC-123"
//
// Wildcards (selector paths):
//
//   - any child value of an object (objects only; arrays do not match)
//     []  any element of an array (arrays only; objects do not match)
//     **  any child (object value or array element)
//     ... recursive descent (any depth, objects or arrays)
//
// Type mismatches do not match (e.g. [] on an object). Bracket sugar expands
// "/items[]/sku" to "/items/[]/sku".
//
//	/labels/*="production"
//	/items[]/sku="ABC-123"
//	/items/**/sku="ABC-123"
//	/items/.../sku="ABC-123"
//
// Example:
//
//	sel, _ := lql.ParseSelectorString(`/status="open",/progress>=50`)
//	ok := lql.Matches(sel, map[string]any{
//	  "status": "open",
//	  "progress": 72,
//	})
//
// # Mutations
//
// Mutations mutate JSON objects in-place using JSON Pointer paths.
//
//	/state/status=running        # set
//	/state/retries++             # increment by 1
//	/state/retries=+3            # add 3
//	rm:/state/legacy             # delete
//	time:/state/updated=NOW      # RFC3339Nano timestamp (RFC3339 also accepted)
//
// Mutations support the same wildcard semantics as selectors. When a wildcard
// path segment is used, missing paths under matched nodes are skipped.
//
// Brace shorthand applies a set of nested mutations under a prefix:
//
//	/state{/owner="alice",/note="hi"}
//
// File-backed mutation values are supported only in streaming mutation paths.
// They are disabled by default and require ParseMutationsWithOptions:
//
//	muts, _ := lql.ParseMutationsWithOptions([]string{`file:/payload=blob.bin`}, time.Now(), lql.ParseMutationsOptions{
//	  EnableFileValues: true,
//	  FileValueBaseDir: "/workdir",
//	})
//
// Streaming mutation can also create a new JSON object from `{}` while loading
// file content into a field:
//
//	muts, _ := lql.ParseMutationsWithOptions([]string{
//	  `/filename=notes.txt`,
//	  `/tags/kind=document`,
//	  `/tags/source=local`,
//	  `textfile:/content=notes.txt`,
//	}, time.Now(), lql.ParseMutationsOptions{
//	  EnableFileValues: true,
//	  FileValueBaseDir: ".",
//	})
//	_ = lql.MutateStream(lql.MutateStreamRequest{
//	  Reader: strings.NewReader(`{}`),
//	  Writer: os.Stdout,
//	  Mutations: muts,
//	})
//
// Example:
//
//	doc := map[string]any{"state": map[string]any{"retries": 1}}
//	_ = lql.Mutate(doc, "/state/retries=+2", "/state/status=running")
//
// Comma/newline separated mutation strings can also drive streaming mutation:
//
//	muts, _ := lql.ParseMutationsString("/filename=notes.txt,\n/tags/kind=document,\n/tags/source=local", time.Now())
//	_ = lql.MutateStream(lql.MutateStreamRequest{
//	  Reader: strings.NewReader(`{}`),
//	  Writer: os.Stdout,
//	  Mutations: muts,
//	})
//
// The package is intentionally small and dependency-free so it can be embedded
// in CLIs and services that need LQL parsing or evaluation.
//
// # Streaming Queries
//
// Query streams can be evaluated without materializing full JSON objects:
//
//	sel, _ := lql.ParseSelectorString(`/status="open"`)
//	_ = lql.QueryStream(lql.QueryStreamRequest{
//	  Reader: strings.NewReader(`{"status":"open"}`),
//	  Ctx: context.Background(),
//	  Selector: sel,
//	  Mode: lql.QueryDecisionPlusValue,
//	  MatchedOnly: true,
//	  OnValue: func(v lql.QueryStreamValue) error {
//	    if v.Matched {
//	      // v.JSON contains candidate payload bytes when in-memory.
//	      // If spooled to disk, use v.OpenJSON.
//	    }
//	    return nil
//	  },
//	})
//
// # Streaming Mutations
//
// Mutation streams can be applied candidate-by-candidate without materializing
// the full input stream:
//
//	muts, _ := lql.ParseMutations([]string{`/state/status=running`}, time.Now())
//	_ = lql.MutateStream(lql.MutateStreamRequest{
//	  Reader: strings.NewReader(`{"state":{"status":"queued"}}`),
//	  Ctx: context.Background(),
//	  Writer: io.Discard, // optional compact NDJSON output sink
//	  SpoolMemoryBytes: 1 << 20, // optional callback payload memory threshold
//	  Mode: lql.MutateSingleObjectOnly,
//	  Mutations: muts,
//	  OnValue: func(v lql.MutateStreamValue) error {
//	    // v.JSON/v.Value are in-memory when available.
//	    // v.OpenJSON works for both in-memory and spooled payloads.
//	    return nil
//	  },
//	})
package lql
