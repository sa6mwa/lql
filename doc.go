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
//	icontains{field=/message,value=timeout}
//	prefix{field=/owner,value=team-}
//	iprefix{field=/owner,value=team-}
//	range{field=/progress,gt=10}
//	in{field=/state,any=["queued","running"]}
//	exists{/metadata/etag}
//
// String terms (`contains`/`prefix`) support `ignoreCase=true|false`
// (or shorthand `ic=t|f`) for per-clause case handling.
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
//	time:/state/updated=NOW      # RFC3339 timestamp
//
// Mutations support the same wildcard semantics as selectors. When a wildcard
// path segment is used, missing paths under matched nodes are skipped.
//
// Brace shorthand applies a set of nested mutations under a prefix:
//
//	/state{/owner="alice",/note="hi"}
//
// Example:
//
//	doc := map[string]any{"state": map[string]any{"retries": 1}}
//	_ = lql.Mutate(doc, "/state/retries=+2", "/state/status=running")
//
// The package is intentionally small and dependency-free so it can be embedded
// in CLIs and services that need LQL parsing or evaluation.
package lql
