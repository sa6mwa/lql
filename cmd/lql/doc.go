// Command lql evaluates LQL selectors and applies LQL mutations over JSON input.
//
// Selector highlights:
//   - Shorthand selectors: /field="value", /field!=value, /field>=10.
//   - Datetime shorthand range selectors:
//     /timestamp>=2026-03-05T10:28:21Z
//   - Explicit date selector:
//     date{field=/timestamp,after=2025-01-01,before=2025-02-01}
//   - date alias and macro support:
//     date{f=/timestamp,a=2025-01-01,b=2025-01-03}
//     date{f=/timestamp,since=yesterday}
//
// Use `lql -h` for full selector, mutation, and output examples.
package main
