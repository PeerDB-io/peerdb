# GitHub JSON benchmark fixtures

These are snapshots of public GitHub REST API responses fetched on 2026-09-04:

- `issues.json`: https://api.github.com/repos/golang/go/issues?state=all&per_page=10
- `commits.json`: https://api.github.com/repos/kubernetes/kubernetes/commits?per_page=15
- `events.json`: https://api.github.com/events?per_page=100

The benchmark also derives two variants of each file in memory: one with a
large JSON number inserted near the middle, and one containing both a raw
invalid UTF-8 byte and a lone `\\uD800` surrogate escape near the middle.

The API responses are live data. Codex session history records the following
metadata for the snapshots used in the original 2026-08-26 experiment. Those
temporary files no longer exist locally.

| Original file | Bytes | SHA-256 |
|---|---:|---|
| `issues.json` | 77,104 | `398fe0be278ecbc81a68918a1639fdd87cfb6670f4bfa290f6baf0f296330388` |
| `commits.json` | 87,159 | `d637571bb8871b0d1fe94978767f2f5a109cbf8435f15e4caae7dc5f28ef500d` |
| `events.json` | 85,559 | `3677be50b555aa77b67326c9ae517ed85b5897e7bb9a37e447bd175b1b53e235` |

Current checked-in snapshots:

| File | Bytes | SHA-256 |
|---|---:|---|
| `issues.json` | 51,759 | `15fdbf23a808ee9fdddac61fb40334a4e4ac707ccdf974174eca85f1370057de` |
| `commits.json` | 80,262 | `644d78b57a23cb1f838dafd2825a76cce2995447b3dc8a074f24cbc25a2b6f2e` |
| `events.json` | 411,203 | `7faf89e1c1e5a2bc2cae7af3053c2575b3581f52b286c1954c9d3991445b3b67` |
