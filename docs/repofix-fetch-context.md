# On-request context: validation and handoff

## Problem and change

The baseline fetcher at `b5c53dd723a0ec3087a9872d32dde3669e93f928`
returned the most recent capped slice without saying that context had
been omitted. A source that repeated a page could also stop acquisition
without a completeness signal. The protocol already promises explicit
truncation reporting for this path.

`fetch_window(cid, min_count, days, cap)` still returns the same
`(metadata, rows)` tuple used by the stream producer. Its metadata now
includes `context_truncation`, also included in the on-request JSON:

```json
{"context_truncation": {"truncated": true, "reason": "cap"}}
```

| Reason | Meaning |
| --- | --- |
| `null`, with `truncated: false` | The requested window was reached or the source returned an empty/short final page, without a detected truncation. |
| `cap` | More requested rows were fetched than can be returned, or a full page reached the cap before completeness was established. The latter is conservative: more rows may exist, but their number is unknown. |
| `pagination_stalled` | A page added no new rows or failed to advance the oldest timestamp. The returned context is partial. |

The newest requested rows remain oldest-first in the output. The default
is the larger of 100 recent messages and seven days of context, capped at
500. A zero-day window retains the stream caller's existing behavior:
read all retained context up to the cap. Arguments must be integers with
`1 <= limit <= cap` and `days >= 0`; invalid CLI bounds exit with status 2
before consulting the source.

Messenger subprocesses now have a 120-second timeout. Nonzero exits,
timeouts, unreadable output, invalid JSON, explicit `success: false`, and
non-object JSON produce sanitized errors. Thread/message collection
shapes and message timestamps are validated before use. A malformed
response is an error, not an empty successful context. Source failures
exit with status 1 and print no source arguments, raw IDs, stdout,
stderr, or chained subprocess exception to diagnostics. Successful JSON
still intentionally includes the requested message bodies.

## Source paging limitation

The available connector interface pages with `--before <timestamp>`.
This repository does not establish whether the cursor is inclusive or
exclusive, or provide an additional stable cursor for messages sharing a
timestamp. This patch detects caps and lack of cursor progress; it does
not claim to recover messages hidden at a same-timestamp source page
boundary. A `truncated: false` result means no truncation was detected
under the existing timestamp-based paging contract, not a proof of
source-side completeness. The external Messenger connector would need
to expose a documented stable cursor to remove that ambiguity.

## Regression evidence and test inventory

The baseline contains no test files or test configuration.
`python -m unittest discover -v` ran zero tests and returned the
environment's `NO TESTS RAN` exit status 5. There were no existing tests
to delete or consolidate safely: removals **0**, consolidations **0**.

The added suite has **14 test methods**. Running the final suite against
the unchanged implementation failed with **31 assertion failures and
11 errors**, including parameterized subtests. The capped five-row
fixture returned four rows without metadata. The forced CLI failure
included the synthetic raw conversation ID in its exception, and a
timeout exposed its command. These are reproduced failures, not
inferences from style checks.

Run the focused suite from the repository root:

```sh
python -m unittest tests.test_fetch_thread_context -v
```

All 14 methods pass after the fix. They exercise the count/day window,
the stream-compatible zero-day window, cap and exact-exhaustion cases,
empty and repeated/non-advancing pages, invalid bounds and malformed
responses, finite subprocess timeouts, sanitized exceptions, and the
public JSON/exit-status contract. All pages and failures are synthetic;
subprocesses are mocked and configuration reads are blocked during
module import. No live Messenger, SSH, receiver, runtime database,
state file, credential, or message send is used. No packages are added.

## Model and dependency inventory

The audit covers all nine tracked baseline files: four Python scripts,
`README.md`, `docs/protocol.md`, `docs/schema.md`,
`config.example.json`, and `.gitignore`.

| Surface | Inventory and conclusion |
| --- | --- |
| Python imports in all four scripts | Python standard library and the repository's own `fetch_thread_context` module only. No third-party Python dependency manifests are present. |
| Provider and model selection | No model identifiers, AI provider SDKs, provider HTTP calls, or model-selection configuration are present. No model migration or provider connectivity probe applies. |
| Messenger access | External `messenger_cli`, configured by a local path. Its implementation and dependencies are outside this repository and were not inspected. |
| Transfers and handover | External SSH/SCP, a configured receiver Python module, and a configured connector client for notices. These are execution prerequisites, not model-provider integrations established by this code. |
| Example configuration | Placeholder connector/host/path settings only; no model or provider key settings. |
| Runtime material | The real local configuration, databases, state, and exports are excluded from the repository and were not read. |

The model/dependency conclusion is limited to this repository. It makes
no claim about model use inside the external hub or connector processes.
