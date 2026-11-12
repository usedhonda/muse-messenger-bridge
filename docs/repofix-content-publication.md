# Verified, immutable content publication

## Finding and change

The previous content transfer checked for an existing final filename in one SSH
command and later renamed a shared `.part` file over that name. A writer that
appeared between the commands could be overwritten. The final filename also
became visible before the byte count was checked, and received bytes were not
hashed before publication. A configured directory containing an apostrophe broke
the shell quoting.

`stream_to_mac` now invokes the configured `receiver.python` interpreter over
the same authenticated SSH route, passing all arguments through shell quoting.
This interpreter (default `/usr/bin/python3`) must be available even when
acceptance is performed manually instead of through the receiver module.
Only the Python standard library is used; no provider/model/API dependency is
introduced.

The receiver writes SSH stdin to a private unique temporary file in the content
directory. It verifies the exact byte count and SHA-256, synchronizes the file,
and creates the final name with `os.link`. An existing destination of any type
causes failure instead of being overwritten. The final file is mode 0600;
ordinary failure paths remove the temporary file. Source bodies are never
written to local producer files or diagnostic output. The filename, batch ID,
payload, completion notice and explicit receiver-ACK protocol remain unchanged.

## Validation

Baseline repository test inventory: no test files. This change adds three
behavioral unittest methods; tests removed/consolidated: 0/0.

`python -m unittest discover -s tests -p 'test_content_publication.py' -v`
passes all three methods. The same tests against the original source reproduce
three assertion failures and one error (including subtests): corrupted
same-length bytes are accepted, truncated bytes become visible as final,
apostrophes break the command, and a competing final file is overwritten.

The tests execute the actual generated receiver command through a local shell
and Python process, with synthetic bytes and temporary directories. Only the
SSH transport boundary is replaced. They verify byte identity, mode 0600,
cleanup, corruption/truncation rejection and a deterministic publication race.
Imports suppress runtime configuration reads. No Messenger data, credentials,
live host, message send or deployment was accessed.

## Limits and follow-up

The remote filesystem must support same-directory hard links; there is no
unsafe rename fallback. A failure after publication or an interrupted SSH
response is still an unknown handover outcome. The companion pending-state fix
preserves the exact filename/hash for later receiver diagnosis; publication
alone does not establish acceptance. A process killed before cleanup may leave
a private temporary file on the receiver. No physical power-loss or actual Mac
host test was performed.

Rollback is a code revert; no saved-state or receiver protocol migration is
required. Reverting restores the previous overwrite and validation races.

Primary API references: [os.link](https://docs.python.org/3/library/os.html#os.link)
and [tempfile.mkstemp](https://docs.python.org/3/library/tempfile.html#tempfile.mkstemp).
