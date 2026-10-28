# Status DB schema (schema_version 3)

The status database is the bridge's *status* channel. It contains no
message bodies, no snippets, and no raw conversation or user IDs.

Conventions:

- `thread_key` / `contact_key` / `call_key` are SHA-256 hex of the source ID.
- `participant_names` and `participant_keys` are JSON arrays stored as TEXT, in the same order; a key element is `null` when the source has none.
- All `*_utc` timestamps are ISO-8601 UTC strings, `NULL` when the source has no value.
- Booleans are integers 0/1.
- Each updater run rewrites the thread index and appends one snapshot and one log row in a single transaction.

```sql
CREATE TABLE messenger_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );

CREATE TABLE messenger_status_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at_utc TEXT NOT NULL,
                connector_connected INTEGER NOT NULL,
                returned_thread_count INTEGER,
                group_thread_count INTEGER,
                one_to_one_thread_count INTEGER,
                inbox_thread_count INTEGER,
                pending_thread_count INTEGER,
                unread_thread_count INTEGER,
                newest_activity_at_utc TEXT,
                oldest_activity_at_utc TEXT,
                threads_last_synced_at_utc TEXT,
                scope_note TEXT
            );

CREATE TABLE messenger_thread_index (
                thread_key TEXT PRIMARY KEY,
                display_name TEXT,
                thread_type TEXT,
                folder TEXT,
                is_unread INTEGER,
                participant_count INTEGER,
                participant_names TEXT,
                updated_at_utc TEXT,
                captured_at_utc TEXT
            , is_e2ee INTEGER, read_timestamp_utc TEXT, provisional INTEGER, participant_keys TEXT);

CREATE TABLE messenger_update_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at_utc TEXT NOT NULL,
                ok INTEGER NOT NULL,
                mode TEXT,
                duration_ms INTEGER,
                error TEXT
            );

CREATE TABLE messenger_contacts (
                    contact_key TEXT PRIMARY KEY,
                    name TEXT,
                    captured_at_utc TEXT
                );

CREATE TABLE messenger_calls (
                    call_key TEXT PRIMARY KEY,
                    started_at_utc TEXT,
                    direction TEXT,
                    call_type TEXT,
                    duration_seconds INTEGER,
                    missed INTEGER,
                    counterparty_name TEXT,
                    captured_at_utc TEXT
                );

CREATE TABLE messenger_thread_stats (
                    thread_key TEXT PRIMARY KEY,
                    message_count INTEGER,
                    newest_message_at_utc TEXT,
                    oldest_message_at_utc TEXT,
                    last_sender_name TEXT,
                    last_message_at_utc TEXT,
                    last_has_attachment INTEGER,
                    last_reaction_count INTEGER,
                    captured_at_utc TEXT
                );

CREATE VIEW latest_messenger_status AS
            SELECT * FROM messenger_status_snapshot ORDER BY id DESC LIMIT 1;

CREATE VIEW messenger_update_health AS
            SELECT * FROM messenger_update_log ORDER BY id DESC LIMIT 1;

```

`latest_messenger_status` and `messenger_update_health` are views over the newest snapshot / log rows.