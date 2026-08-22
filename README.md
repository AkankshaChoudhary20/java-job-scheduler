# java-job-scheduler

A DB-backed job scheduler that runs recurring/one-shot jobs exactly once
across multiple app instances -- the thing you need the moment you scale
past one process and two of them try to run the same cron job
simultaneously.

Plain JDK only for the code itself -- no Maven/Gradle build system. The
one exception is a vendored `sqlite-jdbc` jar (fetched by a script, not
committed to git), because testing "exactly-once across concurrent
database connections" honestly requires an actual JDBC-backed SQL
database, not a mock.

## The problem this solves

Cron works fine until you run more than one instance of your app. Then
every instance's cron fires the same job at the same time, and you get
duplicate emails, duplicate charges, duplicate whatever. The fix is
claiming jobs from a shared database with an operation that's atomic
across connections.

## How it works

SQLite has no `SELECT ... FOR UPDATE SKIP LOCKED` (that's a Postgres/
MySQL 8+ feature), so this uses the more portable alternative that works
identically on any SQL engine: find a candidate row, then claim it with a
conditional `UPDATE jobs SET status='running', locked_by=? WHERE id=? AND
status='pending'`. If the update affects 0 rows, another worker won the
race for that row -- try the next candidate. No row is ever double-claimed
because the claim itself is a single atomic UPDATE statement, not a
read-then-write.

- **One-shot jobs** go `pending -> running -> done`.
- **Recurring jobs** go `pending -> running -> pending` (rescheduled
  `run_at = now + interval`) instead of `done`.
- **Failures** retry with exponential backoff (2s, 4s, 8s...) until
  `max_attempts`, then move to `dead` (dead-letter) so a permanently
  broken job stops being retried forever.
- **Stale locks** (a worker that claimed a job and then crashed) are
  reclaimed after a configurable timeout, checked on every claim attempt.

## Run it

```bash
./run.sh scheduler-demo.db 4    # 4 concurrent "worker instances" against one DB file
```

Seeds 10 one-shot jobs and one 5-second recurring "heartbeat", then 4
worker threads (each its own JDBC connection, standing in for separate
app processes) drain the queue concurrently. Ran this for real: the 10
one-shot jobs were split across all 4 workers with no duplicates, and the
heartbeat fired at 5.1s intervals as configured (11.96s, 17.08s, 22.21s
into a live run) -- not a race, an actual correctly-spaced recurring job.

## Tests

```bash
./test.sh
```

No JUnit -- a small hand-rolled harness, same as the inventory-reservation
project, since the whole point was staying build-tool-free. 7 tests, run
against a real temp SQLite file (not mocked):

- Single-connection correctness: claim assigns exactly once, a job not
  yet due isn't claimable, one-shot completion marks `done`, recurring
  completion reschedules `run_at` forward, failures retry with backoff
  then dead-letter after `max_attempts`, a stale lock gets reclaimed.
- **`testConcurrentWorkersNeverDoubleExecuteJobs`** -- the actual point
  of this project. 300 one-shot jobs, 10 worker threads *each with its
  own JDBC `Connection`* (genuinely separate connections to the same
  database file, not shared state), released simultaneously, draining
  the queue until empty. Asserts every job executed exactly once -- zero
  double-executions, zero skipped jobs. Ran 5 times back to back to rule
  out an occasional race -- passed every time.

Two real bugs surfaced and got fixed while building this (not
hypothetical -- actually hit while running the tests):
1. The naive schema-file loader split SQL on `;`, which broke because an
   inline comment in `schema.sql` itself contained a semicolon.
2. `sqlite-jdbc`'s `ResultSet.getObject(col, Integer.class)` throws on a
   NULL integer column instead of returning `null` -- switched to the
   standard `getInt()` + `wasNull()` pattern.

## What's intentionally not here

No leader election, no job priorities/queues beyond FIFO-by-`run_at`, no
persistence format migration tooling. The claiming algorithm (candidate
SELECT + conditional UPDATE) is the same one that works against Postgres
or MySQL -- swapping the JDBC URL and vendoring a different driver jar is
the only change needed to run this against a real production database
instead of SQLite.
