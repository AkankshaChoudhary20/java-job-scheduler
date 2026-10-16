package scheduler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * DB-backed job claiming/completion, safe across multiple worker
 * processes/instances sharing the same database.
 *
 * <p>Since SQLite has no {@code SELECT ... FOR UPDATE SKIP LOCKED}, claiming
 * uses the portable alternative: find a candidate row, then claim it with an
 * atomic conditional {@code UPDATE ... WHERE id = ? AND locked_by IS NULL}.
 * If zero rows are affected, another worker won the race for that row, so
 * the caller just tries the next candidate. This pattern works identically
 * against Postgres/MySQL, which is why it's the more portable technique even
 * though {@code SKIP LOCKED} exists there.
 */
public final class JobScheduler {

    private final Duration staleLockTimeout;

    public JobScheduler(Duration staleLockTimeout) {
        this.staleLockTimeout = staleLockTimeout;
    }

    public long scheduleOnce(Connection conn, String name, Instant runAt) throws SQLException {
        return insert(conn, name, runAt, null);
    }

    public long scheduleRecurring(Connection conn, String name, Instant firstRunAt, int intervalSeconds) throws SQLException {
        return insert(conn, name, firstRunAt, intervalSeconds);
    }

    private long insert(Connection conn, String name, Instant runAt, Integer intervalSeconds) throws SQLException {
        String sql = "INSERT INTO jobs (name, run_at, interval_seconds) VALUES (?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, name);
            ps.setString(2, runAt.toString());
            if (intervalSeconds == null) {
                ps.setNull(3, java.sql.Types.INTEGER);
            } else {
                ps.setInt(3, intervalSeconds);
            }
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                keys.next();
                return keys.getLong(1);
            }
        }
    }

    /**
     * Reclaims jobs stuck in 'running' with a lock older than the stale
     * timeout -- covers a worker that crashed mid-job without ever calling
     * complete()/fail().
     */
    public int reclaimStaleLocks(Connection conn) throws SQLException {
        String cutoff = Instant.now().minus(staleLockTimeout).toString();
        String sql = "UPDATE jobs SET status='pending', locked_by=NULL, locked_at=NULL " +
                "WHERE status='running' AND locked_at < ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, cutoff);
            return ps.executeUpdate();
        }
    }

    /**
     * Attempts to claim one due, unlocked job for {@code workerId}. Retries
     * against fresh candidates if it loses a race on a particular row.
     */
    public Optional<Job> claimNextJob(Connection conn, String workerId) throws SQLException {
        reclaimStaleLocks(conn);

        String now = Instant.now().toString();
        String selectSql = "SELECT id FROM jobs WHERE status='pending' AND run_at <= ? " +
                "ORDER BY run_at LIMIT 20";

        for (long candidateId : candidateIds(conn, selectSql, now)) {
            String claimSql = "UPDATE jobs SET status='running', locked_by=?, locked_at=? " +
                    "WHERE id=? AND status='pending'";
            try (PreparedStatement ps = conn.prepareStatement(claimSql)) {
                String lockOwner = workerId + ":" + java.util.UUID.randomUUID();
                ps.setString(1, lockOwner);
                ps.setString(2, Instant.now().toString());
                ps.setLong(3, candidateId);
                int updated = ps.executeUpdate();
                if (updated == 1) {
                    return loadJob(conn, candidateId, lockOwner);
                }
                // lost the race for this row -- try the next candidate
            }
        }
        return Optional.empty();
    }

    private java.util.List<Long> candidateIds(Connection conn, String sql, String now) throws SQLException {
        java.util.List<Long> ids = new java.util.ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, now);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                }
            }
        }
        return ids;
    }

    private Optional<Job> loadJob(Connection conn, long id, String lockOwner) throws SQLException {
        String sql = "SELECT id, name, attempts, max_attempts, interval_seconds FROM jobs WHERE id=? AND status='running' AND locked_by=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, id);
            ps.setString(2, lockOwner);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                int intervalRaw = rs.getInt("interval_seconds");
                Integer interval = rs.wasNull() ? null : intervalRaw;
                return Optional.of(new Job(
                        rs.getLong("id"),
                        rs.getString("name"),
                        rs.getInt("attempts"),
                        rs.getInt("max_attempts"),
                        interval, lockOwner));
            }
        }
    }

    /** Marks the current claim successful. Stale claims cannot modify a reclaimed job. */
    public void completeJob(Connection conn, Job job) throws SQLException {
        if (job.intervalSeconds == null) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE jobs SET status='done', locked_by=NULL, locked_at=NULL WHERE id=? AND status='running' AND locked_by=?")) {
                ps.setLong(1, job.id);
                ps.setString(2, job.lockOwner);
                ps.executeUpdate();
            }
        } else {
            String nextRun = Instant.now().plusSeconds(job.intervalSeconds).toString();
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE jobs SET status='pending', run_at=?, locked_by=NULL, locked_at=NULL, " +
                            "attempts=0, last_error=NULL WHERE id=? AND status='running' AND locked_by=?")) {
                ps.setString(1, nextRun);
                ps.setLong(2, job.id);
                ps.setString(3, job.lockOwner);
                ps.executeUpdate();
            }
        }
    }

    /**
     * Marks the current claim failed. Stale claims cannot modify a reclaimed job.
     * Retries with exponential backoff until
     * max_attempts is reached, then moves it to the 'dead' (dead-letter)
     * state so it stops being picked up.
     */
    public void failJob(Connection conn, Job job, String error) throws SQLException {
        int attempts = job.attempts + 1;
        if (attempts >= job.maxAttempts) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE jobs SET status='dead', attempts=?, last_error=?, locked_by=NULL, locked_at=NULL WHERE id=? AND status='running' AND locked_by=?")) {
                ps.setInt(1, attempts);
                ps.setString(2, error);
                ps.setLong(3, job.id);
                ps.setString(4, job.lockOwner);
                ps.executeUpdate();
            }
            return;
        }

        long backoffSeconds = 1L << attempts; // 2, 4, 8, ... seconds
        String nextRun = Instant.now().plusSeconds(backoffSeconds).toString();
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE jobs SET status='pending', run_at=?, attempts=?, last_error=?, " +
                        "locked_by=NULL, locked_at=NULL WHERE id=? AND status='running' AND locked_by=?")) {
            ps.setString(1, nextRun);
            ps.setInt(2, attempts);
            ps.setString(3, error);
            ps.setLong(4, job.id);
            ps.setString(5, job.lockOwner);
            ps.executeUpdate();
        }
    }

    // -- inspection helpers, mainly for tests/CLI reporting --

    public String getStatus(Connection conn, long id) throws SQLException {
        return getStringColumn(conn, id, "status");
    }

    public String getRunAt(Connection conn, long id) throws SQLException {
        return getStringColumn(conn, id, "run_at");
    }

    public int getAttempts(Connection conn, long id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT attempts FROM jobs WHERE id=?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    public int countByStatus(Connection conn, String status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM jobs WHERE status=?")) {
            ps.setString(1, status);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    /** Directly sets locked_at, bypassing the normal claim flow -- used by tests to simulate a stale lock. */
    public void forceSetLockedAt(Connection conn, long id, Instant lockedAt) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE jobs SET status='running', locked_by='stale-worker', locked_at=? WHERE id=?")) {
            ps.setString(1, lockedAt.toString());
            ps.setLong(2, id);
            ps.executeUpdate();
        }
    }

    private String getStringColumn(Connection conn, long id, String column) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT " + column + " FROM jobs WHERE id=?")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }
}
