package scheduler;

import java.io.File;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Plain-JDK test harness (no JUnit): each test method returns normally on
 * success or throws AssertionError on failure. main() runs them all and
 * exits non-zero if anything failed.
 */
public final class JobSchedulerTest {

    public static void main(String[] args) throws Exception {
        int failures = 0;
        failures += runTest("testInvalidTimeoutsAndRecurringIntervalsAreRejected", JobSchedulerTest::testInvalidTimeoutsAndRecurringIntervalsAreRejected);
        failures += runTest("testStaleWorkerCannotFinalizeReclaimedJob", JobSchedulerTest::testStaleWorkerCannotFinalizeReclaimedJob);
        failures += runTest("testClaimAssignsJobExactlyOnce", JobSchedulerTest::testClaimAssignsJobExactlyOnce);
        failures += runTest("testClaimReturnsEmptyWhenNoJobsReady", JobSchedulerTest::testClaimReturnsEmptyWhenNoJobsReady);
        failures += runTest("testCompleteOneShotJobMarksDone", JobSchedulerTest::testCompleteOneShotJobMarksDone);
        failures += runTest("testCompleteRecurringJobReschedules", JobSchedulerTest::testCompleteRecurringJobReschedules);
        failures += runTest("testFailJobRetriesThenGoesDead", JobSchedulerTest::testFailJobRetriesThenGoesDead);
        failures += runTest("testStaleLockIsReclaimed", JobSchedulerTest::testStaleLockIsReclaimed);
        failures += runTest("testConcurrentWorkersNeverDoubleExecuteJobs", JobSchedulerTest::testConcurrentWorkersNeverDoubleExecuteJobs);

        if (failures > 0) {
            System.out.println("\n" + failures + " test(s) FAILED");
            System.exit(1);
        }
        System.out.println("\nALL TESTS PASSED");
    }

    private interface FallibleTest {
        void run() throws Exception;
    }

    private static int runTest(String name, FallibleTest test) {
        try {
            test.run();
            System.out.println("OK:   " + name);
            return 0;
        } catch (AssertionError e) {
            System.out.println("FAIL: " + name + " -- " + e.getMessage());
            return 1;
        } catch (Exception e) {
            System.out.println("FAIL: " + name + " -- unexpected exception: " + e);
            e.printStackTrace();
            return 1;
        }
    }

    private static void assertEquals(Object expected, Object actual, String msg) {
        if (!expected.equals(actual)) {
            throw new AssertionError(msg + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }

    private static void assertTrue(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    /** Creates a fresh temp SQLite file with the schema applied, deleted on JVM exit. */
    private static String freshDb() throws Exception {
        File f = File.createTempFile("scheduler-test-", ".db");
        f.deleteOnExit();
        new File(f.getPath() + "-wal").deleteOnExit();
        new File(f.getPath() + "-shm").deleteOnExit();
        try (Connection conn = Db.connect(f.getPath())) {
            Db.initSchema(conn);
        }
        return f.getPath();
    }

    static void testStaleWorkerCannotFinalizeReclaimedJob() throws Exception {
        String path = freshDb();
        JobScheduler scheduler = new JobScheduler(Duration.ofSeconds(1));
        try (Connection conn = Db.connect(path)) {
            long id = scheduler.scheduleOnce(conn, "reclaimed", Instant.now().minusSeconds(1));
            Job oldClaim = scheduler.claimNextJob(conn, "same-worker").orElseThrow();
            scheduler.forceSetLockedAt(conn, id, Instant.now().minusSeconds(10));
            Job newClaim = scheduler.claimNextJob(conn, "same-worker").orElseThrow();
            assertTrue(!oldClaim.lockOwner.equals(newClaim.lockOwner), "each claim needs a unique lease");
            scheduler.completeJob(conn, oldClaim);
            assertEquals("running", scheduler.getStatus(conn, id), "old completion must not finalize new claim");
            scheduler.failJob(conn, oldClaim, "stale error");
            assertEquals("running", scheduler.getStatus(conn, id), "old failure must not requeue new claim");
            assertEquals(0, scheduler.getAttempts(conn, id), "stale failure must not increment attempts");
            scheduler.completeJob(conn, newClaim);
            assertEquals("done", scheduler.getStatus(conn, id), "current owner can complete");
        }
    }

    // -- single-worker correctness --

    static void testClaimAssignsJobExactlyOnce() throws Exception {
        String dbPath = freshDb();
        JobScheduler sched = new JobScheduler(Duration.ofMinutes(5));
        try (Connection conn = Db.connect(dbPath)) {
            long id = sched.scheduleOnce(conn, "send-email", Instant.now().minusSeconds(1));

            Optional<Job> claimed = sched.claimNextJob(conn, "worker-1");
            assertTrue(claimed.isPresent(), "expected a job to be claimed");
            assertEquals(id, claimed.get().id, "claimed the wrong job");
            assertEquals("running", sched.getStatus(conn, id), "status should be 'running' after claim");
        }
    }

    static void testClaimReturnsEmptyWhenNoJobsReady() throws Exception {
        String dbPath = freshDb();
        JobScheduler sched = new JobScheduler(Duration.ofMinutes(5));
        try (Connection conn = Db.connect(dbPath)) {
            sched.scheduleOnce(conn, "future-job", Instant.now().plusSeconds(3600));
            Optional<Job> claimed = sched.claimNextJob(conn, "worker-1");
            assertTrue(claimed.isEmpty(), "a job scheduled an hour in the future should not be claimable yet");
        }
    }

    static void testCompleteOneShotJobMarksDone() throws Exception {
        String dbPath = freshDb();
        JobScheduler sched = new JobScheduler(Duration.ofMinutes(5));
        try (Connection conn = Db.connect(dbPath)) {
            long id = sched.scheduleOnce(conn, "one-shot", Instant.now().minusSeconds(1));
            Job job = sched.claimNextJob(conn, "worker-1").orElseThrow();
            sched.completeJob(conn, job);
            assertEquals("done", sched.getStatus(conn, id), "one-shot job should be 'done' after completion");
        }
    }

    static void testCompleteRecurringJobReschedules() throws Exception {
        String dbPath = freshDb();
        JobScheduler sched = new JobScheduler(Duration.ofMinutes(5));
        try (Connection conn = Db.connect(dbPath)) {
            long id = sched.scheduleRecurring(conn, "heartbeat", Instant.now().minusSeconds(1), 60);
            Job job = sched.claimNextJob(conn, "worker-1").orElseThrow();
            String beforeRunAt = sched.getRunAt(conn, id);

            sched.completeJob(conn, job);

            assertEquals("pending", sched.getStatus(conn, id), "recurring job should go back to 'pending', not 'done'");
            String afterRunAt = sched.getRunAt(conn, id);
            assertTrue(afterRunAt.compareTo(beforeRunAt) > 0, "run_at should have advanced into the future after completion");
        }
    }

    static void testFailJobRetriesThenGoesDead() throws Exception {
        String dbPath = freshDb();
        JobScheduler sched = new JobScheduler(Duration.ofMinutes(5));
        try (Connection conn = Db.connect(dbPath)) {
            long id = sched.scheduleOnce(conn, "flaky-job", Instant.now().minusSeconds(1));

            // max_attempts defaults to 3 -- fail it 3 times, forcing run_at back to "now"
            // each time so the next claim can find it despite backoff.
            for (int i = 0; i < 3; i++) {
                Job job = sched.claimNextJob(conn, "worker-1").orElseThrow();
                sched.failJob(conn, job, "boom " + i);
                if (i < 2) {
                    forceRunAtToNow(conn, id);
                }
            }

            assertEquals("dead", sched.getStatus(conn, id), "job should be dead-lettered after exhausting max_attempts");
            assertEquals(3, sched.getAttempts(conn, id), "attempts should equal max_attempts");
            assertTrue(sched.claimNextJob(conn, "worker-1").isEmpty(), "a dead job must never be claimable again");
        }
    }

    private static void forceRunAtToNow(Connection conn, long id) throws Exception {
        try (var ps = conn.prepareStatement("UPDATE jobs SET run_at=? WHERE id=?")) {
            ps.setString(1, Instant.now().minusSeconds(1).toString());
            ps.setLong(2, id);
            ps.executeUpdate();
        }
    }

    static void testStaleLockIsReclaimed() throws Exception {
        String dbPath = freshDb();
        JobScheduler sched = new JobScheduler(Duration.ofSeconds(1)); // 1s stale timeout for a fast test
        try (Connection conn = Db.connect(dbPath)) {
            long id = sched.scheduleOnce(conn, "crashed-job", Instant.now().minusSeconds(1));
            // Simulate a worker that claimed the job 10 seconds ago and then crashed.
            sched.forceSetLockedAt(conn, id, Instant.now().minusSeconds(10));

            Optional<Job> reclaimed = sched.claimNextJob(conn, "worker-2");
            assertTrue(reclaimed.isPresent(), "expected the stale-locked job to be reclaimed and re-claimed");
            assertEquals(id, reclaimed.get().id, "reclaimed the wrong job");
        }
    }

    // -- the real point of this project: exactly-once across concurrent workers --

    /**
     * 300 one-shot jobs, all due now. 10 "worker instances" (each with its
     * own JDBC Connection, simulating separate processes) race to drain the
     * queue concurrently. If claimNextJob's atomic-update claiming has any
     * race, a job either gets executed twice (recorded via a per-job
     * execution counter) or gets missed entirely.
     */
    static void testConcurrentWorkersNeverDoubleExecuteJobs() throws Exception {
        String dbPath = freshDb();
        JobScheduler sched = new JobScheduler(Duration.ofMinutes(5));
        int jobCount = 300;
        int workerCount = 10;

        List<Long> jobIds;
        try (Connection conn = Db.connect(dbPath)) {
            jobIds = new java.util.ArrayList<>();
            for (int i = 0; i < jobCount; i++) {
                jobIds.add(sched.scheduleOnce(conn, "job-" + i, Instant.now().minusSeconds(1)));
            }
        }

        ConcurrentHashMap<Long, AtomicInteger> executionCounts = new ConcurrentHashMap<>();
        for (long id : jobIds) {
            executionCounts.put(id, new AtomicInteger(0));
        }

        AtomicReference<Throwable> firstError = new AtomicReference<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);

        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        for (int w = 0; w < workerCount; w++) {
            String workerId = "worker-" + w;
            pool.submit(() -> {
                try (Connection conn = Db.connect(dbPath)) {
                    startLatch.await();
                    while (true) {
                        Optional<Job> maybeJob = sched.claimNextJob(conn, workerId);
                        if (maybeJob.isEmpty()) {
                            break; // queue drained
                        }
                        Job job = maybeJob.get();
                        executionCounts.get(job.id).incrementAndGet();
                        sched.completeJob(conn, job);
                    }
                } catch (Throwable e) {
                    firstError.compareAndSet(null, e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(60, TimeUnit.SECONDS);
        pool.shutdown();

        if (firstError.get() != null) {
            throw new AssertionError("worker thread threw: " + firstError.get());
        }

        int neverExecuted = 0;
        int doubleExecuted = 0;
        for (var entry : executionCounts.entrySet()) {
            int count = entry.getValue().get();
            if (count == 0) neverExecuted++;
            if (count > 1) doubleExecuted++;
        }

        assertEquals(0, neverExecuted, "jobs that were never executed");
        assertEquals(0, doubleExecuted, "jobs that were executed more than once");

        try (Connection conn = Db.connect(dbPath)) {
            assertEquals(jobCount, sched.countByStatus(conn, "done"), "every job should end up 'done'");
        }
    }

    static void testInvalidTimeoutsAndRecurringIntervalsAreRejected() throws Exception {
        for (Duration timeout : new Duration[]{Duration.ZERO, Duration.ofSeconds(-1)}) {
            try {
                new JobScheduler(timeout);
                throw new AssertionError("stale timeout must be positive");
            } catch (IllegalArgumentException expected) { }
        }
        JobScheduler scheduler = new JobScheduler(Duration.ofMinutes(5));
        for (int interval : new int[]{0, -1, Integer.MIN_VALUE}) {
            try {
                scheduler.scheduleRecurring(null, "invalid", Instant.now(), interval);
                throw new AssertionError("interval must be validated before accessing the database");
            } catch (IllegalArgumentException expected) { }
        }
    }
}
