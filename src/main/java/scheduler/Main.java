package scheduler;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demo runner: seeds a mix of one-shot and recurring jobs, then starts
 * several worker threads (each its own JDBC connection, standing in for
 * separate app instances sharing one database) that drain the queue
 * concurrently.
 */
public final class Main {
    public static void main(String[] args) throws Exception {
        String dbPath = args.length > 0 ? args[0] : "scheduler-demo.db";
        int workerCount = args.length > 1 ? Integer.parseInt(args[1]) : 4;

        JobScheduler sched = new JobScheduler(Duration.ofMinutes(1));

        try (Connection conn = Db.connect(dbPath)) {
            Db.initSchema(conn);
            for (int i = 0; i < 10; i++) {
                sched.scheduleOnce(conn, "send-email-" + i, Instant.now());
            }
            sched.scheduleRecurring(conn, "heartbeat", Instant.now(), 5);
        }

        AtomicInteger executed = new AtomicInteger(0);
        JobHandler handler = job -> {
            System.out.println("t=" + Instant.now() + " [" + Thread.currentThread().getName() + "] executing " + job);
            Thread.sleep(50); // simulate work
            executed.incrementAndGet();
        };

        System.out.println("starting " + workerCount + " workers against " + dbPath + " (Ctrl+C to stop)");

        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        for (int w = 0; w < workerCount; w++) {
            String workerId = "worker-" + w;
            pool.submit(() -> runWorkerLoop(dbPath, sched, workerId, handler));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nshutting down, executed " + executed.get() + " job runs total");
        }));

        pool.awaitTermination(30, TimeUnit.SECONDS);
    }

    private static void runWorkerLoop(String dbPath, JobScheduler sched, String workerId, JobHandler handler) {
        try (Connection conn = Db.connect(dbPath)) {
            while (!Thread.currentThread().isInterrupted()) {
                Optional<Job> maybeJob = sched.claimNextJob(conn, workerId);
                if (maybeJob.isEmpty()) {
                    Thread.sleep(200);
                    continue;
                }
                Job job = maybeJob.get();
                try {
                    handler.run(job);
                    sched.completeJob(conn, job);
                } catch (Exception e) {
                    sched.failJob(conn, job, e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("[" + workerId + "] fatal error: " + e);
        }
    }
}
