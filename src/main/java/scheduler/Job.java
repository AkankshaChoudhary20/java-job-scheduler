package scheduler;

public final class Job {
    public final long id;
    public final String name;
    public final int attempts;
    public final int maxAttempts;
    /** Unique token for this claim; only the current claim may finalize a job. */
    public final String lockOwner;
    public final Integer intervalSeconds; // null = one-shot

    public Job(long id, String name, int attempts, int maxAttempts, Integer intervalSeconds) {
        this(id, name, attempts, maxAttempts, intervalSeconds, null);
    }

    public Job(long id, String name, int attempts, int maxAttempts, Integer intervalSeconds, String lockOwner) {
        this.lockOwner = lockOwner;
        this.id = id;
        this.name = name;
        this.attempts = attempts;
        this.maxAttempts = maxAttempts;
        this.intervalSeconds = intervalSeconds;
    }

    @Override
    public String toString() {
        return "Job{id=" + id + ", name='" + name + "', attempts=" + attempts + "}";
    }
}
