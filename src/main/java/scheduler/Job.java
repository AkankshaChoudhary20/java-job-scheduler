package scheduler;

public final class Job {
    public final long id;
    public final String name;
    public final int attempts;
    public final int maxAttempts;
    public final Integer intervalSeconds; // null = one-shot

    public Job(long id, String name, int attempts, int maxAttempts, Integer intervalSeconds) {
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
