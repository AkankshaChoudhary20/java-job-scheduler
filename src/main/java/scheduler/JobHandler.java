package scheduler;

@FunctionalInterface
public interface JobHandler {
    void run(Job job) throws Exception;
}
