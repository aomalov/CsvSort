package threading;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.*;

public class ExecutorPool implements Closeable {

    private final ExecutorService executorService;
    private final Semaphore lock;

    public ExecutorPool(int maxThreads) {
        executorService= Executors.newFixedThreadPool(maxThreads);
        lock = new Semaphore(maxThreads);
    }

    public void submitTask(PartialSortingTask newTask) throws InterruptedException {
        newTask.setCallback(v->lock.release());
        lock.acquire();
        executorService.execute(newTask);
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
    }
}
