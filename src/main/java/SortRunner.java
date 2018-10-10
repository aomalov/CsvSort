import Utils.DataGenerator;
import com.google.common.collect.ImmutableList;
import threading.ExecutorPool;
import threading.PartMerger;
import threading.PartialSortingTask;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

public class SortRunner {

    private final int MAX_CAPACITY = 5000;
    private final int MAX_PARALLEL = 2;

    //Read sequentially
    private List<String> divideToSortedParts(String path) throws IOException, InterruptedException {
        AtomicInteger counter=new AtomicInteger(0);
        List<String> parts = new ArrayList<>();

        try (FileInputStream inputStream = new FileInputStream(path);
             Scanner sc = new Scanner(inputStream, "UTF-8");
             ExecutorPool limitedThreadPool = new ExecutorPool(MAX_PARALLEL)) {
            List<String> buffer = new ArrayList<>();
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                buffer.add(line);
                if (buffer.size() == MAX_CAPACITY) {
                    System.out.println("submitting part #"+(counter.get()+1));
                    String sortedPart=String.format("./src/main/resources/sorted-part-%d.csv",counter.incrementAndGet());
                    parts.add(sortedPart);
                    limitedThreadPool.submitTask(new PartialSortingTask(sortedPart, ImmutableList.copyOf(buffer)));
                    buffer.clear();
                }
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        }
        return parts;
    }

    private String mergeFiles(List<String> sortedParts) {
        ForkJoinPool pool = new ForkJoinPool();
        PartMerger mergeTask= new PartMerger(sortedParts);
        pool.execute(mergeTask);
        String res= mergeTask.join();
        pool.shutdown();
        return res;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        DataGenerator.generate(55000);

        SortRunner runner=new SortRunner();
        List<String> splitSet = runner.divideToSortedParts("./src/main/resources/sample.csv");
        //TODO wait for Executor pool to get gracefully terminated and release resources
        Thread.sleep(1000);
        String resultingFile= runner.mergeFiles(splitSet);
        System.out.println("Result file: "+resultingFile);
    }
}
