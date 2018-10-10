package threading;

import com.opencsv.CSVWriter;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import model.DataPojo;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PartialSortingTask implements Runnable {

    private final String partName;
    private final List<String> buffer;
    private Consumer<Void> callback;

    public PartialSortingTask(String partName, List<String> buffer) {
        this.partName = partName;
        this.buffer=buffer;
    }

    public void setCallback(Consumer<Void> callback) {
        this.callback=callback;
    }

    @Override
    public void run() {
        //SORT
        List<DataPojo> part = buffer.stream().map(DataPojo::new).sorted().collect(Collectors.toList());
        //PERSIST
        try (
                Writer writer = Files.newBufferedWriter(Paths.get(partName));
        ) {
            StatefulBeanToCsv<DataPojo> beanToCsv = new StatefulBeanToCsvBuilder(writer)
                                                        .withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
                                                        .build();

            System.out.println(">> Writing "+partName+" in "+Thread.currentThread().getName());
            beanToCsv.write(part);
        } catch (IOException | CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
            e.printStackTrace();
        } finally {
            callback.accept(null);
        }
    }
}
