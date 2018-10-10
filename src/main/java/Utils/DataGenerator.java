package Utils;

import com.opencsv.CSVWriter;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import model.DataPojo;
import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public final class DataGenerator {

    public static void generate(int rows) {
        List<DataPojo> sample = new ArrayList<>();
        RandomStringGenerator generator=new RandomStringGenerator.Builder()
                .withinRange('0', 'z')
                .filteredBy(
//                        CharacterPredicates.LETTERS,
                        CharacterPredicates.DIGITS).build();

        for(int k=0;k<rows;k++) {
            sample.add(new DataPojo(generator.generate(5),generator.generate(10)));
        }
        //PERSIST
        try (Writer writer = Files.newBufferedWriter(Paths.get("./src/main/resources/sample.csv"))) {
            StatefulBeanToCsv<DataPojo> beanToCsv = new StatefulBeanToCsvBuilder(writer)
                    .withQuotechar(CSVWriter.NO_QUOTE_CHARACTER)
                    .build();

            beanToCsv.write(sample);
        } catch (IOException | CsvDataTypeMismatchException | CsvRequiredFieldEmptyException e) {
            e.printStackTrace();
        }
    }
}
