package model;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvBindByPosition;

public class DataPojo implements Comparable<DataPojo> {

    @CsvBindByPosition(position = 0)
    private String key;
    @CsvBindByPosition(position = 1)
    private String data;

    public String getKey() {
        return key;
    }

    public String getData() {
        return data;
    }

    public DataPojo(String key, String data) {
        this.key = key;
        this.data = data;
    }

    public DataPojo(String csvLine) {
        String[] fields = csvLine.split(",");
        this.key=fields[0];
        this.data=fields[1];
    }

    public int compareTo(DataPojo other) {
        return this.key.compareTo(other.key);
    }
}
