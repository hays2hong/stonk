package edu.hhu.stonk.spark.datafile;

public enum DataFileType {
    CSV("csv"),
    LIBSVM("libsvm");

    private String type;

    DataFileType(String type) {
        this.type = type;
    }
}
