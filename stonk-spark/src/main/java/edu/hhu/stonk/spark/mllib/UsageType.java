package edu.hhu.stonk.spark.mllib;

public enum UsageType {

    CLUSTERING("clustering"),
    CLASSIFICATION("classification");

    private String type;

    UsageType(String type) {
        this.type = type;
    }
}
