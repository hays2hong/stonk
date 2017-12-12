package edu.hhu.stonk.spark.mllib;

public enum ComponentType {

    TRANSFORMER("transformer"),
    ESTIMATOR("estimator");


    private String type;

    ComponentType(String type) {
        this.type = type;
    }
}
