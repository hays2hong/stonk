package edu.hhu.stonk.spark.proxy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 训练后的模型代理
 *
 * @author hayes, @create 2017-12-12 15:21
 **/
public class ModelProxy {

    /**
     * 模型实例
     */
    protected Object model;

    protected Class modelClazz;

    public ModelProxy(Object model, Class modelClazz) {
        this.model = model;
        this.modelClazz = modelClazz;
    }

    public static ModelProxy load(String path) {
        return null;
    }

    public Dataset<Row> tranform(Dataset<Row> dataset) {
        return null;
    }

    public void save(String path) {

    }
}
