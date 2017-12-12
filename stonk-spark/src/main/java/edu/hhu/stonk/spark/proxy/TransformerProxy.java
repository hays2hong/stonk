package edu.hhu.stonk.spark.proxy;

import edu.hhu.stonk.spark.task.TaskMLalgorithm;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.lang.reflect.Method;

/**
 * Transformers Proxy
 *
 * @author hayes, @create 2017-12-12 15:20
 **/
public class TransformerProxy extends MLAlgorithmProxy {

    TransformerProxy(TaskMLalgorithm mlAlgo) throws Exception {
        super(mlAlgo);
    }

    public Dataset<Row> transform(Dataset<Row> dataset) throws Exception {
        Method method = algoClazz.getMethod("transform");
        return (Dataset<Row>) method.invoke(algo, dataset);
    }
}
