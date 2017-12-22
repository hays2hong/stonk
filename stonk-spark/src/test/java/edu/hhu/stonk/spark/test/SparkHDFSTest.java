package edu.hhu.stonk.spark.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/**
 * test
 *
 * @author hayes, @create 2017-12-21 18:31
 **/
public class SparkHDFSTest {
    @Test
    public void test() {
        String hdfsPath = "hdfs://10.196.83.90:9000/stonk/spark/aa/spark-task--aa-b5x59zpv/out3";

        SparkConf conf = new SparkConf().setAppName("111").setMaster("local[3]");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> rdd = context.textFile(hdfsPath);
        rdd.foreach((str) -> System.out.println(str));
    }
}
