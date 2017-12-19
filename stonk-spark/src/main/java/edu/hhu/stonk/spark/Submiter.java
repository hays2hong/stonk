package edu.hhu.stonk.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hhu.stonk.spark.mllib.ComponentType;
import edu.hhu.stonk.spark.mllib.MLAlgorithmDesc;
import edu.hhu.stonk.spark.mllib.MLAlgorithmLoader;
import edu.hhu.stonk.spark.proxy.EstimatorProxy;
import edu.hhu.stonk.spark.proxy.ModelProxy;
import edu.hhu.stonk.spark.proxy.TransformerProxy;
import edu.hhu.stonk.spark.task.SparkTaskInfo;
import edu.hhu.stonk.utils.HDFSClient;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * 提交Spark任务
 *
 * @author hayes, @create 2017-12-11 16:46
 **/
public class Submiter {

    /**
     * spark master
     */
    private static String masterUrl;

    /**
     * hdfs host地址
     */
    private static String hdfsHost;

    private static HDFSClient hdfsClient;

    /**
     * 任务信息
     */
    private static SparkTaskInfo sparkTaskInfo;

    /**
     * 该用户该任务的hdfs文件前缀
     */
    private static String hdfsFilePrefix;

    public static void main(String[] args) throws Exception {
        //加载配置
        loadArgs(args);

        //JavaSparkContext初始化
        SparkSession sparkSession = SparkSession
                .builder()
                .appName(sparkTaskInfo.getName())
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());

        Dataset<Row> dataset = sparkTaskInfo.getDataFile().convertToDataFrame(context);
        String mlAlgoName = sparkTaskInfo.getMlAlgorithm().getName();
        MLAlgorithmDesc mlAlgoDesc = MLAlgorithmLoader.getMLAlgorithmDesc(mlAlgoName);

        if (mlAlgoDesc.getComponentsType() == ComponentType.ESTIMATOR) {
            EstimatorProxy estimatorProxy = new EstimatorProxy(sparkTaskInfo.getMlAlgorithm());
            ModelProxy modelProxy = estimatorProxy.fit(dataset);
        } else if (mlAlgoDesc.getComponentsType() == ComponentType.TRANSFORMER) {
            TransformerProxy transformerProxy = new TransformerProxy(sparkTaskInfo.getMlAlgorithm());
            Dataset<Row> transformedDataset = transformerProxy.transform(dataset);
            transformedDataset.write()
                    .json(hdfsFilePrefix + "/out16");
        }
    }

    private static void loadArgs(String[] args) throws Exception {
        //配置
        hdfsHost = args[0];
        hdfsClient = new HDFSClient(hdfsHost);
        sparkTaskInfo = loadTask(args[1]);
        hdfsFilePrefix = new StringBuilder()
                .append(hdfsHost).append("/stonk/spark/")
                .append(sparkTaskInfo.getUid()).append("/")
                .append(sparkTaskInfo.getName()).append("/")
                .toString();
    }

    public static SparkTaskInfo loadTask(String taskJsonFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        DataInputStream in = hdfsClient.getFileInputStream(taskJsonFile);
        return mapper.readValue(in, SparkTaskInfo.class);
    }
}
