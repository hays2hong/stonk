package edu.hhu.stonk.spark;

import edu.hhu.stonk.dao.task.StonkTaskInfo;
import edu.hhu.stonk.dao.task.StonkTaskMapper;
import edu.hhu.stonk.spark.datafile.SparkDataFileConverter;
import edu.hhu.stonk.spark.mllib.ComponentType;
import edu.hhu.stonk.spark.mllib.MLAlgorithmDesc;
import edu.hhu.stonk.spark.mllib.MLAlgorithmLoader;
import edu.hhu.stonk.spark.proxy.EstimatorProxy;
import edu.hhu.stonk.spark.proxy.ModelProxy;
import edu.hhu.stonk.spark.proxy.TransformerProxy;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

    /**
     * 任务信息
     */
    private static StonkTaskInfo taskInfo;

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
                .appName(taskInfo.getName())
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());

        Dataset<Row> dataset = SparkDataFileConverter.extractDataFrame(taskInfo, context);
        String mlAlgoName = taskInfo.getSparkTaskAlgorithm().getName();
        MLAlgorithmDesc mlAlgoDesc = MLAlgorithmLoader.getMLAlgorithmDesc(mlAlgoName);

        if (mlAlgoDesc.getComponentsType() == ComponentType.ESTIMATOR) {
            EstimatorProxy estimatorProxy = new EstimatorProxy(taskInfo.getSparkTaskAlgorithm());
            ModelProxy modelProxy = estimatorProxy.fit(dataset);
        } else if (mlAlgoDesc.getComponentsType() == ComponentType.TRANSFORMER) {
            TransformerProxy transformerProxy = new TransformerProxy(taskInfo.getSparkTaskAlgorithm());
            Dataset<Row> transformedDataset = transformerProxy.transform(dataset);
            transformedDataset.write()
                    .json(hdfsFilePrefix + "/out16");
        }
    }

    private static void loadArgs(String[] args) throws Exception {
        //配置
        hdfsHost = args[0];
        String uname = args[1];
        String taskName = args[2];

        StonkTaskMapper taskMapper = new StonkTaskMapper();
        taskInfo = taskMapper.get(uname, taskName);

        hdfsFilePrefix = new StringBuilder()
                .append(hdfsHost).append("/stonk/spark/")
                .append(taskInfo.getUname()).append("/")
                .append(taskInfo.getName()).append("/")
                .toString();
    }
}
