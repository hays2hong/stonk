package edu.hhu.stonk.spark.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hhu.stonk.dao.datafile.DataFile;
import edu.hhu.stonk.dao.datafile.DataFileType;
import edu.hhu.stonk.dao.datafile.FieldInfo;
import edu.hhu.stonk.dao.task.SparkTaskAlgorithm;
import edu.hhu.stonk.spark.task.SparkTaskInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SparkTaskInfo 测试类
 *
 * @author hayes, @create 2017-12-14 19:34
 **/
public class TaskTest {
    public static void main(String[] args) throws IOException {
        SparkTaskInfo sparkTaskInfo = new SparkTaskInfo();
        sparkTaskInfo.setName("test");
        sparkTaskInfo.setUname("1001");
        SparkTaskAlgorithm algo = new SparkTaskAlgorithm();
        algo.setName("tokenizer");
        Map<String, String> params = new HashMap<>();
        params.put("inputCol", "sentence");
        params.put("outputCol", "words");
        algo.setParameters(params);
        sparkTaskInfo.setAlgorithm(algo);
        DataFile data = new DataFile();
        data.setDataFileType(DataFileType.CSV);
        data.setHeader(false);
        data.setDelim(",");
        data.setPath("hdfs://10.196.83.90:9000/stonk/spark/1001/test/testfile.csv");
        data.setName("testfile");
        FieldInfo info = new FieldInfo();
        info.setDataType("string");
        info.setNullable(false);
        info.setIndex(0);
        info.setName("sentence");
        List<FieldInfo> list = new ArrayList<>();
        list.add(info);
        data.setFieldInfos(list);
        sparkTaskInfo.setDataFile(data);

        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(System.out, sparkTaskInfo);
    }
}
