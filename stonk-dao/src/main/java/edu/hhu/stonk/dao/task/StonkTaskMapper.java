package edu.hhu.stonk.dao.task;

import edu.hhu.stonk.dao.config.HbaseConfig;
import edu.hhu.stonk.utils.HbaseClient;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * stonk task info mapper
 *
 * @author hayes, @create 2017-12-20 16:50
 **/
public class StonkTaskMapper implements Closeable {
    private static final String TABLE_NAME = "stonk-task";

    private static final String FAMILY_NAME = "info";

    private static final String JSON_QUALIFIER_NAME = "json";


    private HbaseClient hbaseClient;

    private ObjectMapper JSON;

    public StonkTaskMapper() throws IOException {
        String hbaseRootdir = HbaseConfig.getHbaseRootdir();
        String hbaseRetryNum = HbaseConfig.getHbaseRetryNum();
        String hbaseZkQuorum = HbaseConfig.getHbaseZkQuorum();
        String hbaseZkClientPort = HbaseConfig.getHbaseZkClientPort();

        hbaseClient = new HbaseClient(hbaseZkQuorum, hbaseZkClientPort, hbaseRootdir, hbaseRetryNum);
        JSON = new ObjectMapper();
    }

    /**
     * 添加
     *
     * @param taskInfo
     * @throws Exception
     */
    public void put(StonkTaskInfo taskInfo) throws Exception {
        String rowKey = buildRowKey(taskInfo.getUname(), taskInfo.getName());

        hbaseClient.putData(TABLE_NAME, rowKey, FAMILY_NAME, JSON_QUALIFIER_NAME, JSON.writeValueAsString(taskInfo));
    }

    /**
     * 查询
     *
     * @param uname
     * @param taskName
     * @return
     * @throws Exception
     */
    public StonkTaskInfo get(String uname, String taskName) throws Exception {
        String rowKey = buildRowKey(uname, taskName);

        String dataFileJson = hbaseClient.getValue(TABLE_NAME, rowKey, FAMILY_NAME, JSON_QUALIFIER_NAME);
        return JSON.readValue(dataFileJson, StonkTaskInfo.class);
    }

    /**
     * 查询用户的所有任务
     *
     * @param uname
     * @return
     * @throws Exception
     */
    public List<StonkTaskInfo> get(String uname) throws Exception {
        List<String> taskInfoJsons = hbaseClient.getValueByRowPrefix(TABLE_NAME, uname, FAMILY_NAME, JSON_QUALIFIER_NAME);
        List<StonkTaskInfo> taskInfos = new ArrayList<>();
        for (String json : taskInfoJsons) {
            taskInfos.add(JSON.readValue(json, StonkTaskInfo.class));
        }
        return taskInfos;
    }


    /**
     * 生成rowkey  {uname}_{taskName}
     *
     * @param uname
     * @param taskName
     * @return
     */
    private String buildRowKey(String uname, String taskName) {
        return uname + "_" + taskName;
    }


    @Override
    public void close() throws IOException {
        hbaseClient.close();
    }
}
