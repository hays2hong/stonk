package edu.hhu.stonk.dao.datafile;

import edu.hhu.stonk.dao.config.HbaseConfig;
import edu.hhu.stonk.utils.HbaseClient;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * DataFile Mapper
 *
 * @author hayes, @create 2017-12-20 16:08
 **/
public class DataFileMapper implements Closeable {

    private static final String TABLE_NAME = "stonk-datafile";

    private static final String FAMILY_NAME = "info";

    private static final String JSON_QUALIFIER_NAME = "json";


    private HbaseClient hbaseClient;

    private ObjectMapper JSON;

    public DataFileMapper() throws IOException {
        String hbaseRootdir = HbaseConfig.getHbaseRootdir();
        String hbaseRetryNum = HbaseConfig.getHbaseRetryNum();
        String hbaseZkQuorum = HbaseConfig.getHbaseZkQuorum();
        String hbaseZkClientPort = HbaseConfig.getHbaseZkClientPort();

        hbaseClient = new HbaseClient(hbaseZkQuorum, hbaseZkClientPort, hbaseRootdir, hbaseRetryNum);
        JSON = new ObjectMapper();
    }

    /**
     * 添加数据
     *
     * @param dataFile
     * @throws Exception
     */
    public void put(DataFile dataFile) throws Exception {
        String rowKey = buildRowKey(dataFile.getUname(), dataFile.getName());

        hbaseClient.putData(TABLE_NAME, rowKey, FAMILY_NAME, JSON_QUALIFIER_NAME, JSON.writeValueAsString(dataFile));
    }

    /**
     * 查询
     *
     * @param uname
     * @param dataFileName
     * @return
     * @throws Exception
     */
    public DataFile get(String uname, String dataFileName) throws Exception {
        String rowKey = buildRowKey(uname, dataFileName);

        String dataFileJson = hbaseClient.getValue(TABLE_NAME, rowKey, FAMILY_NAME, JSON_QUALIFIER_NAME);
        return JSON.readValue(dataFileJson, DataFile.class);
    }

    /**
     * 查询用户的所有数据文件
     *
     * @param uname
     * @return
     * @throws Exception
     */
    public List<DataFile> get(String uname) throws Exception {
        List<String> dataFileJsons = hbaseClient.getValueByRowPrefix(TABLE_NAME, uname, FAMILY_NAME, JSON_QUALIFIER_NAME);
        List<DataFile> dataFiles = new ArrayList<>();
        for (String json : dataFileJsons) {
            dataFiles.add(JSON.readValue(json, DataFile.class));
        }
        return dataFiles;
    }


    /**
     * 生成rowkey  {uname}-{dataFile.name}
     *
     * @param uname
     * @param dataFileName
     * @return
     */
    private String buildRowKey(String uname, String dataFileName) {
        return uname + "_" + dataFileName;
    }

    @Override
    public void close() throws IOException {
        hbaseClient.close();
    }
}
