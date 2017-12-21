package edu.hhu.stonk.utils.test;

import edu.hhu.stonk.utils.HbaseClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * HbaseClientTest
 *
 * @author hayes, @create 2017-12-20 13:46
 **/
public class HbaseClientTest {

    private static final String ZOOKEEPER_QUORUM = "10.196.83.90,10.196.83.91,10.196.83.92";
    private static final String ZOOKEEPER_CLIENTPORT = "2181";
    private static final String HBASE_ROOTDIR = "hdfs://10.196.83.90:9000/hbase";
    private static final String RETRIES_NUMBER = "3";

    HbaseClient client;

    @Before
    public void before() throws IOException {
        client = new HbaseClient(ZOOKEEPER_QUORUM, ZOOKEEPER_CLIENTPORT, HBASE_ROOTDIR, RETRIES_NUMBER);
    }

    @After
    public void after() {
        client.close();
    }

    @Test
    public void testCreateTable() throws Exception {
        //client.createTable("stonk-user", new String[]{"info"});
        client.createTable("stonk-datafile", new String[]{"info"});
        //client.createTable("stonk-task", new String[]{"info"});
    }

    @Test
    public void testPutData() throws Exception {
        client.putData("stonk-test", "stonk-2", "c1", "k1", "v2");
    }

    @Test
    public void testGetValue() throws Exception {
        String value = client.getValue("stonk-test", "stonk-1", "c1", "k1");
        System.out.println(value);
    }

    @Test
    public void testGetValueByRowPrefix() throws Exception {
        List<String> values = client.getValueByRowPrefix("stonk-test", "stonk", "c1", "k1");
        values.forEach((str) -> System.out.println(str));
    }

}
