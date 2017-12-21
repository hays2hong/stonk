package edu.hhu.stonk.dao.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Hbase Config
 *
 * @author hayes, @create 2017-12-20 16:59
 **/
public class HbaseConfig {

    private static final String CONF_FILE_PATH = "/hbase-conf.conf";

    private static String hbaseRootdir;

    private static String hbaseRetryNum;

    private static String hbaseZkQuorum;

    private static String hbaseZkClientPort;


    static {
        try {
            InputStream in = HbaseConfig.class.getResourceAsStream(CONF_FILE_PATH);
            Properties pro = new Properties();
            pro.load(in);
            hbaseRootdir = pro.getProperty("hbase.rootdir");
            hbaseRetryNum = pro.getProperty("hbase.retries.num", "3");
            hbaseZkQuorum = pro.getProperty("hbase.zk.quorum");
            hbaseZkClientPort = pro.getProperty("hbase.zk.clientport", "2181");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getHbaseRootdir() {
        return hbaseRootdir;
    }

    public static String getHbaseRetryNum() {
        return hbaseRetryNum;
    }

    public static String getHbaseZkQuorum() {
        return hbaseZkQuorum;
    }

    public static String getHbaseZkClientPort() {
        return hbaseZkClientPort;
    }

}
