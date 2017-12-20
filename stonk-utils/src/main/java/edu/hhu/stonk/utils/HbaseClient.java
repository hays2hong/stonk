package edu.hhu.stonk.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HBASE Client
 *
 * @author hayes, @create 2017-12-19 16:18
 **/
public class HbaseClient implements Closeable {

    Configuration hbaseConf = null;

    Admin admin;

    Connection connection;

    private Map<String, HTable> htables = new ConcurrentHashMap<>();

    //TODO: 连接池和用户相关设置
    public HbaseClient(String zkMaster, String zkClientPort, String rootDir, String retryNum) throws IOException {
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.property.clientPort", zkClientPort);
        hbaseConf.set("hbase.zookeeper.quorum", zkMaster);
        hbaseConf.set("hbase.rootdir", rootDir);
        hbaseConf.set("hbase.client.retries.number", retryNum);
        connection = ConnectionFactory.createConnection(hbaseConf);
        admin = connection.getAdmin();
    }


    /**
     * 创建表
     *
     * @param tableNameStr
     * @param familys
     * @throws Exception
     */
    public void createTable(String tableNameStr, String[] familys) throws Exception {
        TableName tableName = TableName.valueOf(tableNameStr);
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);

        for (String family : familys) {
            tableDesc.addFamily(new HColumnDescriptor(family));
        }

        if (admin.tableExists(tableName)) {
            throw new Exception("该表已经存在");
        } else {
            admin.createTable(tableDesc);
        }
    }

    public HTableDescriptor getTableDesc(String tableName) throws Exception {
        return admin.getTableDescriptor(TableName.valueOf(tableName));
    }

    private synchronized HTable getHtable(String tableName) throws IOException {
        HTable htable = htables.get(tableName);
        if (htable == null) {
            htable = (HTable) connection.getTable(TableName.valueOf(tableName));
            htables.put(tableName, htable);
        }

        return htable;
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param qualifierName
     * @param value
     * @throws Exception
     */
    public void putData(String tableName, String rowKey, String familyName, String qualifierName, String value)
            throws Exception {
        HTable htable = getHtable(tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName), Bytes.toBytes(value));
        htable.put(put);
    }

    /**
     * 根据rowkey 查询
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws Exception
     */
    public Result getResult(String tableName, String rowKey) throws Exception {
        HTable htable = getHtable(tableName);

        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = htable.get(get);
        return result;
    }

    /**
     * 查询指定的某列
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param qualifierName
     * @return
     * @throws Exception
     */
    public String getValue(String tableName, String rowKey, String familyName, String qualifierName) throws Exception {
        HTable htable = getHtable(tableName);

        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = htable.get(get);
        Cell cell = result.getColumnLatestCell(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName));
        if (cell == null) {
            return null;
        }
        return Bytes.toString(CellUtil.cloneValue(cell));
    }

    /**
     * 根据Row的前缀获得value
     *
     * @param tableName
     * @param rowPrefix
     * @param familyName
     * @param qualifierName
     * @return
     * @throws Exception
     */
    public List<String> getValueByRowPrefix(String tableName, String rowPrefix, String familyName, String qualifierName) throws Exception {
        HTable htable = getHtable(tableName);
        List<String> values = new ArrayList<>();

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(Bytes.toBytes(rowPrefix)));
        htable.getScanner(scan).forEach((result) -> {
            Cell cell = result.getColumnLatestCell(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName));
            if (cell != null) {
                values.add(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        });
        return values;
    }


    /**
     * 删除指定某列
     *
     * @param tableName
     * @param rowKey
     * @param falilyName
     * @param qualifierName
     * @throws Exception
     */
    public void deleteColumn(String tableName, String rowKey, String falilyName, String qualifierName) throws Exception {
        HTable htable = getHtable(tableName);

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(falilyName), Bytes.toBytes(qualifierName));
        htable.delete(delete);
    }

    /**
     * 删除指定的某个rowkey
     *
     * @param tableName
     * @param rowKey
     * @throws Exception
     */
    public void deleteColumn(String tableName, String rowKey) throws Exception {
        HTable htable = getHtable(tableName);

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        htable.delete(delete);
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws Exception
     */
    public void dropTable(String tableName) throws Exception {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    @Override
    public void close() {
        for (HTable htable : htables.values()) {
            try {
                htable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
