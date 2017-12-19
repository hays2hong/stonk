package edu.hhu.stonk.utils.test;

import edu.hhu.stonk.utils.HDFSClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class HDFSClientTest {
    static HDFSClient client;

    @Before
    public void setUpBeforeClass() throws Exception {
        System.out.println("@Before");
        client = new HDFSClient("hdfs://10.196.83.90:9000");
    }

    @After
    public void tearDownAfterClass() throws Exception {
        System.out.println("@After");
        client.close();
    }

    @Test
    public void testMkdir() {
        System.out.print("mkdirTest:");
        System.out.println(client.mkdir("\\stonk\\spark\\"));

    }

    @Test
    public void testExists() throws IOException {
        System.out.print("exists:");
        System.out.println(client.exists("\\stonk\\spark\\"));

    }

    @Test
    public void testList() {
        System.out.println("listTest:");
        try {
            List<String> paths = client.list("\\");
            for (String path : paths) {
                System.out.println(path);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    @Test
    public void test() {
        client.uploadFromLocal("D://test/ngram/testtask.json", "hdfs://10.196.83.90:9000/stonk/spark/1001/test/testtask.json");
        // client.uploadFromLocal("D://test/ngram/testdata.csv", "hdfs://10.196.83.90:9000/stonk/spark/1001/test/testdata.csv");
    }
}
