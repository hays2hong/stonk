package edu.hhu.stonk.utils.test;

import edu.hhu.stonk.utils.HDFSClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
    public void testList() {
        System.out.println("testList");
        try {
            client.list("\\");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
