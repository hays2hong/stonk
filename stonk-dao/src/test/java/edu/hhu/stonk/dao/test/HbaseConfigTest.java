package edu.hhu.stonk.dao.test;

import edu.hhu.stonk.dao.config.HbaseConfig;
import org.junit.Test;

/**
 * test
 *
 * @author hayes, @create 2017-12-20 22:17
 **/
public class HbaseConfigTest {

    @Test
    public void test() {
        System.out.println(HbaseConfig.getHbaseRootdir());
    }
}
