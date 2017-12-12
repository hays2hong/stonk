package edu.hhu.stonk.spark.mllib;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 从配置文件中读取算法描述
 *
 * @author hayes, @create 2017-12-12 14:43
 **/
public class MLAlgorithmLoader {

    private static Map<String, MLAlgorithmDesc> mlAlgos = new HashMap<>();

    private static String MLALGOS_JSON_FILE_PATH = "../mlalgos.json";

    static {
        ObjectMapper mapper = new ObjectMapper();
        try {
            mlAlgos = mapper.readValue(MLALGOS_JSON_FILE_PATH, new TypeReference<HashMap<String, MLAlgorithmDesc>>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static MLAlgorithmDesc getMLAlgorithmDesc(String name) {
        return mlAlgos.get(name);
    }


}
