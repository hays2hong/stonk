package edu.hhu.stonk.spark.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hhu.stonk.spark.mllib.*;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * MLAlgorithmLoader测试类
 *
 * @author hayes, @create 2017-12-14 19:35
 **/
public class MLAlgorithmLoaderTest {

    @Test
    public void load() throws IOException {
        MLAlgorithmDesc desc = MLAlgorithmLoader.getMLAlgorithmDesc("tokenizer");
        System.out.println(desc.getName());
    }


    @Test
    public void gen() throws IOException {
        Map<String, MLAlgorithmDesc> mlAlgos = new HashMap<>();
        MLAlgorithmDesc tokenizerDesc = new MLAlgorithmDesc();
        tokenizerDesc.setClassName("org.apache.spark.ml.feature.Tokenizer");
        tokenizerDesc.setName("tokenizer");
        tokenizerDesc.setComponentsType(ComponentType.TRANSFORMER);
        tokenizerDesc.setUsageType(UsageType.CLUSTERING);
        tokenizerDesc.setShowName("分词");
        Map<String, ParameterDesc> parameterDescs = new HashMap<>();
        ParameterDesc parameterDesc1 = new ParameterDesc();
        parameterDesc1.setName("inputCol");
        parameterDesc1.setShowName("输入");
        parameterDesc1.setValueType(ParameterValueType.STRING);
        parameterDescs.put("inputCol", parameterDesc1);
        ParameterDesc parameterDesc2 = new ParameterDesc();
        parameterDesc2.setName("outputCol");
        parameterDesc2.setShowName("输入");
        parameterDesc2.setValueType(ParameterValueType.STRING);
        parameterDescs.put("outputCol", parameterDesc2);
        tokenizerDesc.setParameterDescs(parameterDescs);

        mlAlgos.put("tokenizer", tokenizerDesc);

        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(System.out, mlAlgos);

    }
}
