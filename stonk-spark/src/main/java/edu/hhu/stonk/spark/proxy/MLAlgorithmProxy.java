package edu.hhu.stonk.spark.proxy;

import edu.hhu.stonk.dao.task.SparkTaskAlgorithm;
import edu.hhu.stonk.spark.mllib.MLAlgorithmDesc;
import edu.hhu.stonk.spark.mllib.MLAlgorithmLoader;
import edu.hhu.stonk.spark.mllib.ParameterDesc;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * 算法代理
 *
 * @author hayes, @create 2017-12-12 16:16
 **/
public class MLAlgorithmProxy {

    /**
     * 算法实例
     */
    protected Object algo;

    protected Class algoClazz;

    protected MLAlgorithmDesc desc;

    MLAlgorithmProxy(SparkTaskAlgorithm mlAlgo) throws Exception {
        desc = MLAlgorithmLoader.getMLAlgorithmDesc(mlAlgo.getName());
        algoClazz = Class.forName(desc.getClassName());
        algo = algoClazz.newInstance();

        for (Map.Entry<String, String> param : mlAlgo.getParameters().entrySet()) {
            //获得参数描述
            ParameterDesc paramDesc = desc.getParameterDescs().get(param.getKey());
            String setterMethodName = param.getKey().substring(0, 1).toUpperCase()
                    + param.getKey().substring(1);
            Method method = algoClazz.getMethod("set" + setterMethodName,
                    paramDesc.javaTypeClass());
            method.invoke(algo, paramDesc.valueOf(param.getValue()));
        }
    }
}
