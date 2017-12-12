package edu.hhu.stonk.spark.task;


import java.util.Map;

/**
 * 任务的算法信息
 *
 * @author hayes, @create 2017-12-11 20:05
 **/
public class TaskMLalgorithm {

    /**
     * 算法名称，对应MLAlgorithmDesc中的name
     */
    private String name;

    /**
     * 参数名称及值
     */
    private Map<String, String> parameters;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }
}

