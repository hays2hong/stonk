package edu.hhu.stonk.dao.task;


import java.io.Serializable;
import java.util.Map;

/**
 * 任务的算法信息
 *
 * @author hayes, @create 2017-12-11 20:05
 **/
public class SparkTaskAlgorithm implements Serializable {

    private static final long serialVersionUID = -2219671422459865122L;
    /**
     * 算法名称，对应AlgorithmDesc中的name
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

