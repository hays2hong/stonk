package edu.hhu.stonk.dao.task;

import java.io.Serializable;

/**
 * Stonk task info
 *
 * @author hayes, @create 2017-12-19 13:40
 **/
public class StonkTaskInfo implements Serializable {

    private static final long serialVersionUID = -7755459521939958459L;

    private StonkTaskType taskType;

    private String name;

    private String uname;

    private String dataFileName;

    private int sparkExecutorNum = 1;

    private long timeStamp;

    private SparkTaskAlgorithm sparkTaskAlgorithm;


    public StonkTaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(StonkTaskType taskType) {
        this.taskType = taskType;
    }

    public String getUname() {
        return uname;
    }

    public void setUname(String uname) {
        this.uname = uname;
    }

    public String getDataFileName() {
        return dataFileName;
    }

    public void setDataFileName(String dataFileName) {
        this.dataFileName = dataFileName;
    }

    public SparkTaskAlgorithm getSparkTaskAlgorithm() {
        return sparkTaskAlgorithm;
    }

    public void setSparkTaskAlgorithm(SparkTaskAlgorithm sparkTaskAlgorithm) {
        this.sparkTaskAlgorithm = sparkTaskAlgorithm;
    }

    public int getSparkExecutorNum() {
        return sparkExecutorNum;
    }

    public void setSparkExecutorNum(int sparkExecutorNum) {
        this.sparkExecutorNum = sparkExecutorNum;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
