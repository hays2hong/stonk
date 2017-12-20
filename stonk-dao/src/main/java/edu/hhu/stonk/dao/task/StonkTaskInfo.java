package edu.hhu.stonk.dao.task;

/**
 * Stonk task info
 *
 * @author hayes, @create 2017-12-19 13:40
 **/
public class StonkTaskInfo {

    private StonkTaskType taskType;

    private String uname;

    private String dataFile;

    private int sparkExecutorNum = 1;

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

    public String getDataFile() {
        return dataFile;
    }

    public void setDataFile(String dataFile) {
        this.dataFile = dataFile;
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
}
