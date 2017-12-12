package edu.hhu.stonk.spark.task;

import edu.hhu.stonk.spark.datafile.DataFile;

/**
 * 任务
 *
 * @author hayes, @create 2017-12-11 19:38
 **/
public class Task {

    /**
     * 数据集
     */
    private DataFile dataFile;

    /**
     * 算法信息
     */
    private TaskMLalgorithm mlAlgorithm;

    public static void main(String[] args) {

    }

    public DataFile getDataFile() {
        return dataFile;
    }

    public void setDataFile(DataFile dataFile) {
        this.dataFile = dataFile;
    }

    public TaskMLalgorithm getMlAlgorithm() {
        return mlAlgorithm;
    }

    public void setMlAlgorithm(TaskMLalgorithm mlAlgorithm) {
        this.mlAlgorithm = mlAlgorithm;
    }
}
