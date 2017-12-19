package edu.hhu.stonk.manager.task;

import edu.hhu.stonk.manager.conf.SystemConfig;
import edu.hhu.stonk.spark.datafile.DataFile;
import edu.hhu.stonk.spark.task.SparkTaskInfo;
import edu.hhu.stonk.utils.RandomUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * spark task executor
 *
 * @author hayes, @create 2017-12-19 14:40
 **/
@Component
public class SparkTaskExecutor {

    private static final String TASK_PREFIX = "spark-task-";

    @Autowired
    SystemConfig systemConfig;


    /**
     * 执行Spark任务  TODD: Process的管理
     *
     * @param taskInfo
     * @return
     * @throws IOException
     */
    public String execute(StonkTaskInfo taskInfo) throws IOException {
        SparkTaskInfo sparkTaskInfo = buildSparkTaskInfo(taskInfo);

        ProcessBuilder pb = new ProcessBuilder();
        pb.command(buildCommand(sparkTaskInfo));
        Process p = pb.start();

        return sparkTaskInfo.getName();
    }

    private SparkTaskInfo buildSparkTaskInfo(StonkTaskInfo taskInfo) {
        String taskName = new StringBuilder().append(TASK_PREFIX)
                .append("-").append(taskInfo.getUname())
                .append("-").append(RandomUtil.getRandomString(8))
                .toString();

        SparkTaskInfo sparkTaskInfo = new SparkTaskInfo();
        sparkTaskInfo.setName(taskName);
        sparkTaskInfo.setMlAlgorithm(taskInfo.getMlAlgorithm());
        sparkTaskInfo.setUname(taskInfo.getUname());
        sparkTaskInfo.setSparkExecutorNum(taskInfo.getSparkExecutorNum());
        sparkTaskInfo.setDataFile(getDataFileDesc(taskInfo.getUname(), taskInfo.getDataFile()));

        return sparkTaskInfo;
    }

    private DataFile getDataFileDesc(String uname, String dataFile) {

        return null;
    }

    public List<String> buildCommand(SparkTaskInfo taskInfo) {
        List<String> command = new ArrayList<>();
        command.add("bin/spark-submit");
        command.add("--deploy-mode");
        command.add("cluster");
        command.add("--master");
        command.add(systemConfig.getK8sMaster());
        command.add("--kubernetes-namespace");
        command.add(systemConfig.getK8sSparkNamespace());
        command.add("--conf");
        command.add("spark.executor.instances=" + taskInfo.getSparkExecutorNum());
        command.add("--conf");
        command.add("spark.app.name=" + taskInfo.getName());
        command.add("--conf");
        command.add("spark.kubernetes.driver.docker.image=" + systemConfig.getK8sSparkDriverDockerImage());
        command.add("--conf");
        command.add("spark.kubernetes.executor.docker.image=" + systemConfig.getK8sSparkExecutorDockerImage());
        command.add("--conf");
        command.add(systemConfig.getTaskJarPath());
        command.add(systemConfig.getHdfsMaster());
        //TODO：task file path
        command.add(systemConfig.getHdfsMaster());

        return command;
    }
}
