package edu.hhu.stonk.manager.task;

import edu.hhu.stonk.dao.task.StonkTaskInfo;
import edu.hhu.stonk.manager.conf.SystemConfig;
import edu.hhu.stonk.utils.RandomUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
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
    public void execute(StonkTaskInfo taskInfo) throws IOException {
        fillStonkTaskInfo(taskInfo);

        ProcessBuilder pb = new ProcessBuilder();
        pb.directory(new File("/root/spark-k8s/spark-k8s/bin"));
        pb.command(buildCommand(taskInfo));
        Process p = pb.start();
    }

    private void fillStonkTaskInfo(StonkTaskInfo taskInfo) {
        String taskName = new StringBuilder().append(TASK_PREFIX)
                .append("-").append(taskInfo.getUname())
                .append("-").append(RandomUtil.getRandomString(8))
                .toString();
        taskInfo.setName(taskName);
        taskInfo.setTimeStamp(System.currentTimeMillis());
    }

    public List<String> buildCommand(StonkTaskInfo taskInfo) {
        List<String> command = new ArrayList<>();
        command.add("./spark-submit");
        command.add("--deploy-mode");
        command.add("cluster");
        command.add("--master");
        command.add(systemConfig.getK8sMaster());
        command.add("--class");
        command.add("edu.hhu.stonk.spark.Submiter");
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
        command.add("spark.kubernetes.authenticate.driver.serviceAccountName=" + systemConfig.getK8sSparkServiceAccountName());
        command.add(systemConfig.getTaskJarPath());
        command.add(systemConfig.getHdfsMaster());
        command.add(taskInfo.getUname());
        command.add(taskInfo.getName());

        return command;
    }
}
