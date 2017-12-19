package edu.hhu.stonk.manager.conf;


import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * config
 *
 * @author hayes, @create 2017-12-19 13:00
 **/
@Component
public class SystemConfig {

    private static final String CONF_FILE_PATH = "/conf.conf";

    private String k8sMaster;

    private String k8sSparkNamespace;

    private String k8sSparkServiceAccountName;

    private String k8sSparkDriverDockerImage;

    private String k8sSparkExecutorDockerImage;

    private String hdfsMaster;

    private String taskJarPath;

    private String hbaseMaster;

    @PostConstruct
    public void load() throws IOException {
        InputStream in = this.getClass().getResourceAsStream(CONF_FILE_PATH);
        Properties pro = new Properties();
        pro.load(in);
        k8sMaster = pro.getProperty("k8s.master", "localhost:4388");
        k8sSparkNamespace = pro.getProperty("k8s.spark.namespace", "default");
        k8sSparkServiceAccountName = pro.getProperty("k8s.spark.serviceAccountName", "default");
        k8sSparkDriverDockerImage = pro.getProperty("k8s.spark.driver.docker.image");
        k8sSparkExecutorDockerImage = pro.getProperty("k8s.spark.executor.docker.image");
        hdfsMaster = pro.getProperty("hdfs.master");
        taskJarPath = pro.getProperty("task.jar.path");
        hbaseMaster = pro.getProperty("hbase.master");
    }

    public String getK8sMaster() {
        return k8sMaster;
    }

    public void setK8sMaster(String k8sMaster) {
        this.k8sMaster = k8sMaster;
    }

    public String getK8sSparkNamespace() {
        return k8sSparkNamespace;
    }

    public void setK8sSparkNamespace(String k8sSparkNamespace) {
        this.k8sSparkNamespace = k8sSparkNamespace;
    }

    public String getK8sSparkServiceAccountName() {
        return k8sSparkServiceAccountName;
    }

    public void setK8sSparkServiceAccountName(String k8sSparkServiceAccountName) {
        this.k8sSparkServiceAccountName = k8sSparkServiceAccountName;
    }

    public String getK8sSparkDriverDockerImage() {
        return k8sSparkDriverDockerImage;
    }

    public void setK8sSparkDriverDockerImage(String k8sSparkDriverDockerImage) {
        this.k8sSparkDriverDockerImage = k8sSparkDriverDockerImage;
    }

    public String getK8sSparkExecutorDockerImage() {
        return k8sSparkExecutorDockerImage;
    }

    public void setK8sSparkExecutorDockerImage(String k8sSparkExecutorDockerImage) {
        this.k8sSparkExecutorDockerImage = k8sSparkExecutorDockerImage;
    }

    public String getHdfsMaster() {
        return hdfsMaster;
    }

    public void setHdfsMaster(String hdfsMaster) {
        this.hdfsMaster = hdfsMaster;
    }

    public String getTaskJarPath() {
        return taskJarPath;
    }

    public void setTaskJarPath(String taskJarPath) {
        this.taskJarPath = taskJarPath;
    }

    public String getHbaseMaster() {
        return hbaseMaster;
    }

    public void setHbaseMaster(String hbaseMaster) {
        this.hbaseMaster = hbaseMaster;
    }
}
