package edu.hhu.stonk.manager.task;

import edu.hhu.stonk.dao.task.StonkTaskInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * task manager
 *
 * @author hayes, @create 2017-12-19 13:25
 **/
@Component
public class TaskManager {

    @Autowired
    SparkTaskExecutor sparkTaskExecutor;

    public void execute(StonkTaskInfo taskInfo) throws IOException {
        sparkTaskExecutor.execute(taskInfo);
    }

}
