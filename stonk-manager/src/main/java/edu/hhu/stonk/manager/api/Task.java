package edu.hhu.stonk.manager.api;

import edu.hhu.stonk.dao.task.StonkTaskInfo;
import edu.hhu.stonk.dao.task.StonkTaskMapper;
import edu.hhu.stonk.dao.task.StonkTaskType;
import edu.hhu.stonk.manager.common.ApiResult;
import edu.hhu.stonk.manager.task.TaskManager;
import edu.hhu.stonk.utils.RandomUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;

/**
 * job
 *
 * @author hayes, @create 2017-12-18 21:25
 **/
@Controller
@RequestMapping("/task")
public class Task {

    private static final String SPARK_TASK_PREFIX = "spark-task";

    @Autowired
    private TaskManager taskManager;

    private StonkTaskMapper taskMapper;


    //TODO：提交任务后，任务的管理
    @RequestMapping(value = "/submit", method = RequestMethod.POST)
    @ResponseBody
    public ApiResult<String> submit(@RequestBody StonkTaskInfo taskInfo) {
        if (taskInfo.getTaskType() == StonkTaskType.SPARK_TASK_TYPE) {
            try {
                fillSparkTaskInfo(taskInfo);
                taskMapper.put(taskInfo);
                taskManager.execute(taskInfo);
                return ApiResult.buildSucessWithData(taskInfo);
            } catch (Exception e) {
                return ApiResult.buildFail(e.getMessage());
            }
        }

        return ApiResult.buildFail("不支持的任务类型");
    }

    private void fillSparkTaskInfo(StonkTaskInfo taskInfo) {
        String taskName = new StringBuilder().append(SPARK_TASK_PREFIX)
                .append("-").append(taskInfo.getUname())
                .append("-").append(RandomUtil.getRandomString(8))
                .toString();
        taskInfo.setName(taskName);
        taskInfo.setTimeStamp(System.currentTimeMillis());
    }

    @RequestMapping(value = "/{uname}", method = RequestMethod.GET)
    @ResponseBody
    public ApiResult<List<StonkTaskInfo>> tasks(@PathVariable String uname) {
        try {
            return ApiResult.buildSucessWithData(taskMapper.get(uname));
        } catch (Exception e) {
            return ApiResult.buildFail(e.getMessage());
        }
    }

    @RequestMapping(value = "/{uname}/{taskName}", method = RequestMethod.GET)
    @ResponseBody
    public ApiResult<List<StonkTaskInfo>> task(@PathVariable String uname, @PathVariable String taskName) {
        try {
            return ApiResult.buildSucessWithData(taskMapper.get(uname, taskName));
        } catch (Exception e) {
            return ApiResult.buildFail(e.getMessage());
        }
    }

    @PostConstruct
    public void init() throws IOException {
        taskMapper = new StonkTaskMapper();
    }

    @PreDestroy
    public void destroy() throws IOException {
        taskMapper.close();
    }


    public TaskManager getTaskManager() {
        return taskManager;
    }

    public void setTaskManager(TaskManager taskManager) {
        this.taskManager = taskManager;
    }
}
