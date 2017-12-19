package edu.hhu.stonk.manager.api;

import edu.hhu.stonk.manager.common.ApiResult;
import edu.hhu.stonk.manager.task.StonkTaskInfo;
import edu.hhu.stonk.manager.task.StonkTaskType;
import edu.hhu.stonk.manager.task.TaskManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

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

    @Autowired
    private TaskManager taskManager;

    //TODO：提交任务后，任务的管理
    @RequestMapping(value = "/submit", method = RequestMethod.PUT)
    @ResponseBody
    public ApiResult<String> submit(@RequestBody StonkTaskInfo taskInfo) {
        if (taskInfo.getTaskType() == StonkTaskType.SPARK_TASK_TYPE) {
            try {
                String taskName = taskManager.execute(taskInfo);
                return ApiResult.buildSucessWithData("sucess", taskName);
            } catch (IOException e) {
                e.printStackTrace();
                return ApiResult.buildFail(e.getMessage());
            }
        }

        return ApiResult.buildFail("不支持的任务类型");
    }

    //TODO：从数据库取该用户的所有任务
    @RequestMapping(value = "/{uname}/tasks", method = RequestMethod.GET)
    @ResponseBody
    public ApiResult<List<StonkTaskInfo>> tasks(@PathVariable String uname) {
        return null;
    }

    //TODO：从数据库取该用户的所有任务
    @RequestMapping(value = "/{uname}/tasks/{taskName}", method = RequestMethod.GET)
    @ResponseBody
    public ApiResult<List<StonkTaskInfo>> task(@PathVariable String uname, @PathVariable String taskName) {
        return null;
    }


    public TaskManager getTaskManager() {
        return taskManager;
    }

    public void setTaskManager(TaskManager taskManager) {
        this.taskManager = taskManager;
    }
}
