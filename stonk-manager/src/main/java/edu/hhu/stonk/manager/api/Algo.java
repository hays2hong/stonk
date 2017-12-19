package edu.hhu.stonk.manager.api;

/**
 * API接口
 *
 * @author hayes, @create 2017-12-18 20:28
 **/

import edu.hhu.stonk.manager.common.ApiResult;
import edu.hhu.stonk.spark.mllib.MLAlgorithmDesc;
import edu.hhu.stonk.spark.mllib.MLAlgorithmLoader;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.Map;

@Controller
@RequestMapping("/algo")
public class Algo {

    @RequestMapping("/spark/all")
    @ResponseBody
    public ApiResult<Map<String, MLAlgorithmDesc>> getSparkAll() {
        try {
            Map<String, MLAlgorithmDesc> algos = MLAlgorithmLoader.getAll();
            return ApiResult.buildSucessWithData("sucess", algos);
        } catch (IOException e) {
            return ApiResult.buildFail(e.getMessage());
        }
    }

    @RequestMapping("/spark/{algoName}")
    @ResponseBody
    public ApiResult<MLAlgorithmDesc> getSparkAlgo(@PathVariable String algoName) {
        try {
            return ApiResult.buildSucessWithData("sucess",
                    MLAlgorithmLoader.getMLAlgorithmDesc(algoName));
        } catch (IOException e) {
            return ApiResult.buildFail(e.getMessage());
        }
    }
}


