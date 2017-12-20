package edu.hhu.stonk.manager.api;

import edu.hhu.stonk.dao.datafile.DataFile;
import edu.hhu.stonk.dao.datafile.DataFileMapper;
import edu.hhu.stonk.manager.common.ApiResult;
import edu.hhu.stonk.manager.conf.SystemConfig;
import edu.hhu.stonk.utils.HDFSClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Data File
 *
 * @author hayes, @create 2017-12-20 18:55
 **/
@Controller
public class Datafile {

    private static final String DATAFILE_TMP_FOLDER = "datafiletmp";

    private DataFileMapper dataFileMapper;

    private HDFSClient hdfsClient;

    @Autowired
    private SystemConfig systemConfig;

    @RequestMapping(value = "/upload", method = RequestMethod.POST)
    @ResponseBody
    public ApiResult handleFileUpload(@RequestParam("uname") String uname,
                                      @RequestParam("file") MultipartFile file) {
        try {
            String fileName = file.getOriginalFilename();
            String tmpPath = DATAFILE_TMP_FOLDER + "/" + fileName;
            byte[] bytes = file.getBytes();
            BufferedOutputStream stream =
                    new BufferedOutputStream(new FileOutputStream(new File(tmpPath)));
            stream.write(bytes);
            stream.close();
            String hdfsPath = new StringBuilder()
                    .append(systemConfig.getHdfsMaster())
                    .append("/stonk/spark/")
                    .append(uname)
                    .append("/datafile/")
                    .append(fileName)
                    .toString();
            boolean r = hdfsClient.uploadFromLocal(tmpPath, hdfsPath, true);
            if (r) {
                return ApiResult.buildSucess();
            } else {
                return ApiResult.buildFail("上传失败");
            }
        } catch (Exception e) {
            return ApiResult.buildFail(e.getMessage());
        }
    }

    @RequestMapping(value = "/desc", method = RequestMethod.POST)
    @ResponseBody
    public ApiResult desc(@RequestBody DataFile dataFile) {
        try {
            dataFileMapper.put(dataFile);
            return ApiResult.buildSucess();
        } catch (Exception e) {
            return ApiResult.buildFail(e.getMessage());
        }
    }

    @PostConstruct
    public void init() throws Exception {
        hdfsClient = new HDFSClient(systemConfig.getHdfsMaster());
    }

    @PreDestroy
    public void destroy() throws IOException {
        hdfsClient.close();
    }

}
