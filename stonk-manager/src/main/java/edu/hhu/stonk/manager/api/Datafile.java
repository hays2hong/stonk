package edu.hhu.stonk.manager.api;

import edu.hhu.stonk.dao.datafile.DataFile;
import edu.hhu.stonk.dao.datafile.DataFileMapper;
import edu.hhu.stonk.manager.common.ApiResult;
import edu.hhu.stonk.manager.conf.SystemConfig;
import edu.hhu.stonk.utils.HDFSClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;

/**
 * Data File
 *
 * @author hayes, @create 2017-12-20 18:55
 **/
@Controller
@RequestMapping(value = "/datafile")
public class Datafile {

    private DataFileMapper dataFileMapper;

    private HDFSClient hdfsClient;

    @Autowired
    private SystemConfig systemConfig;


    @RequestMapping(value = "/upload", method = RequestMethod.POST)
    @ResponseBody
    public ApiResult handleFileUpload(@RequestParam("uname") String uname,
                                      @RequestParam("file") MultipartFile file,
                                      @RequestParam("dataFileDesc") String dataFileDesc) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            DataFile dataFile = mapper.readValue(dataFileDesc.replaceAll("\\s+", ""), DataFile.class);
            String fileName = file.getOriginalFilename();
            String hdfsPath = new StringBuilder()
                    .append(systemConfig.getHdfsMaster())
                    .append("/stonk/spark/")
                    .append(uname)
                    .append("/datafile/")
                    .append(fileName)
                    .toString();
            boolean r = hdfsClient.uploadFromBytes(file.getBytes(), hdfsPath, true);
            if (r) {
                dataFile.setName(fileName);
                dataFile.setPath(hdfsPath);
                dataFile.setUname(uname);
                dataFileMapper.put(dataFile);
                return ApiResult.buildSucess();
            } else {
                return ApiResult.buildFail("上传失败");
            }
        } catch (Exception e) {
            return ApiResult.buildFail(e.getMessage());
        }
    }

    @RequestMapping(value = "/changedesc", method = RequestMethod.POST)
    @ResponseBody
    public ApiResult changeDesc(@RequestBody DataFile dataFile) {
        try {
            dataFileMapper.put(dataFile);
            return ApiResult.buildSucess();
        } catch (Exception e) {
            return ApiResult.buildFail(e.getMessage());
        }
    }

    @RequestMapping(value = "/{uname}", method = RequestMethod.GET)
    @ResponseBody
    public ApiResult getAll(@PathVariable String uname) {
        try {
            List<DataFile> dataFiles = dataFileMapper.get(uname);
            return ApiResult.buildSucessWithData(dataFiles);
        } catch (Exception e) {
            return ApiResult.buildFail(e.getMessage());
        }
    }

    @PostConstruct
    public void init() throws Exception {
        dataFileMapper = new DataFileMapper();
        hdfsClient = new HDFSClient(systemConfig.getHdfsMaster());
    }

    @PreDestroy
    public void destroy() throws IOException {
        hdfsClient.close();
        dataFileMapper.close();
    }

}
