package com.ucar.streamsuite.config.web;

import com.alibaba.fastjson.JSONObject;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.CommonStatusEnum;
import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import com.ucar.streamsuite.common.constant.EngineVersionTypeEnum;
import com.ucar.streamsuite.common.constant.StreamContant;

import com.ucar.streamsuite.common.dto.OperResultDTO;
import com.ucar.streamsuite.common.dto.PageQueryDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;

import com.ucar.streamsuite.common.util.HdfsClientProxy;
import com.ucar.streamsuite.config.dto.EngineVersionDTO;
import com.ucar.streamsuite.config.po.EngineVersionPO;
import com.ucar.streamsuite.config.service.EngineVersionService;

import com.ucar.streamsuite.task.service.FileService;

import org.apache.commons.lang.StringUtils;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Description: 引擎版本管理控制器
 * Created on 2018/2/1 上午9:53
 *
 */
@Controller
@RequestMapping("/engineVersion")
public class EngineVersionController {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngineVersionController.class);

    @Autowired
    private EngineVersionService engineVersionService;
    @Autowired
    private FileService fileServiceImpl;

    @ResponseBody
    @RequestMapping(value = "/list", method = RequestMethod.POST)
    public PageResultDTO list(HttpServletRequest request, HttpServletResponse response) {

        Integer pageNum = Integer.valueOf(request.getParameter("pageNum"));
        Integer pageSize = Integer.valueOf(request.getParameter("pageSize"));

        PageQueryDTO pageQueryDTO = new PageQueryDTO(pageNum,pageSize);
        return  engineVersionService.pageQuery(pageQueryDTO);
    }

    @ResponseBody
    @RequestMapping(value = "/queryById", method = RequestMethod.POST)
    public EngineVersionDTO queryById(HttpServletRequest request, HttpServletResponse response) {

        Integer id = Integer.valueOf(request.getParameter("id"));

        EngineVersionPO engineVersionPO = engineVersionService.getEngineVersionById(id);

        EngineVersionDTO engineVersionDTO = new EngineVersionDTO();
        engineVersionDTO.setId(engineVersionPO.getId());
        engineVersionDTO.setVersionName(engineVersionPO.getVersionName());
        engineVersionDTO.setVersionType(engineVersionPO.getVersionType());
        engineVersionDTO.setVersionRemark(engineVersionPO.getVersionRemark());
        engineVersionDTO.setVersionUrl(engineVersionPO.getVersionUrl());
        engineVersionDTO.setCreateTime(engineVersionPO.getCreateTime());
        engineVersionDTO.setModifyTime(engineVersionPO.getModifyTime());

        return engineVersionDTO;
    }


    @ResponseBody
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public OperResultDTO save(HttpServletRequest request, HttpServletResponse response) {

        String id = request.getParameter("id");
        String versionName = request.getParameter("versionName");
        String versionType = request.getParameter("versionType");
        String versionRemark = request.getParameter("versionRemark");
        String versionUrl = request.getParameter("versionUrl");

        EngineVersionPO engineVersionPO = new EngineVersionPO();
        engineVersionPO.setVersionName(versionName);
        engineVersionPO.setVersionType(versionType);
        engineVersionPO.setVersionRemark(versionRemark);
        engineVersionPO.setVersionUrl(versionUrl);

        OperResultDTO operResultDTO = new OperResultDTO();

        if (id == null || id.equals("-1")) {
            boolean result = engineVersionService.insertEngineVersion(engineVersionPO);
            operResultDTO.setResult(result);
        }

        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/disable", method = RequestMethod.POST)
    public OperResultDTO disable(HttpServletRequest request, HttpServletResponse response) {

        String id = request.getParameter("id");


        OperResultDTO operResultDTO = new OperResultDTO();
        operResultDTO.setResult(false);

        if (!(id == null || id.equals("-1"))) {

            EngineVersionPO engineVersionPO = engineVersionService.getEngineVersionById(Integer.valueOf(id));
            if(!HdfsClientProxy.deleteFile(engineVersionPO.getVersionUrl())){
                operResultDTO.setErrMsg("删除hdfs文件失败!");
                return operResultDTO;
            }

            if(engineVersionPO != null){
                engineVersionPO.setStatus(CommonStatusEnum.DISABLE.ordinal());
                engineVersionService.updateEngineVersion(engineVersionPO);
                operResultDTO.setResult(true);
            }

        }

        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/upload", method = RequestMethod.POST)
    public JSONObject upload(HttpServletRequest request, HttpServletResponse response, @RequestParam(value="file",required = false) MultipartFile file) {
        JSONObject jsonObject = new JSONObject();
        String filename=file.getOriginalFilename();
        String versionType = request.getParameter("versionType");
        String hdfspath= StreamContant.HDFS_SYS_PACKAGE_ROOT;
        if(EngineVersionTypeEnum.JSTORM_AM.toString().equals(versionType)){
            if (!filename.endsWith(".jar")) {
                jsonObject.put("status","failed");
                jsonObject.put("errorString", "文件格式错误，操作失败！请您上传正确的文件后提交任务。");
                return jsonObject;
            }
            hdfspath= StreamContant.HDFS_AM_PACKAGE_ROOT;
        }else if (!filename.endsWith(".zip")) {
            jsonObject.put("status","failed");
            jsonObject.put("errorString", "文件格式错误，操作失败！请您上传正确的文件后提交任务。");
            return jsonObject;
        }
        if(HdfsClientProxy.checkFileExists(hdfspath+filename)){
            jsonObject.put("status","failed");
            jsonObject.put("errorString", "上传hdfs文件失败,该文件已经存在!");
            return jsonObject;
        }
        StreamContant.LOCAL_JAR_TMP_PATH = ConfigProperty.getConfigValue(ConfigKeyEnum.LOCAL_PROJECT_ITEM_DIR);
        fileServiceImpl.storeFileToLocalTmp(file,StreamContant.LOCAL_JAR_TMP_PATH+filename);
        if(HdfsClientProxy.uploadFile2Hdfs(StreamContant.LOCAL_JAR_TMP_PATH+filename,hdfspath+filename)){
            File delfile=new File(StreamContant.LOCAL_JAR_TMP_PATH+filename);
            if(delfile.exists()){
                delfile.delete();
            }
            jsonObject.put("status","success");
            jsonObject.put("fileName",hdfspath + filename);
        }else{
            jsonObject.put("status","failed");
            jsonObject.put("errorString", "上传hdfs文件失败。");
            return jsonObject;
        }

        return jsonObject;
    }

    @ResponseBody
    @RequestMapping(value = "/queryByType", method = RequestMethod.POST)
    public List<EngineVersionDTO> queryByType(String engineVersionType) {
        if(StringUtils.isBlank(engineVersionType)){
            return Lists.newArrayList();
        }
        List<EngineVersionPO>  engineVersionPOList =engineVersionService.queryByType(engineVersionType);
        List<EngineVersionDTO> engineVersionDTOList = new ArrayList<EngineVersionDTO>();
        for(EngineVersionPO engineVersionPO : engineVersionPOList){
            EngineVersionDTO engineVersionDTO = new EngineVersionDTO();
            engineVersionDTO.setId(engineVersionPO.getId());
            engineVersionDTO.setVersionRemark("版本（"+ engineVersionPO.getVersionName() +"）版本说明["+ engineVersionPO.getVersionRemark() +"]");
            engineVersionDTOList.add(engineVersionDTO);
        }
        return engineVersionDTOList;
    }
}
