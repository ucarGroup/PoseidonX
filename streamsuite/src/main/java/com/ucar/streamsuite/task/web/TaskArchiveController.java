package com.ucar.streamsuite.task.web;

import com.alibaba.fastjson.JSONObject;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import com.ucar.streamsuite.common.constant.StreamContant;
import com.ucar.streamsuite.common.constant.UserRoleEnum;
import com.ucar.streamsuite.common.dto.OperResultDTO;
import com.ucar.streamsuite.common.dto.PageResultDTO;
import com.ucar.streamsuite.common.filter.WebContextHolder;
import com.ucar.streamsuite.common.util.HdfsClientProxy;
import com.ucar.streamsuite.task.dto.TaskArchiveDTO;
import com.ucar.streamsuite.task.dto.TaskArchivePageQueryDTO;
import com.ucar.streamsuite.task.dto.TaskArchiveVersionDTO;
import com.ucar.streamsuite.task.po.TaskArchivePO;
import com.ucar.streamsuite.task.po.TaskArchiveVersionPO;
import com.ucar.streamsuite.task.service.FileService;
import com.ucar.streamsuite.task.service.TaskArchiveService;
import com.ucar.streamsuite.user.po.UserPO;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.assertj.core.util.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 * Created on 2018/2/8 下午5:23
 *
 */
@Controller
@RequestMapping("/task/archive")
public class TaskArchiveController {

    @Autowired
    private TaskArchiveService taskArchiveService;

    @Autowired
    private FileService fileServiceImpl;

    @Autowired
    private TaskArchiveService taskArchiveServiceImpl;

    @ResponseBody
    @RequestMapping(value = "/list", method = RequestMethod.POST)
    public PageResultDTO list(HttpServletRequest request, HttpServletResponse response) {
        Integer pageNum = Integer.valueOf(request.getParameter("pageNum"));
        Integer pageSize = Integer.valueOf(request.getParameter("pageSize"));
        String createUser =  request.getParameter("createUser");
        String archiveName  = request.getParameter("archiveName");

        TaskArchivePageQueryDTO pageQueryDTO = new TaskArchivePageQueryDTO(pageNum,pageSize,createUser,archiveName);
        return  taskArchiveService.pageQuery(pageQueryDTO);
    }


    @ResponseBody
    @RequestMapping(value = "/queryById", method = RequestMethod.POST)
    public TaskArchiveDTO queryById(Integer id) {

        TaskArchivePO taskArchivePO = taskArchiveService.getArchiveById(id);

        TaskArchiveDTO taskArchiveDTO = new TaskArchiveDTO();
        taskArchiveDTO.setId(taskArchivePO.getId());
        taskArchiveDTO.setCreateTime(taskArchivePO.getCreateTime());
        taskArchiveDTO.setStatus(taskArchivePO.getStatus());
        taskArchiveDTO.setTaskArchiveName(taskArchivePO.getTaskArchiveName());
        taskArchiveDTO.setTaskArchiveRemark(taskArchivePO.getTaskArchiveRemark());
        taskArchiveDTO.setUserGroupId(taskArchivePO.getUserGroupId());
        return taskArchiveDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/update", method = RequestMethod.POST)
    public OperResultDTO update(TaskArchivePO  taskArchivePO){
        OperResultDTO operResultDTO= new OperResultDTO();
        operResultDTO.setResult(true);
        try{
            taskArchiveService.updateArchive(taskArchivePO);
        }catch (Exception e){
            operResultDTO.setErrMsg(e.getMessage());
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public OperResultDTO save(TaskArchivePO  taskArchivePO){
        OperResultDTO operResultDTO= new OperResultDTO();
        operResultDTO.setResult(true);
        try{
            taskArchiveService.insertArchive(taskArchivePO);
        }catch (Exception e){
            operResultDTO.setErrMsg(e.getMessage());
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        return operResultDTO;
    }

    @ResponseBody
    @RequestMapping(value = "/saveNewVersion", method = RequestMethod.POST)
    public OperResultDTO saveNewVersion(HttpServletRequest request,TaskArchiveVersionPO  taskArchiveVersionPO){

        OperResultDTO operResultDTO= new OperResultDTO();
        operResultDTO.setResult(true);
        String archiveId = request.getParameter("archiveId");
        try{
            taskArchiveVersionPO.setTaskArchiveId(Integer.valueOf(archiveId));
            taskArchiveService.insertArchiveVersion(taskArchiveVersionPO);
        }catch (Exception e){
            e.printStackTrace();
            operResultDTO.setErrMsg(e.getMessage());
            operResultDTO.setResult(false);
            return operResultDTO;
        }
        return operResultDTO;
    }

    //undo 传递的war地址时候从远程下载war
    @ResponseBody
    @RequestMapping(value = "/upload", method = RequestMethod.POST)
    public JSONObject upload(HttpServletRequest request,  @RequestParam(value="file",required = false) MultipartFile file) {
        JSONObject jsonObject = new JSONObject();
        String archiveId = request.getParameter("archiveId");
        if(StringUtils.isBlank(archiveId)){
            jsonObject.put("status","failed");
            jsonObject.put("errorString", "项目参数ID不全");
            return jsonObject;
        }

        String filename=file.getOriginalFilename();
        TaskArchivePO taskArchivePO=taskArchiveServiceImpl.getArchiveById(Integer.valueOf(archiveId));
        String archiveName = taskArchivePO.getTaskArchiveName();
        if (StringUtils.isBlank(archiveName) || file == null || file.getSize()<=0 ||StringUtils.isBlank(filename)) {
            jsonObject.put("status","failed");
            jsonObject.put("errorString", "上传的文件名为空或上传文件大小为0，请您重新上传");
            return jsonObject;
        }

        if (!filename.endsWith(".war")) {
            jsonObject.put("status","failed");
            jsonObject.put("errorString", "文件格式错误，操作失败！请您上传正确的文件。");
            return jsonObject;
        }
        StreamContant.LOCAL_JAR_TMP_PATH = ConfigProperty.getConfigValue(ConfigKeyEnum.LOCAL_PROJECT_ITEM_DIR);
        String hdfspath=fileServiceImpl.transferFileWarToJar(file,archiveName);
        if(hdfspath.startsWith("ERROR")){
            jsonObject.put("status","failed");
            jsonObject.put("errorString", hdfspath);
            return jsonObject;
        }else{
            jsonObject.put("status","success");
            jsonObject.put("fileName",hdfspath);
            return jsonObject;
        }
    }

    @ResponseBody
    @RequestMapping(value = "/getArchiveByUser", method = RequestMethod.POST)
    public List<TaskArchiveDTO> getArchiveByUser() {
        UserPO userPO = WebContextHolder.getLoginUser();
        String queryUserName = "";
        if(userPO.getUserRole() != UserRoleEnum.SUPER_ADMIN.ordinal()){
            queryUserName = userPO.getUserName();
        }
        List<TaskArchivePO> taskArchivePOList =taskArchiveService.getArchiveByUser(queryUserName);
        List<TaskArchiveDTO> taskArchiveDTOList = new ArrayList<TaskArchiveDTO>();
        for(TaskArchivePO taskArchivePO : taskArchivePOList){
            TaskArchiveDTO taskArchiveDTO = new TaskArchiveDTO();
            taskArchiveDTO.setId(taskArchivePO.getId());
            taskArchiveDTO.setTaskArchiveName(taskArchivePO.getTaskArchiveName());
            taskArchiveDTOList.add(taskArchiveDTO);
        }
        return taskArchiveDTOList;
    }

    @ResponseBody
    @RequestMapping(value = "/getArchiveVersionByArchiveId", method = RequestMethod.POST)
    public List<TaskArchiveVersionDTO> getTaskVersionByArchiveId(String archiveId) {
        if(StringUtils.isBlank(archiveId)){
            return Lists.newArrayList();
        }
        List<TaskArchiveVersionPO> taskArchiveVersionPOs =taskArchiveService.getTaskArchiveVersionByArchiveId(Integer.valueOf(archiveId));
        List<TaskArchiveVersionDTO> taskArchiveVersionDTOs = new ArrayList<TaskArchiveVersionDTO>();
        for(TaskArchiveVersionPO taskArchiveVersionPO : taskArchiveVersionPOs){

            TaskArchiveVersionDTO taskArchiveVersionDTO = new TaskArchiveVersionDTO();
            taskArchiveVersionDTO.setId(taskArchiveVersionPO.getId());

            String url = taskArchiveVersionPO.getTaskArchiveVersionUrl().replace(StreamContant.HDFS_PROJECT_PACKAGE_ROOT,"");
            url = url + " " + (StringUtils.isBlank(taskArchiveVersionPO.getTaskArchiveVersionRemark())?"": "[" +taskArchiveVersionPO.getTaskArchiveVersionRemark()+ "]");

            taskArchiveVersionDTO.setTaskArchiveVersionUrl(url);
            taskArchiveVersionDTO.setTaskArchiveVersionRemark(taskArchiveVersionPO.getTaskArchiveVersionRemark());
            taskArchiveVersionDTO.setCreateUser(taskArchiveVersionPO.getCreateUser());
            taskArchiveVersionDTO.setCreateTime(taskArchiveVersionPO.getCreateTime());

            taskArchiveVersionDTOs.add(taskArchiveVersionDTO);
        }
        return taskArchiveVersionDTOs;
    }

    @RequestMapping(value="/download",method = RequestMethod.GET)
    public String download(@RequestParam(value = "id") String id, HttpServletResponse response){
        TaskArchiveVersionPO taskArchiveVersionPO = taskArchiveService.getTaskArchiveVersionById(Integer.valueOf(id));
        String archiveVersionUrl = taskArchiveVersionPO.getTaskArchiveVersionUrl();
        String[] archiveVersionUrls = archiveVersionUrl.split("/");
        String fileName =  archiveVersionUrls[archiveVersionUrls.length-1];

        response.setContentType("application/force-download");
        response.addHeader("Content-Disposition", "attachment;fileName=" + fileName);
        byte[] buffer = new byte[8*1024*1024];
        FSDataInputStream fsIn = null;
        FileSystem hadoopFS = null;
        try{
            hadoopFS = HdfsClientProxy.getHadoopFS();
            fsIn = hadoopFS.open(new Path( archiveVersionUrl ));

            OutputStream os = response.getOutputStream();

            int i = fsIn.read(buffer);
            while (i != -1) {
                os.write(buffer, 0, i);
                i = fsIn.read(buffer);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if (fsIn != null) {
                try {
                    fsIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(hadoopFS != null){
                try {
                    hadoopFS.close();
                }catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

}




