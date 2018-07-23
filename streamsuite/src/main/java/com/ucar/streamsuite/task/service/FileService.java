package com.ucar.streamsuite.task.service;

import org.springframework.web.multipart.MultipartFile;

/**
 * Description: 任务文件处理类
 * Created on 2018/1/18 下午4:33
 *
 */
public interface FileService {
     boolean storeFileToLocalTmp(MultipartFile file,String localpath);
     String transferFileWarToJar(MultipartFile file, String archiveName);
     String transferFileToHdfs(MultipartFile file, String archiveName, String fileExtName);
}
