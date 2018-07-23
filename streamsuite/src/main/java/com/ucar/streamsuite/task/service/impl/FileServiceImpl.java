package com.ucar.streamsuite.task.service.impl;
import com.ucar.streamsuite.common.constant.StreamContant;
import com.ucar.streamsuite.common.util.CreateToJar;
import com.ucar.streamsuite.common.util.DateUtil;
import com.ucar.streamsuite.common.util.HdfsClientProxy;
import com.ucar.streamsuite.task.service.FileService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * jar jstorm project文件管理类
 * Created by 20141022 on 2018/2/8.
 */
@Service
public class FileServiceImpl implements FileService{
    public static final Logger LOGGER = LoggerFactory.getLogger(FileServiceImpl.class);

    @Override
    public boolean storeFileToLocalTmp(MultipartFile file,String localpath){
        try{
            File tfile= new File(localpath);
            if(tfile.exists()){
                tfile.delete();
            }
            file.transferTo(tfile);
        }catch(Exception e){
            LOGGER.error(e.getMessage(),e);
            return false;
        }
        return true;
    }

    @Override
    public  String transferFileWarToJar(MultipartFile file, String archiveName) {
        String fileName=file.getOriginalFilename();
        String jarPackage = "";
        String taskName = "";
        String jarPath = "";
        List<String> needDelFileList = new ArrayList<String>();

        try {
            taskName = fileName.substring(0, fileName.indexOf(".war"));
            CreateToJar.projectname = taskName;
            fileName = taskName + ".zip";

            // 压缩文件保存路径，原始war文件
            String fileCompressPackage = StreamContant.LOCAL_JAR_TMP_PATH + fileName;
            jarPath = StreamContant.LOCAL_JAR_TMP_PATH + taskName;
            String fileDecompressPath = jarPath + "path";

            File tFile = new File(fileCompressPackage);
            if(tFile.exists()){
                tFile.delete();
            }
            file.transferTo(tFile);

            //解压缩
            CreateToJar.decompress(fileCompressPackage, fileDecompressPath);

            //把解压缩以后的的文件目录转为jar文件
            jarPackage = CreateToJar.makeJar(fileDecompressPath, true);

            needDelFileList.add(fileCompressPackage);
            needDelFileList.add(fileDecompressPath);
            needDelFileList.add(jarPath);
            needDelFileList.add(jarPackage);

            String hdfsPath = StreamContant.HDFS_PROJECT_PACKAGE_ROOT + archiveName + "/" + taskName + "_" +  new SimpleDateFormat("yyyyMMddHHmm").format(new Date()) +".jar";
            if(HdfsClientProxy.uploadFile2Hdfs(jarPackage, hdfsPath)){
                return hdfsPath;
            }

            return "ERROR:文件上传失败";
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return "ERROR:文件上传失败";
        }finally {
            for(String deleteFile:needDelFileList){
                deleteAllFile(new File(deleteFile));
            }
        }
    }

    @Override
    public  String transferFileToHdfs(MultipartFile file, String archiveName, String fileExtName) {
        String fileName=file.getOriginalFilename();
        String taskName = "";
        String jarPath = "";
        List<String> needDelFileList = new ArrayList<String>();
        try {
            String localFilePath = StreamContant.LOCAL_JAR_TMP_PATH + fileName;
            File tFile = new File(localFilePath);
            if(tFile.exists()){
                tFile.delete();
            }
            file.transferTo(tFile);

            needDelFileList.add(localFilePath);

            if(fileName.endsWith(fileExtName) ) {
                taskName = fileName.substring(0, fileName.indexOf(fileExtName));
            }
            String hdfsPath = StreamContant.HDFS_PROJECT_PACKAGE_ROOT + archiveName + "/" + taskName + "_" +  new SimpleDateFormat("yyyyMMddHHmm").format(new Date())  + fileExtName;
            if(HdfsClientProxy.uploadFile2Hdfs(localFilePath, hdfsPath)){
                return hdfsPath;
            }

            return "ERROR:文件上传失败";
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return "ERROR:文件上传失败";
        }finally {
            for(String deleteFile:needDelFileList){
                deleteAllFile(new File(deleteFile));
            }
        }
    }

    private static boolean deleteAllFile(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            //递归删除目录中的子目录下
            for (int i=0; i<children.length; i++) {
                boolean success = deleteAllFile(new File(dir, children[i]));
                if (!success) {
                    LOGGER.error("file delete   failed   ----"+children[i].toString());
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}
