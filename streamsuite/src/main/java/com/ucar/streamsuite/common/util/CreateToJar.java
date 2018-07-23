package com.ucar.streamsuite.common.util;
import com.ucar.streamsuite.common.constant.StreamContant;
import org.apache.tools.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 * 打包Spark作业
 */
public final class CreateToJar {
    private static final Logger LOG = LoggerFactory.getLogger(CreateToJar.class);

    private static final String CLASESES="classes";
    private static final String LIB="lib";
    private static final String WEBINF="WEB-INF";
    private static final int FOUR = 4;
    private static final int EIGHT = 8;
    private static final int FILE_BUFFER_SIZE =2048;

    private CreateToJar(){}

    public static String  makeJarWithLib() throws IOException, URISyntaxException {
        return makeJar(true);
    }

    public static String  makeJar() throws IOException, URISyntaxException {
        return makeJar(false);
    }

    /**
     * 执行打包操作，包括内容包括：class + lib 包
     *
     * <br/> Created on 2014-8-25 上午9:13:32
     * @since 3.3
     * @return
     * @throws java.io.IOException
     * @throws java.net.URISyntaxException
     */
    private static String  makeJar(boolean includeLib) throws IOException, URISyntaxException {
        String urlString =  getWEBINFPath();
        if(urlString.contains(WEBINF)) {
            if (urlString.startsWith("jar:")) {
                urlString = urlString.substring(FOUR, urlString.lastIndexOf(WEBINF));

            } else {
                urlString = urlString.substring(0, urlString.lastIndexOf(WEBINF));
            }
        }
        File file = new File(urlString);
        JarOutputStream zos = null;
        try {
            String outDir = outPutPath();
            LOG.info(outDir);
            Manifest manifest = new Manifest();
            manifest.getMainAttributes().putValue("Manifest-Version", "1.0");

            zos = new JarOutputStream(new FileOutputStream(outDir),manifest);
            LOG.info(System.getProperty("java.io.tmpdir")+"此为Job.jar文件产生目录");

            recurseFiles(file, zos, includeLib);

            addConf(file.getPath()+"/WEB-INF/classes",zos);
            XMLUtil.storeElement=null;
            return outDir;
        } finally {
            if (null != zos) {
                zos.close();
            }
        }

    }
    public static String  makeJar(String urlString,boolean includeLib) throws Exception {
        if(urlString.contains(WEBINF)) {
            if (urlString.startsWith("jar:")) {
                urlString = urlString.substring(FOUR, urlString.lastIndexOf(WEBINF));
            } else {
                urlString = urlString.substring(0, urlString.lastIndexOf(WEBINF));
            }
        }
        File file = new File(urlString);
        JarOutputStream zos = null;
        try {
            String outDir = outPutPath();
            LOG.info(outDir);
            Manifest manifest = new Manifest();
            manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
            zos = new JarOutputStream(new FileOutputStream(outDir),manifest);
            LOG.info(System.getProperty("java.io.tmpdir")+"此为Job.jar文件产生目录");
            recurseFiles(file, zos, includeLib);
            addConf(file.getPath()+"/WEB-INF/classes",zos);
            return outDir;
        }finally {
            if (null != zos) {
                zos.close();
            }
        }
    }
    private static void addConf(String confpath,JarOutputStream zos) throws IOException {
        File rootDir = new File(confpath);
        LOG.error("-----confpath"+confpath);
        for(File child : rootDir.listFiles()) {
            String filename=child.getName();
            if(child.getName().equals("zkConfig.Properties")){
                filename="zkConfig.properties";
            }
            if(child.isFile() && (child.getPath().endsWith(".properties") || child.getPath().endsWith(".xml"))) {
                if(child.getName().contains("log4j") || child.getName().equals("defaults.yaml")) {
                    continue;
                }
                LOG.debug("add "+child.getName()+" to jar");
                try {
                    FileInputStream fin = new FileInputStream(child);
                    BufferedInputStream in = new BufferedInputStream(fin);
                    byte[] buf = new byte[FILE_BUFFER_SIZE];
                    int len;
                    JarEntry jarEntry = new JarEntry(filename);
                    zos.putNextEntry(jarEntry);
                    try {
                        while ((len = in.read(buf)) >= 0) {
                            zos.write(buf, 0, len);
                        }
                    } finally {
                        in.close();
                        zos.closeEntry();
                    }
                } catch (Exception e) {
                    LOG.error("add "+child.getName()+" to jar failed");
                }
            }
        }

    }

    private static void recurseFiles(File file, JarOutputStream zos, boolean includeLib)
            throws IOException {
        if (file.isDirectory()) {
            String[] fileNames = file.list();
            if (fileNames != null) {
                for (int i = 0; i < fileNames.length; i++) {
                    recurseFiles(new File(file, fileNames[i]), zos, includeLib);// 给子目录里的文件打包!!
                }
            }
        }else {
            if(file.getName().equals("defaults.yaml")){
                return;
            }
            if(file.getName().equals("storm.yaml")){
                return;
            }
            if(file.getName().contains("slf4j-log4j12")){
                return;
            }
            if(includeLib && file.getName().endsWith(".jar")) {
                assemblyJarToJar(file, zos);
            } else if(!file.getName().endsWith(".jar")
                      && !file.getName().endsWith(".jsp")
                    && !file.getName().endsWith(".js")
                    && !file.getName().endsWith(".css")
                    && !file.getName().endsWith(".png")
                    && !file.getName().endsWith(".jpg")
                    && !file.getName().endsWith(".gif") && !file.getName().equals("defaults.yaml")){
                productJar(file, zos);
            }
        }
    }

    /**
     * 将lib包中的class加入jar包中，其中屏蔽spark和hadoop等相关jar包
     * @param file
     * @param zos
     * @throws java.io.IOException
     */
    private static void assemblyJarToJar(File file, JarOutputStream zos) throws IOException {
        String fileName = file.getName();
        if(fileName.contains("jcl-over-slf4j-")
                || fileName.startsWith("hive-serde")
                || (fileName.startsWith("spark") && !fileName.startsWith("spark-plugin") )
                || fileName.startsWith("base-integrate-jstorm")
                || fileName.startsWith("akka")
                || fileName.startsWith("scala")
                || fileName.startsWith("tachyon")) {
            return;
        }
        boolean customedHbase = false;
        if(fileName.startsWith("base-dao-hbase")) {
            customedHbase = true;
        }
        LOG.error("*******file.getPath():"+file.getPath());
        JarFile jarFile =null;
        try {
             jarFile = new JarFile(file.getPath());
        }catch (Exception e){
            LOG.error(e.getMessage(),e);
            return;
        }
        Enumeration<JarEntry> entries = jarFile.entries();
        while(entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            if((entry.getName().startsWith("META-INF") && (entry.getName().endsWith(".SF")) || entry.getName().endsWith(".DSA")) || entry.getName().endsWith(".RSA") || entry.getName().endsWith(".yaml")) {
                    continue;
            }

            JarEntry jarEntry = new JarEntry(entry.getName());

            try {
                if(entry.getName().contains("components.xml") ){
                    XMLUtil.dealComponentsXml(fileName,entry,zos,jarFile);
                    continue;
                }
                zos.putNextEntry(jarEntry);
                byte[] buf = new byte[FILE_BUFFER_SIZE];
                int len;
                InputStream in = jarFile.getInputStream(entry);
                try {
                        while ((len = in.read(buf)) >= 0) {
                            zos.write(buf, 0, len);
                        }

                } finally {
                    in.close();
                    zos.closeEntry();
                }
            } catch (Exception e) {
                LOG.info("加入jar包错误:"+jarEntry.getName());
            }
        }
    }

    private static  void productJar(File file, JarOutputStream zos) throws IOException {
        JarEntry jarEntry = null;
        if(!isToProductJar(file,CLASESES) && !isToProductJar(file,LIB)){
            return;
        }

        if (isToProductJar(file,CLASESES)) {
            String path = file.getPath().substring(
                    file.getPath().indexOf(CLASESES) + EIGHT);
            path = path.replaceAll("\\\\", "/");
            jarEntry = new JarEntry(path);

        } else if (isToProductJar(file,LIB)) {
            String path = file.getPath().substring(
                    file.getPath().indexOf(LIB));
            path = path.replaceAll("\\\\", "/");

            jarEntry = new JarEntry(path);

        }
        createJar(file, jarEntry, zos);
    }

    private static void createJar(File file, ZipEntry zipEntry, JarOutputStream zos) throws IOException {
        String fileName = file.getName();
        //加载类时和common-logger 冲突，忽略此jar
        if(fileName.trim().equals("defaults.yaml") || fileName.contains("jcl-over-slf4j-") || fileName.contains("log4j") ) {
            return;
        }
        byte[] buf = new byte[FILE_BUFFER_SIZE];
        int len;
        FileInputStream fin = new FileInputStream(file);
        BufferedInputStream in = new BufferedInputStream(fin);

        try {
            zos.putNextEntry(zipEntry);
            while ((len = in.read(buf)) >= 0) {
                zos.write(buf, 0, len);
            }
        }catch(Exception e){

            LOG.error("*******文件名:"+fileName,e.getMessage());
        }
        finally{
            in.close();
            zos.closeEntry();
        }
    }

    private static boolean isToProductJar(File file,String names){
        return file.getPath().contains(names);
    }

    public static String outPutPath(){
        SimpleDateFormat formate = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = formate.format(new Date());
        String outDir = StreamContant.LOCAL_JAR_TMP_PATH+projectname;
        File f=new File(outDir);
        if(!f.exists()){
            f.mkdirs();
        }
        f.setWritable(true, false);
        f=null;
        return outDir+"/"+projectname+"_"+date+".jar" ;
    }

    public static void cleanTempJars() {
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        for(File file : tempDir.listFiles()) {
            if(file.getName().startsWith("Job_") || file.getName().equals("hdfsTransferJar")) {
                try {
                    deleteFile(file);
                } catch (Exception e) {
                    LOG.error("删除文件失败:"+file.getPath(), e);
                }
            }
        }
    }
    public static void deleteFileinPath(String path) {
        File file = new File(path);
        if(file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteFile(child);
            }
        }else{
            file.delete();
        }

    }

    public static void deleteFile(String path) {
        File file = new File(path);
        deleteFile(file);
    }

    public static void deleteFile(File file) {
        if(file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteFile(child);
            }
        }
        file.delete();
    }

    public static String projectname="name";
    public static String getWEBINFPath()
    {
        String path=Thread.currentThread().getContextClassLoader().getResource("").toString();
        path=path.replace('/', '\\'); // 将/换成\
        path=path.replace("file:", ""); //去掉file:
        path=path.replace("classes\\", ""); //去掉class\
        path=path.substring(1); //去掉第一个\,如 \D:\JavaWeb...
        projectname=path;
        projectname=projectname.replace("\\target\\","");
        projectname=projectname.substring(projectname.lastIndexOf("\\")+1,projectname.length());
        System.out.println("projectname="+projectname);
        System.out.println("path="+path+projectname+"\\WEB-INF");
        return path+projectname+"\\WEB-INF";
    }

    public static void decompress(String srcPath, String dest) throws Exception {
        File file = new File(srcPath);
        if (!file.exists()) {
            throw new RuntimeException(srcPath + "所指文件不存在");
        }
        ZipFile zf = new ZipFile(file);
        Enumeration entries = zf.getEntries();
        ZipEntry entry = null;
        while (entries.hasMoreElements()) {
            entry = (ZipEntry) entries.nextElement();
            if (entry.isDirectory()) {
                String dirPath = dest + File.separator + entry.getName();
                File dir = new File(dirPath);
                dir.mkdirs();
            } else {
                // 表示文件
                File f = new File(dest + File.separator + entry.getName());
                if (!f.exists()) {
                    String dirs = f.getParent();
                    File parentDir = new File(dirs);
                    parentDir.mkdirs();
                }
                f.createNewFile();
                // 将压缩文件内容写入到这个文件中
                InputStream is = zf.getInputStream((org.apache.tools.zip.ZipEntry) entry);

                FileOutputStream fos = new FileOutputStream(f);
                int count;
                byte[] buf = new byte[8192];
                while ((count = is.read(buf)) != -1) {
                    fos.write(buf, 0, count);
                }
                is.close();
                fos.close();
            }
        }
    }

    public static String findLastFile(String filepath){
        File file=new File(filepath);
        File[] tempList = file.listFiles();
        String filename="";
        long lasttime=0;
        for (int i = 0; i < tempList.length; i++) {
            String tfilename=tempList[i].getName();
            String time= tfilename.substring(tfilename.indexOf("_")+1,tfilename.indexOf(".jar"));
            if(Long.valueOf(time) >lasttime){
                lasttime=Long.valueOf(time);
                filename=tfilename;
            }
        }
        return filename;
    }

    public static boolean delAllFile(String path) {
        boolean flag = false;
        File file = new File(path);
        if (!file.exists()) {
            return flag;
        }
        if (!file.isDirectory()) {
            return flag;
        }
        String[] tempList = file.list();
        File temp = null;
        for (int i = 0; i < tempList.length; i++) {
            if (path.endsWith(File.separator)) {
                temp = new File(path + tempList[i]);
            } else {
                temp = new File(path + File.separator + tempList[i]);
            }
            if (temp.isFile()) {
                temp.delete();
            }
            if (temp.isDirectory()) {
                delAllFile(path + "/" + tempList[i]);//先删除文件夹里面的文件
                delFolder(path + "/" + tempList[i]);//再删除空文件夹
                flag = true;
            }
        }
        return flag;
    }

    public static void delFolder(String folderPath) {
        try {
            delAllFile(folderPath); //删除完里面所有内容
            String filePath = folderPath;
            filePath = filePath.toString();
            File myFilePath = new File(filePath);
            myFilePath.delete(); //删除空文件夹
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
