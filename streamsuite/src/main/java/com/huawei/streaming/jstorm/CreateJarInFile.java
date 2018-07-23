package com.huawei.streaming.jstorm;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.ThrowsAdvice;


/**
 * 打包mr 任务的工具类
 *  
 * <br/> Created on 2014-12-18 上午9:12:42
 * @since 3.4
 */

public final class  CreateJarInFile {
	 private static final Logger LOG = LoggerFactory.getLogger(CreateJarInFile.class);
	 private static final String CLASESES="classes";
	 private static final String LIB="lib";
	 public static final String JSTORM="rtcp_task";
	 private static final String WEBINF="WEB-INF";
	 private static final int FOUR = 4;
	 private static final int EIGHT = 8;
	 private static final int FILE_BUFFER_SIZE =2048;
	
	 private CreateJarInFile(){}
/**
 * 执行打包操作，包括内容包括：class + lib 包
 * 
 * <br/> Created on 2014-12-18 上午9:13:32
 * @since 3.4
 * @return
 * @throws IOException
 * @throws URISyntaxException
 */
	public static String makeJar() throws IOException, URISyntaxException {
		
		//获取当前工程路径
		String classFile = CreateJarInFile.class.getName().replaceAll("\\.", "/") + ".class";
		URL url = CreateJarInFile.class.getClassLoader().getResource(classFile);
		String urlString = url.toString();
		if(urlString.startsWith("jar:")){
			urlString = urlString.substring(FOUR,urlString.lastIndexOf(WEBINF));
			
		}else{
			urlString = urlString.substring(0,urlString.lastIndexOf(WEBINF));
		}
		File file = new File(new URI(urlString));
		JarOutputStream zos = null;
		try {
			 
		 String outDir = outPutPath();
		
		  LOG.info(outDir);
		  Manifest manifest = new Manifest();
		  manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
		  String outDirClassJar=outDir+ File.separator+JSTORM+".jar";
		  zos = new JarOutputStream(new FileOutputStream(outDirClassJar),manifest);
		  LOG.info(outDir+"此为jar文件产生目录");
		  
		  recurseFiles(file, zos,outDir);
		  
		  return outDir;
		} finally {
			if (null != zos) {
				zos.close();
			}
		}
	
	}

	private static void recurseFiles(File file, JarOutputStream zos, String outDir)
			throws IOException {
		if (file.isDirectory()) {
			String[] fileNames = file.list();
			if (fileNames != null) {
				for (int i = 0; i < fileNames.length; i++) {
					productJar(file,zos ,outDir);
					recurseFiles(new File(file, fileNames[i]), zos, outDir);// 给子目录里的文件打包!!
				}
			}
		}else {
			productJar(file,zos ,outDir);
		}
	}
	
	private static  void 	productJar(File file, JarOutputStream zos, String outDir) throws IOException {
	
		JarEntry jarEntry = null;
		//过滤 defaults.yaml 和 storm.yaml
        if(file.getName().contains(".yaml"))
         {
             LOG.error("-------------"+file.getName());
          }
		if(file.getName().equals("defaults.yaml")
                || file.getName().equals("storm.yaml")){
			return ;
		}
		//过滤配置文件 ,但是不过滤 配置中心 和zk 的文件
        if((file.getName().contains(".xml")
                || file.getName().contains(".properties"))
                && !file.getName().contains("conf_center.properties")
                && !file.getName().contains("zkConfig.properties")){
            return ;
        }

		
		if(!isToProductJar(file,CLASESES) && !isToProductJar(file,LIB)){
			return ;
		}
		
		try{
			if (isToProductJar(file,CLASESES)) {
				int indexLength = file.getPath().indexOf(CLASESES) + EIGHT;
				if (file.getPath().length() < indexLength)
					return;
				String path = file.getPath().substring(indexLength);
				path = path.replaceAll("\\\\", "/");
				if(file.isDirectory()){
					path = path + File.separator ;
				}
				jarEntry = new JarEntry(path);
				createJar(file, jarEntry, zos);
			
			} else if (isToProductJar(file,LIB)) {
				int indexLength = file.getPath().indexOf(LIB) + FOUR;
				if (file.getPath().length() < indexLength)
					return;
				String path = file.getPath().substring(indexLength);
				path = path.replaceAll("\\\\", "/");
				
				copytoJarInFile(file, outDir+path);
			
			}
		}catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		
		
		
	
	}

	private static void createJar(File file, ZipEntry zipEntry, JarOutputStream zos) throws IOException {
		String fileName = file.getName();
		//加载类时和common-logger 冲突，忽略此jar
		if(fileName.contains("jcl-over-slf4j-")){
			return ;
		}
        if(fileName.contains("storm.yaml")){
            return ;
        }
		//slf4j-log4j12 和 log4j-over-slf4j 冲突
        if(fileName.contains("slf4j-log4j12")){
            return ;
        }


		try {
            zos.putNextEntry(zipEntry);
        }catch(IOException e){
		    if(e.getMessage() != null && e.getMessage().indexOf("duplicate entry") != -1) {
                return;
            }
            else
                throw e;
        }
		if(file.isDirectory()){
			
			return ;
		}
		byte[] buf = new byte[FILE_BUFFER_SIZE];
		int len;
		FileInputStream fin = new FileInputStream(file);
		BufferedInputStream in = new BufferedInputStream(fin);
		try {
			while ((len = in.read(buf)) >= 0) {
				zos.write(buf, 0, len);
			}
		}finally{
			in.close();
			zos.closeEntry();
		}
	
	}

	private static boolean isToProductJar(File file, String names){
		return file.getPath().contains(WEBINF+ File.separator+names);
	}
	
	private static String outPutPath(){
	
		String pah= System.getProperty("java.io.tmpdir")+ File.separator+JSTORM+"_"+ getStirngDate();
		File file=new File(pah);
		file.mkdir();
		String outDir =file.getPath()+ File.separator;
		return outDir ;
	}
	
	private static String getStirngDate(){
		SimpleDateFormat formate = new SimpleDateFormat("yyyyMMddHHmmss");
		String date = formate.format(new Date());
		return date;
	}
	private static void copytoJarInFile(File file, String outPath) throws IOException {
		String fileName = file.getName();
		//加载类时和common-logger 冲突，忽略此jar
		if(fileName.contains("jcl-over-slf4j-")){
			return ;
		}
		byte[] buf = new byte[FILE_BUFFER_SIZE];
		int len;
		FileInputStream fin = new FileInputStream(file);
		BufferedInputStream in = new BufferedInputStream(fin);
		FileOutputStream fos=new FileOutputStream(outPath);
		BufferedOutputStream bfout=new BufferedOutputStream(fos);

		try {
			while ((len = in.read(buf)) >= 0) {
				bfout.write(buf, 0, len);
			}
		}finally{
			in.close();
			bfout.close();
		}
	
	}
	
}