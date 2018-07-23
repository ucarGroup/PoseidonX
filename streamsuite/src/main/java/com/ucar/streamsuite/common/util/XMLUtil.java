package com.ucar.streamsuite.common.util;


import com.ucar.streamsuite.common.constant.StreamContant;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 * Created by 20141022 on 2017/9/19.
 */
public class XMLUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(XMLUtil.class);
    public static  List<Element>  storeElement=null;
    public static boolean dealComponentsXml(File  orgfile){
      if(orgfile.getName().equals("components.xml")){
          LOGGER.error("deal components.xml");
          if(null ==storeElement){
              storeElement=readNodeStore(orgfile);
          }else{
              writeNodeToFile(orgfile);
              storeElement=null;

          }
          return true;
      }
        return false;
    }

    public static boolean dealComponentsXml(String filename,JarEntry entry,JarOutputStream zos,JarFile jarFile){
       try {
           if(filename.startsWith("foundation-service") || filename.startsWith("cat-client")){
           }else{
               return false;
           }
           InputStream in = jarFile.getInputStream(entry);
           LOGGER.error("dealed components.xml");
           if(null ==storeElement){
               storeElement=readNodeStore(in);
           }else{
               JarEntry jarEntry = new JarEntry(entry.getName());
               zos.putNextEntry(jarEntry);
               writeNodeToFile(in,zos);
               storeElement=null;
           }
           in.close();
           zos.closeEntry();
       }catch (Exception e){
           LOGGER.error(e.getMessage(),e);
       }

        return true;
    }
    public static boolean dealComponentsXml(InputStream in ,JarOutputStream zos){
            LOGGER.error("dealed components.xml");
            if(null ==storeElement){
                storeElement=readNodeStore(in);
            }else{
                writeNodeToFile(in,zos);
                storeElement=null;

            }
            return true;
    }
    public static List readNodeStore(InputStream in) {
        List<Element> returnElement=new LinkedList<Element>();
        try {
            boolean validate = false;
            SAXBuilder builder = new SAXBuilder(validate);
            Document doc = builder.build(in);
            Element root = doc.getRootElement();
            // 获取根节点 <university>
            for(Element  element: root.getChildren()){
                List<Element> childElement= element.getChildren();
                for(Element tmpele:childElement){
                    returnElement.add(tmpele);
                }

            }
            return returnElement;
            //readNode(root, "");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
        }
        return returnElement;
    }
    public static List readNodeStore(File orgfile) {
        List<Element> returnElement=new LinkedList<Element>();
        try {
            boolean validate = false;
            SAXBuilder builder = new SAXBuilder(validate);
            InputStream in =new FileInputStream(orgfile);
            Document doc = builder.build(in);
            Element root = doc.getRootElement();
            // 获取根节点 <university>
            for(Element  element: root.getChildren()){
                List<Element> childElement= element.getChildren();
                for(Element tmpele:childElement){
                    returnElement.add(tmpele);
                }

            }
            return returnElement;
            //readNode(root, "");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
        }
        return returnElement;
    }


    public static void writeNodeToFile(InputStream in,JarOutputStream zos) {
        boolean validate = false;
        try {
            SAXBuilder builder = new SAXBuilder(validate);
            Document doc = builder.build(in);
            // 获取根节点 <university>
            Element root = doc.getRootElement();
            List<Element> list1=root.getChildren();
            for(Element ele:list1){
                for(Element addele:storeElement){
                    ele.addContent(addele.detach());
                }
            }
            String tmppath=System.getProperty("user.dir");
            File file=new File(StreamContant.LOCAL_JAR_TMP_PATH);
            LOGGER.error("write component to file"+StreamContant.LOCAL_JAR_TMP_PATH+"\\tmpfile");
            if(file.exists()){
                file.delete();
                file=new File(StreamContant.LOCAL_JAR_TMP_PATH+"\\tmpfile");
            }
            FileOutputStream fos = new FileOutputStream(file);
            XMLOutputter out = new XMLOutputter();
            out.output(doc, fos);

            deleteFileline(file);
           // BufferedReader  br=new BufferedReader(new FileReader(tmppath+"\tmpfile"));
            byte[] buf = new byte[2048];
            int len;
            InputStream filein = new FileInputStream(file);
                 //   jarFile.getInputStream(entry);
            while ((len = filein.read(buf)) >= 0) {
                zos.write(buf, 0, len);
            }
            filein.close();
        }  catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
        }
    }

    public static void deleteFileline(File file){
        int line =1;
        int num =0;
        try {
            BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
            String str = null;
            List list = new ArrayList();
            while( (str=br.readLine()) != null ){
                ++num;
                if(num == line )
                    continue;
                list.add(str);
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsolutePath()));
            for( int i=0;i<list.size();i++ ){
                bw.write(list.get(i).toString());
                bw.newLine();
            }
            bw.flush();
            bw.close();
        }catch (Exception e){
            LOGGER.error(e.getMessage(),e);
        }

    }
    public static void writeNodeToFile(File destFile,JarOutputStream zos) {
        boolean validate = false;
        try {
            InputStream in = new FileInputStream(destFile);
            SAXBuilder builder = new SAXBuilder(validate);
            Document doc = builder.build(in);
            // 获取根节点 <university>
            Element root = doc.getRootElement();
            List<Element> list=root.getChildren();
            for(Element ele:list){
                for(Element addele:storeElement){
                    ele.addContent(addele.detach());
                }

            }
            XMLOutputter out = new XMLOutputter();
          //  SAXOutput
         //   FileOutputStream fos = new FileOutputStream(destFile);
            out.output(doc, zos);
//            in=null;
//            in = new FileInputStream(destFile);
//            int len;
//            byte[] buf = new byte[2048];
//            while ((len = in.read(buf)) >= 0) {
//                zos.write(buf, 0, len);
//
//            }
            zos.closeEntry();
            in.close();
        }  catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
        }

    }
    public static void writeNodeToFile(File destFile) {
        boolean validate = false;
        try {
            InputStream in = new FileInputStream(destFile);
            //XMLUtil.class.getClassLoader().getResourceAsStream("components2.xml");
            SAXBuilder builder = new SAXBuilder(validate);
            // InputStream in = XMLUtil.class.getClassLoader().getResourceAsStream("components.xml");
            //  InputStream in = new FileInputStream(resfile);
            Document doc = builder.build(in);
            // 获取根节点 <university>
            Element root = doc.getRootElement();
            List<Element> list=root.getChildren();
            for(Element ele:list){
                for(Element addele:storeElement){
                    ele.addContent(addele.detach());
                }

            }
            XMLOutputter out = new XMLOutputter();
            //   Thread.currentThread().getContextClassLoader().getResource("").getPath();
            FileOutputStream fos = new FileOutputStream(destFile);
            out.output(doc, fos);
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        dealComponentsXml(new File(Thread.currentThread().getContextClassLoader().getResource("").getPath()+"components.xml"));
        dealComponentsXml(new File(Thread.currentThread().getContextClassLoader().getResource("").getPath()+"components2.xml"));
        //write(read());
    }
}
