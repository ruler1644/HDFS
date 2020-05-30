package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @Auther wu
 * @Date 2019/6/19  16:13
 */
public class Code_02_HdfsIO {
    FileSystem fileSystem;

    Configuration configuration = new Configuration();
    @Before
    public void before() throws Exception{
        fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),configuration,"wu");
    }
    @Test
    /**将本地文件上传到集群
     * 获取输入流
     * 获取输出流
     * 流的对拷
     */
    public void load() throws Exception{
        FileInputStream fileInputStream = new FileInputStream(new File("D:\\delete\\install.txt"));
        FSDataOutputStream outputStream = fileSystem.create(new Path("/install.aa"));
//        int len;
//        byte[] buff = new byte[128];
//        while((len = fileInputStream.read())!= -1){
//            outputStream.write(buff,0,len);
//        }
        IOUtils.copyBytes(fileInputStream,outputStream,configuration);
        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(outputStream);
        System.out.println("over");
    }

    @Test
    /**将集群文件下载到本地
     * 获取输入流
     * 获取输出流
     * 流的对拷
     */
    public void download() throws  Exception{
        FSDataInputStream fis = fileSystem.open(new Path("/panjinlian.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\delete\\haha.txt"));
        IOUtils.copyBytes(fis,fileOutputStream,configuration);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fileOutputStream);
        System.out.println("over");
    }
    @Test
    /**
     * 定位读取文件，读取第一块
     */
    public void seek1() throws Exception{
        FSDataInputStream fis = fileSystem.open(new Path("/jdk-8u144-linux-x64.tar.gz"));
        FileOutputStream fos = new FileOutputStream(new File("D:\\delete\\jdk-8u144-linux-x64.tar.gz.part1"));
        byte[] buff = new byte[1024];

        for(int i = 0; i < 1024 * 128;++i){
            fis.read(buff);
            fos.write(buff);
        }
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
    @Test
    /**
     * 定位读取文件，读取第二块
     */
    public void seek2() throws Exception{
        FSDataInputStream fis = fileSystem.open(new Path("/jdk-8u144-linux-x64.tar.gz"));

        //设置指定的读取起点
        fis.seek(1024 * 1024 * 128);

        FileOutputStream fos = new FileOutputStream(new File("D:\\delete\\jdk-8u144-linux-x64.tar.gz.part2"));
        IOUtils.copyBytes(fis,fos,configuration);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
    @After
    public void after()throws Exception{
        fileSystem.close();
    }
}
