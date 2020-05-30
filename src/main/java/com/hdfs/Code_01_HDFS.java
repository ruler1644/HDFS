package com.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * @Auther wu
 * @Date 2019/6/19  14:57
 */
public class Code_01_HDFS {
    FileSystem fs;

    //连接集群
    @Before
    public void before() throws Exception {
        fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "wu");
    }

    //操作
    @Test
    public void mkdirs() throws Exception {
        boolean flag = fs.mkdirs(new Path("/input"));
        System.out.println(flag);
        System.out.println("over");
    }

    @Test
    public void delete() throws Exception {
        boolean flag = fs.delete(new Path("/install"), true);
        System.out.println(flag);
        System.out.println("over");
    }

    //验证参数优先级
    @Test
    public void copyFromLocalFile() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "wu");

        fs.copyFromLocalFile(new Path("D:\\delete\\123.txt"), new Path("/input/"));
        System.out.println("over");
    }

    @Test
    public void put() throws Exception {
        fs.copyFromLocalFile(new Path("D:\\delete\\123.txt"), new Path("/input/"));
        System.out.println("over");
    }

    @Test
    public void get() throws Exception {
        fs.copyToLocalFile(new Path("/panjinlian"), new Path("D:\\delete"));
        System.out.println("over");
    }

    @Test
    public void appendToFile() throws Exception {
        FSDataOutputStream append = fs.append(new Path("/input/123.txt"));
        append.write("haha".getBytes());
        IOUtils.closeStream(append);
    }

    @Test
    public void mv() throws Exception {
        boolean flag = fs.rename(new Path("/panjinlian"), new Path("/panjinlian.txt"));
        //fs.rename(new Path("/panjinlian"), new Path("/input/panjinlian"));
        System.out.println("over");
    }

    @Test
    public void openStream() throws Exception {
        FSDataInputStream is = fs.open(new Path("/input/123.txt"));
        byte[] buff = new byte[1024];
        int len;
        while ((len = is.read(buff)) != -1) {
            String str = new String(buff, 0, len);
            System.out.println(str);
        }
        IOUtils.closeStream(is);
    }

    @Test
    //查看文件详情
    public void fileStatus() throws Exception {

        //一个节点上存储的文件的集合
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            //获取一个文件的详情
            String name = fileStatus.getPath().getName();
            FsPermission permission = fileStatus.getPermission();
            long len = fileStatus.getLen();
            System.out.println(name);
            System.out.println(permission);
            System.out.println(len);

            //获取文件切分的块的信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (int i = 0; i < blockLocations.length; i++) {

                //获取块具体存储的节点
                String[] hosts = blockLocations[i].getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("---------------------");
        }
    }

    @Test
    public void ls() throws Exception {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
    }

    //关闭连接
    @After
    public void after() throws Exception {
        fs.close();
    }
}
