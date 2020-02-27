import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class TestHadoopApi {

    //HDFS获取文件系统
    @Test
    public void initHDFS() throws IOException {
        //创建配置信息
        Configuration config = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(config);
        //打印文件系统
        System.out.println(fs.toString());
    }

    //HDFS文件上传
    @Test
    public void putHDFS() throws IOException, URISyntaxException, InterruptedException {
        //创建配置信息
        Configuration conf = new Configuration();
        //设置部分的临时参数
        conf.set("dfs.replication", "2");
        //获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        //要上传的本地文件上传的路径
//        Path input = new Path("F:\\input\\word.txt");
        Path input = new Path("d:\\soft\\hadoop-2.8.4.tar.gz");
        //输出的路径，iput复制到output
        Path output = new Path("hdfs://bigdata113:9000/");
        //以拷贝的方式
        fs.copyFromLocalFile(input, output);
        fs.close();
        System.out.println("上传成功！！");
    }

    //HDFS文件下载
    @Test
    public void getHDFS() throws URISyntaxException, IOException, InterruptedException {
        //创建配置信息
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        //下载文件
        //boolean delSrc:是否将原文件删除
        //Path src ：要下载的路径
        //Path dst ：要下载到哪
        //boolean useRawLocalFileSystem ：是否校验文件
        fs.copyToLocalFile(false,
                new Path("hdfs://bigdata111:9000/word.txt"),
                new Path("F:\\output"),
                true);
        fs.close();
        System.out.println("下载成功！！");
    }

    //HDFS目录创建
    @Test
    public void mkmdirHDFS() throws Exception {
        //1.创新配置信息对象
        Configuration configuration = new Configuration();
        //2.链接文件系统
        //final URI uri  地址
        //final Configuration conf  配置
        //String user   Linux用户
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata113:9000"), configuration, "root");
        //3.创建目录
        fs.mkdirs(new Path("hdfs://bigdata111:9000/idea/aa"));
        //4.关闭
        fs.close();
        System.out.println("创建文件夹成功");
    }

    //HDFS文件夹删除
    @Test
    public void deleteHDFS() throws Exception {
        //1.创建配置对象
        Configuration conf = new Configuration();

        //2.链接文件系统
        //final URI uri, final Configuration conf, String user
        //final URI uri  地址
        //final Configuration conf  配置
        //String user   Linux用户
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        //Path var1   : HDFS地址
        //boolean var2 : 是否递归删除
        fs.delete(new Path("hdfs://bigdata113:9000/idea/bb"), true);
        fs.close();
        System.out.println("删除成功啦");
    }

    //HDFS文件名更改
    @Test
    public void renameAtHDFS() throws Exception {
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), configuration, "root");

        //2 重命名文件或文件夹
        fs.rename(new Path("hdfs://bigdata113:9000/idea/aa"), new Path("hdfs://bigdata113:9000/idea/cc"));
        fs.close();
        System.out.println("重命名成功啦");
    }

    //HDFS文件详情查看
    @Test
    public void readListFiles() throws Exception {
        //1.创建配置对象
        Configuration conf = new Configuration();
        //2.链接文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");
        //3.迭代器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        //4.遍历迭代器
        while (listFiles.hasNext()) {
            //一个一个出
            LocatedFileStatus fileStatus = listFiles.next();

            //名字
            System.out.println("文件名：" + fileStatus.getPath().getName());
            //块大小
            System.out.println("大小：" + fileStatus.getBlockSize());
            //权限
            System.out.println("权限：" + fileStatus.getPermission());
            System.out.println(fileStatus.getLen());


            BlockLocation[] locations = fileStatus.getBlockLocations();

            for (BlockLocation bl : locations) {
                System.out.println("block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("------------------华丽的分割线----------------");
        }

    }
    //HDFS文件和文件夹判断
    @Test
    public void judge() throws Exception {
        //1.创建配置文件信息
        Configuration conf = new Configuration();

        //2.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //3.遍历所有的文件
        //List the statuses of the files/directories in the given path if the path is a directory.
        FileStatus[] liststatus = fs.listStatus(new Path("/"));
        for(FileStatus status :liststatus)
        {
            //判断是否是文件
            if (status.isFile()){
                //ctrl + d:复制一行
                //ctrl + x 是剪切一行，可以用来当作是删除一行
                System.out.println("文件:" + status.getPath().getName());
            } else {
                System.out.println("目录:" + status.getPath().getName());
            }
        }
    }
    /**
     * 通过IO流操作HDFS
     */
    //HDFS文件上传
    @Test
    public void putFileToHDFSIO() throws URISyntaxException, IOException, InterruptedException {
        //1.创建配置文件信息
        Configuration conf = new Configuration();

        //2.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata113:9000"), conf, "root");

        //3.创建输入流
        FileInputStream fis = new FileInputStream(new File("F:\\input\\hello.txt"));

        //4.输出路径
        //注意：不能/Andy  记得后边写个名 比如：/Andy/Sogou.txt
        Path writePath = new Path("hdfs://bigdata113:9000/hfl/Sogou.txt");
        FSDataOutputStream fos = fs.create(writePath);

        //5.流对接
        //InputStream in    输入
        //OutputStream out  输出
        //int buffSize      缓冲区
        //boolean close     是否关闭流
        try {
            IOUtils.copyBytes(fis,fos,4 * 1024,false);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(fos);
            IOUtils.closeStream(fis);
            System.out.println("上传成功啦");
        }
    }

    //HDFS文件下载( IO读取HDFS到控制台)
    @Test
    public void getFileToHDFSIO() throws URISyntaxException, IOException, InterruptedException {
        //1.创建配置文件信息
        Configuration conf = new Configuration();

        //2.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata113:9000"), conf, "root");

        //3.读取路径
        Path readPath = new Path("hdfs://bigdata113:9000/hfl/Sogou.txt");

        //4.输入
        FSDataInputStream fis = fs.open(readPath);

        //5.输出到控制台
        //InputStream in    输入
        //OutputStream out  输出
        //int buffSize      缓冲区
        //boolean close     是否关闭流
        IOUtils.copyBytes(fis,System.out,4 * 1024 ,true);
    }

    //定位文件读取
    //IO读取第一块的内容
    @Test
    public void  readFlieSeek1() throws Exception {
        //1.创建配置文件信息
        Configuration conf = new Configuration();

        //2.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata113:9000"), conf, "root");

        //3.输入
        Path path = new Path("hdfs://bigdata113:9000/hadoop-2.8.4.tar.gz");
        FSDataInputStream fis = fs.open(path);

        //4.输出
        FileOutputStream fos = new FileOutputStream("F:\\output\\readFileSeek\\A1");

        //5.流对接
        byte[] buf = new byte[1024];
        for (int i = 0; i < 128 * 1024; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        //6.关闭流
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
    //②IO读取第二块的内容
    @Test
    public void readFlieSeek2() throws Exception {
        //1.创建配置文件信息
        Configuration conf = new Configuration();

        //2.获取文件系统
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata113:9000"), conf, "root");

        //3.输入
        Path path = new Path("hdfs://bigdata113:9000/hadoop-2.8.4.tar.gz");
        FSDataInputStream fis = fs.open(path);

        //4.输出
        FileOutputStream fos = new FileOutputStream("F:\\output\\readFileSeek\\A2");

        //5.定位偏移量/offset/游标/读取进度 (目的：找到第一块的尾巴，第二块的开头)
        fis.seek(128 * 1024 * 1024);

        //6.流对接
        IOUtils.copyBytes(fis, fos, 1024);

        //7.关闭流
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }




















}
