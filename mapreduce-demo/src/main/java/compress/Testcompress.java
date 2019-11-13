package compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class Testcompress {
    public static void main(String[] args) throws Exception {

//        compress("F:\\input\\plus\\phone.txt","org.apache.hadoop.io.compress.BZip2Codec");

        decompression("F:\\input\\plus\\phone.txt.bz2",".txt");
    }

    /**
     *解压缩
     * @param filename 文件路径
     * @param  method 编码/解码器
     */

    public static void compress(String filename,String method) throws Exception {

        //创建输入流
        FileInputStream fis = new FileInputStream(new File(filename));

        //通过反射找到编码/解码器
        Class codeClass = Class.forName(method);

        //通过工具类获取编码/解码器的对象，同时还需要传入Hadoop的配置
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass,new Configuration());

        //创建一个输出流
        FileOutputStream fos = new FileOutputStream(new File(filename+codec.getDefaultExtension()));

        //解码器流
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //对接流
        IOUtils.copyBytes(fis,cos,1024*1024*5,true);

//        IOUtils.closeStream(fis);
//        IOUtils.closeStream(fos);
//        IOUtils.closeStream(cos);
    }
    public static void decompression(String filename,String name) throws Exception {
        //获取实例
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec =factory.getCodec(new Path(filename));
        //解压的输入路径
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));

        //输出流
        FileOutputStream fos = new FileOutputStream(new File(filename+name));

        //l流拷贝
        IOUtils.copyBytes(cis,fos,1024*1024*5,false);


//        //获取factory实例
//        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
//        CompressionCodec codec = factory.getCodec(new Path(filename));
//        //配置解压缩的输入
//        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));
//
//        //输出流
//        FileOutputStream fos = new FileOutputStream(new File(filename+"."+name));
//
//        //流拷贝
//        IOUtils.copyBytes(cis,fos,1024*1024*5,true);
    }
}
