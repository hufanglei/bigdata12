package inputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
/**
 * 自定义 inputformat
 *
 *案例实操
 * 小文件处理（自定义InputFormat）。
 *
 * 目标
 * 3个小文件通过mapreduce输出到一个文件中：
 *
 * 思路
 * （1）自定义一个类继承FileInputFormat。
 *  （2）改写RecordReader，实现一次读取一个完整文件封装为KV。
 *  （3）在输出时使用SequenceFileOutPutFormat输出合并文件。
 */
public class SequenceDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"d:\\input\\inpuformat","d:\\output\\inputformat"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SequenceDriver.class);
        job.setMapperClass(SequenceMap.class);
        job.setReducerClass(SequenceReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //设置自定义的inputformat
        job.setInputFormatClass(MyInPutFormat.class);
        //设置输出的二进制
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
