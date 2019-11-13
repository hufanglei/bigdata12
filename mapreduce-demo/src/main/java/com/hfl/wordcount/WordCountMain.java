package com.hfl.wordcount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class WordCountMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args=new String[]{"D:\\input\\wordcount.txt","D:\\output\\wcword1"};

        //获取配置文件
        Configuration conf = new Configuration();
        //创建job任务
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountMain.class);

        //指定Map类和map的输出类型 Text, IntWritable
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置分区
//      job.setPartitionerClass(WordCountPartion.class);
////    job.setNumReduceTasks(2);

        //设置combiner
//      job.setCombinerClass(WordCountCombiner.class);

        //指定Reducer类和reduce的输出数据类型 Text,IntWritable
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        //切片优化
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
//        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m

        //NLineInputFormat 按照行数指定分片
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 2);


        //指定数据输入的路径和输出的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交任务
//      job.setJar("WordCount.jar");
        job.waitForCompletion(true);
    }
}
