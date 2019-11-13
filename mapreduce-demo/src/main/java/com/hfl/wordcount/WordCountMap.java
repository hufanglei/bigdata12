package com.hfl.wordcount;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

//坑，自动导包Text很容易导成java
public class WordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        //拿到数据,进行数据转换Text=》String
        String line = value.toString();
        //按照空格切分
        String[] split = line.split(" ");
        //输出数据KEYOUT, VALUEOUT
        for (String s : split) {
            //数据转换String=》Text   int=》IntWritable
            context.write(new Text(s), new LongWritable(1));
        }
    }
}

