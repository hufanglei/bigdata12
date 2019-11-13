package com.hfl.wordcount;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        //        //方法1：初始化一个计数器

        //开始计数
        for (LongWritable value : values) {
            count = count + value.get();
        }
        //方法2: 重写源代码方法
//        Iterator i$ = values.iterator();
//        while(i$.hasNext()) {
//            LongWritable value = (LongWritable) i$.next();
//            count += count + value.get();
//        }
        context.write(key, new LongWritable(count));
    }
}
