package Writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMap extends Mapper<LongWritable,Text,Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取数据
        String line = value.toString();
        //切分
        String[] split = line.split("\t");

        //赋值
        long upflow=Long.parseLong(split[split.length-3]);
        long downflow=Long.parseLong(split[split.length-2]);


        context.write(new Text(split[1]),new FlowBean(upflow,downflow));
    }
}
