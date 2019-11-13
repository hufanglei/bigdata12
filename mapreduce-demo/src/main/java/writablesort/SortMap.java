package writablesort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * LongWritable,Text,Text,FlowBean
 * map 的输入          map的输出
 *
 * */
public class SortMap extends Mapper<LongWritable,Text,FlowBean,Text> {
    FlowBean f = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取数据
            String line = value.toString();
            //切分
            String[] split = line.split("\t");

            //赋值/数据封装

            f.setUpflow(Long.parseLong(split[1]));
            f.setDownflow(Long.parseLong(split[2]));
            f.setSumflow(Long.parseLong(split[3]));



            context.write(f,new Text(split[0]));
    }

}
