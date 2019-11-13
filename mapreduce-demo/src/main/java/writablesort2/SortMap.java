package writablesort2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * KEYIN, VALUEIN, KEYOUT, VALUEOUT
 * LongWritable,Text,FlowBean,NullWritable
 * map 的输入          map的输出
 *
 * */
public class SortMap extends Mapper<LongWritable,Text,FlowBean,NullWritable> {
    FlowBean f = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取数据
            String line = value.toString();
            //切分
            String[] split = line.split("\t");

            //赋值/数据封装

            f.setId(Integer.parseInt(split[0]));
            f.setPrice(Double.parseDouble(split[2]));

            context.write(f,NullWritable.get());
    }

}
