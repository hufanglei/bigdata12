package groupcomparable2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMap extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        OrderBean f = new OrderBean();
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