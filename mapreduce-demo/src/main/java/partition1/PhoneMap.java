package partition1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMap extends Mapper<LongWritable,Text,Text,NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //输出
        String s = value.toString();
        //切分
        String[] ss = s.split("\t");
        //循环输出
        for (String fileds:ss) {
            context.write(new Text(fileds), NullWritable.get());
        }

    }
}
