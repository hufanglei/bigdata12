package partition1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReduce  extends Reducer<Text,NullWritable,Text,NullWritable>{

    @Override
    protected void reduce(Text key, Iterable <NullWritable> values, Context context) throws IOException, InterruptedException {
        //输出
        context.write(key,NullWritable.get());
    }
}
