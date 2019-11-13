package groupcomparable2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReduce extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable> {
    @Override
    protected void reduce(OrderBean flowBean, Iterable <NullWritable> values, Context context) throws IOException, InterruptedException {

            //输出
            context.write(flowBean, NullWritable.get());
        
    }
}