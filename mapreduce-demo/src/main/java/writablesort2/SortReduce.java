package writablesort2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 分组排序
 */
public class SortReduce extends Reducer<FlowBean,NullWritable,FlowBean,NullWritable> {
    @Override
    protected void reduce(FlowBean flowBean, Iterable <NullWritable> values, Context context) throws IOException, InterruptedException {

            //输出
            context.write(flowBean, NullWritable.get());
    }
}