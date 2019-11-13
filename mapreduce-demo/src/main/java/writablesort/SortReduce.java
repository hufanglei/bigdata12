package writablesort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<FlowBean,Text,Text,FlowBean> {
    @Override
    protected void reduce(FlowBean flowBean, Iterable <Text> values, Context context) throws IOException, InterruptedException {
        for (Text text : values) {
            //输出
            context.write(text, flowBean);
        }
    }
}