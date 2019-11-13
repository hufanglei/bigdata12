package Writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable <FlowBean> values, Context context) throws IOException, InterruptedException {

        //初始化两个计数器，分别计算上行流量之和，下行流量之和
        long sumupflow=0;
        long sumdownflow=0;

        for (FlowBean f:values) {
            sumupflow+=f.getUpflow();
            sumdownflow+=f.getDownflow();
        }
        //输出
        context.write(key,new FlowBean(sumupflow,sumdownflow));
    }
}
