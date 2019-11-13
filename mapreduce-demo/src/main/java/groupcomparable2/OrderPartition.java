package groupcomparable2;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartition extends Partitioner<OrderBean, NullWritable> {
    //1%3= 2

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        return (orderBean.getId() & Integer.MAX_VALUE) % i;
    }
}
