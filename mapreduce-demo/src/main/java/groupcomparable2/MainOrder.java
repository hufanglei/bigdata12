package groupcomparable2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * 需求: 求出每一个订单中最贵的商品
 * 二次排序 + 分组排序 + 分区案例
 */
public class MainOrder {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args=new String[]{"d:\\input\\GroupingComparator.txt","d:\\output\\sort3"};
        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(MainOrder.class);

        // 3 关联map和reduce
        job.setMapperClass(OrderMap.class);
        job.setReducerClass(OrderReduce.class);
        // 4 设置最终输出类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);


        //指定辅助排序
        job.setGroupingComparatorClass(GroupComparee.class);

        //指定分区
        job.setPartitionerClass(OrderPartition.class);
        job.setNumReduceTasks(3);

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 提交
        job.waitForCompletion(true);
    }
}
