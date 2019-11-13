package writablesort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * 将统计结果按照总流量倒序排序（排序）
 * 自定义排序
 */
public class MainWritable {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args=new String[]{"d:\\output\\writable1\\part-r-00000","d:\\output\\writable12"};
        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(MainWritable.class);

        // 3 关联map和reduce
        job.setMapperClass(SortMap.class);
        job.setReducerClass(SortReduce.class);
        // 4 设置最终输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 提交
        job.waitForCompletion(true);
    }
}
