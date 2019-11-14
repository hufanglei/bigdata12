package outputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义OutputFormat案例
 * 需求:
 * 根据一个文件的内容,输出指定名称的不同文件.
 * 准备test.txt
 */
public class MyRecordWriter extends RecordWriter<Text,NullWritable>{

    FSDataOutputStream itstarout = null;
    FSDataOutputStream otherout = null;

    public MyRecordWriter(TaskAttemptContext context){

        //获取job里面传递的输出目录
        Path outputPath = FileOutputFormat.getOutputPath(context);

        //获取文件系统
        FileSystem fs = null;
        try {
            fs = FileSystem.get(context.getConfiguration());

            //创建两个输出流
            itstarout = fs.create(new Path(outputPath,"baidu.log"));
            otherout = fs.create(new Path(outputPath,"other.log"));


        }catch (Exception e){
            e.printStackTrace();
        }

    }
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        //判断是否包含itstar字段，是写到itstar.log
        if(text.toString().contains("baidu")){
            itstarout.write(text.toString().getBytes());
        }else {
            otherout.write(text.toString().getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        //关闭资源
        if (itstarout!=null){
            itstarout.close();
        }
        if (otherout!=null){
            otherout.close();
        }
    }
}

