package join.join2.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMap extends Mapper<LongWritable,Text,Text,TableBean>{
    TableBean tableBean = new TableBean();
    Text t = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取文件的路径
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        //每个文件的名字
        String name = fileSplit.getPath().getName();
        //获取数据
        String line = value.toString();
        //判断，根据文件的名字不同添加标记
        if (name.equals("order.txt")){
            String[] fields = line.split("\t");
            //封装
            tableBean.setOrder_id(fields[0]);
            tableBean.setP_id(fields[1]);
            tableBean.setAmonut(Integer.parseInt(fields[2]));
            tableBean.setFlag("0");
            tableBean.setPname("");

            t.set(fields[1]);

        }else {
            String[] fields = line.split("\t");
            //封装
            tableBean.setP_id(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("1");
            tableBean.setOrder_id("");
            tableBean.setAmonut(0);

            t.set(fields[0]);
        }
        context.write(t,tableBean);
    }


}
