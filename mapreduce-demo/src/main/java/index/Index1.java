package index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.URISyntaxException;

public class Index1 {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {

        args = new String[]{"d:\\input\\index",
                "d:\\output\\index1"};
        Drive.run(Index1.class,Index1Map.class, Text.class,IntWritable.class,Index1Reduce.class,
                Text.class, IntWritable.class,args[0],args[1]);
    }
}
/**
 * 倒排索引map
 * 数据 index Inverted index
 *index--a.txt
 *
 * */
class Index1Map extends Mapper<LongWritable,Text,Text,IntWritable>{


    Text k = new Text();
    IntWritable v = new IntWritable();
    String name;
    /**
     * 初始化获取文件的名字
     * */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取数据 index Inverted index
        String line = value.toString();
        //切分数据
        String[] split = line.split(" ");
        //拼接，关键词和文件名字
        for (String s:split){
            k.set(s+"--"+name);
            v.set(1);
            context.write(k,v);
        }

    }
}
/**
 * reduce
 * 获取map数据：index--a.txt 1
 * 最后结果
 * index--a.txt 2
 *
 *
 * */
class Index1Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException {

        //初始化计数器
        int count = 0;
        for (IntWritable iw:values){
            count+=iw.get();

        }
        v.set(count);
        //输出
        context.write(key,v);
    }
}