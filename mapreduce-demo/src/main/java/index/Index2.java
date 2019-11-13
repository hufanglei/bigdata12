package index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.URISyntaxException;

public class Index2  {

    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {

        args = new String[]{"d:\\output\\index1",
                "d:\\output\\index2"};
        Drive.run(Index2.class,Index2Map.class, Text.class,Text.class,Index2Reduce.class,
                Text.class, Text.class,args[0],args[1]);
    }
}
/**
 * Inverted--a.txt	3
 Inverted--b.txt	1
 Inverted--c.txt	3
 * */
class Index2Map extends Mapper<LongWritable,Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取数据
        String line = value.toString();
        //切分“--”
        String[] split = line.split("--");
        k.set(split[0]);
        v.set(split[1]);
        context.write(k,v);
    }
}
/**
 * reduce
 * 获取map数据：index    [a.txt 3,b.txt	1,c.txt	3]
 * 最后结果
 *
 * index    a.txt-->3   b.txt-->1   c.txt-->3
 *
 * */
class Index2Reduce extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text t: values){
            //a.txt 3
            String[] split = t.toString().split("\t");
            //重现按照-->
            String s = split[0] + "-->" + split[1];
            sb.append(s).append("\t");
        }
        //输出
        context.write(key,new Text(sb.toString()));
    }
}
