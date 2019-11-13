package join.join2.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class MapJoin extends Mapper<LongWritable,Text,Text,NullWritable>{

    HashMap hashMap = new HashMap<String,String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存的文件（产品表）
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("d:\\input\\pd.txt"),"UTF-8"));

        //一行一行读取数据
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            //切分  01	小米
            String[] split = line.split("\t");
            //数据存储进集合
            hashMap.put(split[0],split[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取大表（订单表 order.txt）
        String line  = value.toString();
        //切分 201901	01	1
        String[] fileds = line.split("\t");

        String pid  = fileds[1];
        //进行量表按照pid关联
        if(hashMap.containsKey(pid)){

            context.write(new Text(fileds[0]+"\t"+hashMap.get(pid)+"\t"+fileds[2]),NullWritable.get());
        }
    }
}
