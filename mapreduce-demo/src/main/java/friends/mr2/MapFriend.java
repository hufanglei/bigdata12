package friends.mr2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class MapFriend extends Mapper<LongWritable,Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取数据 A(friend)	I,K,C,B,G,F,H,O,D(presion)
        String line = value.toString();
        String[] split1 = line.split("\t");
        //map输出v friend
        v.set(split1[0]);
        //第二次切分 [I,K,C,B,G,F,H,O,D] (presion)
        String[] split2 = split1[1].split(",");

//        Arrays.sort(split2);

        for (int i = 0;i<split2.length;i++){
            for (int j = i+1;j<split2.length-1;j++){
                //拼接两个preson I-K
                //第二次循环  I-C
                k.set(split2[i]+"-"+split2[j]);

                context.write(k,v);
            }
        }

    }
}
