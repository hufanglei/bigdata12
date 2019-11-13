package friends.mr1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceFriend extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {

        //将所有的person以，拼接，为value
        StringBuilder sb = new StringBuilder();
        for (Text preson:values){
            sb.append(preson).append(",");
        }
        //最后输出的末尾去掉，
        context.write(key,new Text(sb.toString().substring(0,sb.lastIndexOf(","))));

    }
}
