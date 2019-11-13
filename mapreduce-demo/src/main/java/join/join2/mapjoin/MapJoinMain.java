package join.join2.mapjoin;

import join.driver.Drive;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;

public class MapJoinMain {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {
        /**
         * 主类
         * @param object 主类
         * @param mymap map类
         * @param mymapkey map输入key
         * @param mymapvalue map输出value
         * @param args1 FileInputFormat输入路径
         * @param args2 FileOutputFormat输出路径
         * @param num reduce个数
         * @param args3 加载缓存的路径
         *            * */
        args = new String[]{"d:\\input\\order.txt", "d:\\output\\mapjoin-mapper", "file:///d:/input/pd.txt"};
        Drive.run(MapJoinMain.class,MapJoin.class, Text.class, NullWritable.class,
                0,args[0],args[1],args[2]);
    }
}
