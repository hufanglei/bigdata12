package friends.mr1;

import index.Drive;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * 博客共同好友 步骤1
 */
public class Friend {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {

        args = new String[]{"d:\\input\\friends.txt","d:\\output\\friend1"};
        Drive.run(Friend.class,MapFriend.class, Text.class,Text.class,ReduceFriend.class,
                Text.class,Text.class,args[0],args[1]);
    }
}
