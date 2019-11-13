package friends.mr2;

import index.Drive;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * /**
 *  * 博客共同好友 步骤2
 *  */

public class Friend2 {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {

        args = new String[]{"d:\\output\\friend1","d:\\output\\friend2"};
        Drive.run(Friend2.class,MapFriend.class, Text.class,Text.class,ReduceFriend.class,
                Text.class,Text.class,args[0],args[1]);
    }
}
