package com.hfl.api;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class PropertiesUtil {
    public static Properties properties = null;

    static {
        //获取配置文件、方便维护
        InputStream is = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取参数值
     * @param key 名字
     * @return 参数值
     */
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }


}
