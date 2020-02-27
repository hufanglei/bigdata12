package com.hfl.test;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 使用Azkaban调度java程序
 */
public class AzkabanTest {

    public void run() throws IOException {
        // 根据需求编写具体代码
        FileOutputStream fos = new FileOutputStream("/root/datas/word1.txt");
        fos.write("this is a java progress".getBytes());
        fos.close();
    }

    public static void main(String[] args) throws IOException {
        AzkabanTest test = new AzkabanTest();
        test.run();
    }

}
