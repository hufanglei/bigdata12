package com.hfl.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartion extends Partitioner<Text, LongWritable> {

    public int getPartition(Text text, LongWritable longWritable, int i) {
        //拿到数据
        String t = text.toString();
        //得到每个单词的长度
        int length = t.length();
        if (length % 2 == 0){
            return 0;
        }else{
            return 1;
        }
    }

}
