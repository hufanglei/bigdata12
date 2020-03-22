package wordcount

import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {

  def main(args: Array[String]): Unit = {
      //获取spark的环境，1.设置任务的名字，2，设置任务运行的模式，本地or集群
      val conf = new SparkConf().setAppName("scalaWordCount").setMaster("local")
      //2. 创建spark的sc---SparkContext
      val sc = new SparkContext(conf)
     //3.读取数据，数据处理，打印
      sc.textFile("D:\\tmp\\test.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
  }

}
