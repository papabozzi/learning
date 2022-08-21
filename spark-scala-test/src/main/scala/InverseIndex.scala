import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.{NewHadoopRDD, RDD}

object InverseIndex {
  def main(args: Array[String]): Unit = {
    //sc: SparkContext
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(InverseIndex.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc: SparkContext = new
        SparkContext(sparkConf)
  sc.setLogLevel("ERROR")
    val line : RDD[(LongWritable, Text)]= sc.newAPIHadoopFile("resources/", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
    val result:RDD[(Path, Text)] = line.asInstanceOf[NewHadoopRDD[LongWritable, Text]].mapPartitionsWithInputSplit((inputSplit, iterator) => {
      val file: FileSplit = inputSplit.asInstanceOf[FileSplit]
      iterator.map(tup => (file.getPath, tup._2))
    })

    //println("length: " + result.map(f => (f._1.getName, f._2.toString)).collect().length)
   // result.map(f => (f._1.getName, f._2.toString)).collect().foreach(println)

    val word : RDD[(String, String)] = result.flatMap ( f => {
      val fileName: String = f._1.getName
      val ws = f._2.toString.split(" ")
      var arr: Array[(String, String)] = Array.empty
      ws.foreach(w => {
        arr = arr.+:((w + "\t" + fileName, fileName))
      })
      arr
    })

    //println("word length: " + word.collect().length)
    //word.collect().foreach(println)

    val wordCountDoc: RDD[(String, Int)] = word.map(e => {
      val wordFileName = e._1
      (wordFileName, 1)
    }).reduceByKey(_ + _)

    val wordCountUnion: RDD[(String, Iterable[String])] = wordCountDoc.map(f => {
      val key: String = f._1.split("\t")(0)
      val v: String =  "(" + f._1.split("\t")(1) +"," + f._2 + ")"
      (key, v)
    }).groupByKey()

    wordCountUnion.collect().foreach(println)
//    (is,Seq((2,1), (0,2), (1,1)))
//    (a,Seq((2,1)))
//    (what,Seq((0,1), (1,1)))
//    (banana,Seq((2,1)))
//    (it,Seq((1,1), (2,1), (0,2)))

  }



}
