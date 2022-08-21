import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import java.lang.String

import scala.collection.mutable.ListBuffer

object SparkDistCp {

  def getFileOrPath(path: String, filePaths: ListBuffer[String]){
    val conf = new Configuration()
    conf.set("fs.defaultFS", "file:///")
    val ls : Array[FileStatus] = FileSystem.get(conf).listStatus(new Path(path))
    ls.foreach{
      fStatus => {
        if (fStatus.isFile) {
          filePaths += fStatus.getPath.toString
        }else if (fStatus.isDirectory) {
          getFileOrPath(fStatus.getPath.toString, filePaths)
        }
      }
    }
  }

  def parseOptions(map: Map[Nothing, Nothing], args: List[String]): OptionMap = {
    def isSwtich (s: String) = (s(0) =="-")

    args match {
      case Nil => map
      case "-i" :: value => parseOptions(map ++ Map("ignorFailure" -> 1), args.tail)
      case "-m" :: value :: tail => parseOptions(map ++ Map("maxconcurrency" -> value.toInt), tail)
      case string::Nil => parseOptions(map ++ Map("outfile" -> string), args.tail)
      case string:: tail => parseOptions(map ++ Map("infile" -> string), tail)
      case option: opt2 ::tail if isSwtich(opt2) => println("unknown option" + option)
        sys.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {

     val argLs : List[String] = args.toList
     val conf : SparkConf = new SparkConf()
       .setAppName("sparkCP")
       .setMaster("local[*]")

     val sc:SparkContext = SparkContext.getOrCreate(conf)

     val options = parseOptions(Map(), argLs)
     println("The command arguments for running: " + options)

     val sfolder = String.valueOf(options(Symbol("infile"))))
     val tfolder = String.valueOf(options(Symbol("outfile")))
     val concurrency = (options(Symbol("concurrency"))).toString.toInt
     val ignoreFailure = (options(Symbol("ignoreFailur"))).toString.toInt
     val sb = new StringBuffer()
     var fileNames = new ListBuffer[String]()
     getFileOrPath(sfolder, fileNames)
     fileNames.foreach(f => {
       sc.textFile(f, concurrency).saveAsTextFile(f.replace(sfolder, tfolder))
     })
  }



}
