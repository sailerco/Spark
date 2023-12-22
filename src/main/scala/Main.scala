import Utils.{createParquetFile, execute}
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder
      .appName("Demo")
      .master("local[*]")
      .getOrCreate()
    //Added for Hadoop Error in Windows https://github.com/globalmentor/hadoop-bare-naked-local-fs
    session.sparkContext.hadoopConfiguration.setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])
    session.sparkContext.setLogLevel("WARN")

    val utils = new Utils(session)
    //createParquetFile(utils.results, utils.scorer, utils.shootouts)
    val queryList = List(SQLQueries(session), DataSetQueries(session, utils.results, utils.scorer))
    for (queries <- queryList) {
      execute(queries, "Gerd Müller", 15, 20)
    }

    Console.in.read()
  }
}
