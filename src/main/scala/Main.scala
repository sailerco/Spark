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
    utils.createParquetFile()
    val queryList = List(ParquetQueries(session, utils), SparkSQLQueries(session, utils))
    for (queries <- queryList) {
      utils.execute(queries, "Gerd Müller", 15, 20)
    }

    Console.in.read()
    for (query <- queryList)
      query.close()
    Console.in.read()
  }
}
