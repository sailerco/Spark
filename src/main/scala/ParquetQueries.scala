import Utils.readParquet
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, count}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ParquetQueries(session: SparkSession) extends SimpleQueries {
  val data: Dataset[_] = readParquet(session)

  override def close(): Future[Unit] = Future {
    session.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    data.groupBy("date", "home_team", "away_team", "team", "home_score", "away_score")
      .agg(count("team").as("goals"))
      .filter((col("home_team") === col("team") && col("home_score") =!= col("goals"))
        || (col("away_team") === col("team") && col("away_score") =!= col("goals")))
      .count() == 0
  }

  override def countGoals(name: String): Future[Int] = Future {
    data.filter(col("scorer") === name).count().toInt
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    data
      .select("date", "home_team", "away_team", "home_score", "away_score")
      .distinct()
      .filter((col("home_score") + col("away_score")).between(min, max))
      .count().toInt
  }
}

object ParquetQueries {
  def apply(session: SparkSession): ParquetQueries = new ParquetQueries(session)
}
