import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class DataSetQueries(session: SparkSession, results: Dataset[_], scorer: Dataset[_]) extends SimpleQueries {
  import session.implicits._
  override def close(): Future[Unit] = Future {
    session.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    val joinedDate = scorer
      .join(results, Seq("date", "home_team", "away_team"))
      .groupBy("date", "home_team", "away_team", "team", "home_score", "away_score")
      .agg(count("team").as("goals")).as[ResultsWithScorers]

    val filtered = joinedDate
      .filter((col("home_team") === col("team") && col("home_score") =!= col("goals"))
        || (col("away_team") === col("team") && col("away_score") =!= col("goals")))

    filtered.count() == 0
  }

  override def countGoals(name: String): Future[Int] = Future {
    scorer.filter(col("scorer") === name).count().toInt
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    results.filter((col("home_score") + col("away_score")).between(min, max)).count().toInt
  }
}

object DataSetQueries {
  def apply(session: SparkSession, results: Dataset[_], scorer: Dataset[_]): DataSetQueries = new DataSetQueries(session, results, scorer)
}
