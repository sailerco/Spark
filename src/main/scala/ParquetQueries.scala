import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ParquetQueries(session: SparkSession, utils: Utils) extends SimpleQueries {

  import session.implicits._

  val data: Dataset[JoinedData] = utils.readParquet()

  override def close(): Future[Unit] = Future {
    session.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    data
      .filter(row => row.home_score.isDefined && row.away_score.isDefined)
      .groupBy("date", "home_team", "away_team", "team", "home_score", "away_score")
      .agg(count("team").as("goals"))
      .as[GroupedData]
      .filter(row => (row.home_team.equals(row.team) && row.home_score != row.goals)
        || (row.away_team.equals(row.team) && row.away_score != row.goals))
      .count() == 0
  }

  override def countGoals(name: String): Future[Int] = Future {
    data.filter(row =>
        row.scorer.isDefined && row.scorer.get.equals(name))
      .count().toInt
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    data
      .filter(row =>
        row.home_score.isDefined &&
          row.away_score.isDefined &&
          (min <= (row.home_score.get + row.away_score.get)) &&
          (max >= (row.home_score.get + row.away_score.get))
      )
      .map(row => (row.date, row.home_team, row.away_team))
      .distinct()
      .count().toInt
  }
}

object ParquetQueries {
  def apply(session: SparkSession, utils: Utils): ParquetQueries = new ParquetQueries(session, utils)
}
