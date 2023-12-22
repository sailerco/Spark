import Utils.readParquet
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SQLQueries(session: SparkSession) extends SimpleQueries {
  readParquet(session)

  override def close(): Future[Unit] = Future {
    session.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    val consistent = session.sql(s"SELECT date, home_team, away_team, team, COUNT(team), home_score, away_score FROM matches "
      + "GROUP BY date, home_team, away_team, team, home_score, away_score "
      + "HAVING (home_team = team AND COUNT(team) != home_score) OR (away_team = team AND COUNT(team) != away_score)"
    )
    consistent.isEmpty
  }

  override def countGoals(name: String): Future[Int] = Future {
    val count = session.sql(s"SELECT COUNT(*) AS goalCount FROM matches WHERE scorer = '$name'")
    count.first().getLong(0).toInt
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    val count = session.sql(s"SELECT COUNT(*) FROM (SELECT DISTINCT date, " +
      s"home_team, away_team, home_score, away_score FROM matches) as unique_matches " +
      s"WHERE (home_score + away_score) BETWEEN $min AND $max")
    count.first().getLong(0).toInt
  }
}

object SQLQueries {
  def apply(session: SparkSession): SQLQueries = new SQLQueries(session)
}

