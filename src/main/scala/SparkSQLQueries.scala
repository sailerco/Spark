import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class SparkSQLQueries(session: SparkSession, results: Dataset[_], scorer: Dataset[_]) extends SimpleQueries {

  scorer.createTempView("goalscorer")
  results.createTempView("results")

  override def close(): Future[Unit] = Future {
    session.close()
  }

  override def isConsistent(): Future[Boolean] = Future {
    val consistent = session.sql("SELECT g.date, g.home_team, g.away_team, g.team, count(g.team), r.home_score, r.away_score FROM goalscorer as g " +
      "INNER JOIN results as r on g.date = r.date and g.home_team = r.home_team and g.away_team = r.away_team " +
      "GROUP BY g.date, g.home_team, g.away_team, g.team, r.home_score, r.away_score " +
      "HAVING ((g.home_team = g.team) and (count(g.team) != r.home_score)) or ((g.away_team = g.team) and (count(g.team) != r.away_score))")
    consistent.isEmpty
  }

  override def countGoals(name: String): Future[Int] = Future {
    val count = session.sql(s"SELECT COUNT(*) AS goalCount FROM goalscorer WHERE scorer = '$name'")
    count.first().getLong(0).toInt
  }

  override def countRangeGoals(min: Int, max: Int): Future[Int] = Future {
    val count = session.sql(s"SELECT COUNT(*) FROM results as unique_matches " +
      s"WHERE (home_score + away_score) BETWEEN $min AND $max")
    count.first().getLong(0).toInt
  }
}

object SparkSQLQueries {
  def apply(session: SparkSession, results: Dataset[_], scorer: Dataset[_]): SparkSQLQueries = new SparkSQLQueries(session, results, scorer)
}
