import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class Utils(session: SparkSession) {

  import session.implicits._

  val shootouts: Dataset[Shootouts] = session.read.option("header", true).option("inferSchema", true).csv("shootouts.csv").as[Shootouts]
  val results: Dataset[Results] = session.read.option("header", true).option("inferSchema", true).csv("results.csv").as[Results]
  val scorer: Dataset[Scorer] = session.read.option("header", true).option("inferSchema", true).csv("goalscorers.csv").as[Scorer]
}

case class Scorer(date: String, home_team: String, away_team: String, team: String, scorer: String,
                  minute: Int, own_goal: Boolean, penalty: Boolean)

case class Results(date: String, home_team: String, away_team: String, home_score: Int, away_score: Int,
                   tournament: String, city: String, country: String, neutral: Boolean)

case class Shootouts(date: String, home_team: String, away_team: String, winner: String)

case class Joined(date: String, home_team: String, away_team: String, home_score: Int, away_score: Int,
                  tournament: String, city: String, country: String, neutral: Boolean, team: String,
                  scorer: String, minute: Int, own_goal: Boolean,penalty: Boolean, winner: String)

case class ResultsWithScorers(date: String, home_team: String, away_team: String, team: String, home_score: Int, away_score: Int, goals: Long)


object Utils {

  def apply(session: SparkSession): Utils = new Utils(session)

  def execute(queries: SimpleQueries, name: String, min: Int, max: Int): Unit = {
    queries.isConsistent().onComplete { case Failure(exception) => exception.printStackTrace()
    case Success(true) => println("no inconsistent records in table 'goalscorers' found...")
    case Success(false) => println("inconsistent records in table 'goalscorers' found.")
    }

    queries.countGoals(name).onComplete { case Failure(exception) => exception.printStackTrace()
    case Success(value) => println(s"$name scored $value goals")
    }

    queries.countRangeGoals(min, max).onComplete { case Failure(exception) => exception.printStackTrace()
    case Success(value) => println(s"number of games with at least $min and at most $max goals: $value")
    }
  }

  def readParquet(session: SparkSession): Dataset[_] = {
    import session.implicits._
    session.read.parquet("./data/matches").as[Joined]
  }

  def createParquetFile(results: Dataset[Results], scorer: Dataset[Scorer], shootouts: Dataset[Shootouts]): Unit = {
    results
      .join(scorer, Seq("date", "home_team", "away_team"), "outer")
      .join(shootouts, Seq("date", "home_team", "away_team"), "outer")
      .write
      .parquet("data/matches")
  }
}
