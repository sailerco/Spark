import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class Utils(session: SparkSession) {

  import session.implicits._

  val shootouts: Dataset[Shootouts] = session.read.option("header", true).option("inferSchema", true).csv("shootouts.csv").as[Shootouts]
  val results: Dataset[Results] = session.read.option("header", true).option("inferSchema", true).csv("results.csv").as[Results]
  val scorer: Dataset[Scorer] = session.read.option("header", true).option("inferSchema", true).csv("goalscorers.csv").as[Scorer]

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

  def readParquet(): Dataset[JoinedData] = {
    session
      .read
      .parquet("./data/matches")
      .as[JoinedData]
  }

  def createParquetFile(): Unit = {
    results
      .join(scorer, Seq("date", "home_team", "away_team"), "outer")
      .join(shootouts, Seq("date", "home_team", "away_team"), "outer")
      .write
      .mode("overwrite")
      .parquet("data/matches")
  }
}
object Utils {
  def apply(session: SparkSession): Utils = new Utils(session)
}
