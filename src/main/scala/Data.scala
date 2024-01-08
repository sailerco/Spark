case class Scorer(date: String, home_team: String, away_team: String, team: String, scorer: String,
                  minute: Int, own_goal: Boolean, penalty: Boolean)

case class Results(date: String, home_team: String, away_team: String, home_score: Int, away_score: Int,
                   tournament: String, city: String, country: String, neutral: Boolean)

case class Shootouts(date: String, home_team: String, away_team: String, winner: String)

case class JoinedData(date: String, home_team: String, away_team: String, home_score: Option[Int], away_score: Option[Int],
                      tournament: Option[String], city: Option[String], country: Option[String], neutral: Option[Boolean], team: Option[String],
                      scorer: Option[String], minute: Option[Int], own_goal: Option[Boolean], penalty: Option[Boolean], winner: Option[String])

case class GroupedData(date: String, home_team: String, away_team: String, team: String,
                       home_score: Int, away_score: Int, goals: Long)
