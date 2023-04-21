import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object MyApp {
  def main(args: Array[String]): Unit = {
      val future1 = Future { GenerateBeeFlight.main(args)}
      val future2 = Future { CountBeeLandings.main(args)}
      val future3 = Future { LongDistanceFlyers.main(args)}
      val future4 = Future { SaveLongDistanceFlyers.main(args) }

    Await.result(Future.sequence(Seq(future1, future2, future3, future4)), Duration.Inf)
  }
}