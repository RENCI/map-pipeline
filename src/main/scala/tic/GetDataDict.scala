package tic
import dispatch._, Defaults._
import java.io.{File, PrintWriter}
import scala.util.{Success, Failure}

object GetDataDict
{
  def getDataDict(token: String, output_file: String, func: () => Unit) : Unit = {
    val myRequest = host("redcap.vanderbilt.edu").secure / "api" / ""
    def myPostWithParams = myRequest.POST << Map(
      "token" -> token,
      "content" -> "metadata",
      "format" -> "json",
      "returnFormat" -> "json"
    ) <:< Map(
      "Content-Type" -> "application/x-www-form-urlencoded",
      "Accept" -> "application/json"
    )

    println(myPostWithParams.toRequest)
    val client = Http.default
    client(myPostWithParams > as.File(new File(output_file))) onComplete {
      x => {
        x match {
          case Success(w) =>
            println(w)
          case Failure(e) =>
            println(e)
        }
        client.shutdown()
        func()
      }
    }

  }

  def main(argv: Array[String]) = {
    getDataDict(argv(0), argv(1), ()=>{})
  }
}
