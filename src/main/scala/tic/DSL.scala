package tic

import scala.util.parsing.combinator.RegexParsers
import org.apache.spark.sql.SparkSession
import scopt._
import tic.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.util.Properties
import scala.collection.mutable.Map

sealed trait AST
case object N_A extends AST
case class Field(a:String) extends AST
case class ExtractFirstName(a:AST) extends AST
case class ExtractLastName(a:AST) extends AST
case class Coalesce(a:AST, b:AST) extends AST

object DSL {

  object NameParser extends RegexParsers {
    override def skipWhitespace = false

    def name0 : Parser[String] = {
      "[a-zA-Z]+[a-z]+".r
    }

    def name : Parser[String] = {
      name0 ~ ("-" ~ name0).? ^^ {
        case a ~ None => a
        case a ~ Some(b ~ c) => a + b + c
      }
    }

    def initial : Parser[String] = {
      "[a-zA-Z]\\.?".r
    }

    def first_name : Parser[String] = {
      initial ||| name
    }

    def middle_name : Parser[String] = {
      "(" ~> name <~ ")" | (initial ||| name)
    }

    def last_name : Parser[String] = {
      name
    }

    def title0 : Parser[String] = {
      "MD" |
      "M.D." |
      "PhD" |
      "MPH" |
      "MBBCH" |
      "MSCE"
    }

    def title : Parser[String] = {
      ",".? ~> " " ~> title0
    }

    def fml : Parser[Seq[String]] = {
      first_name ~ ((" " ~> middle_name) ~> (" " ~> last_name)) ^^ {
          case a ~ b => Seq(a, b)
      }
    }

    def fl : Parser[Seq[String]] = {
      first_name ~ (" " ~> last_name) ^^ {
        case a ~ b => Seq(a, b)
      }
    }

    def lf : Parser[Seq[String]] = {
      last_name ~ (", " ~> first_name) ^^ {
        case a ~ b => Seq(b, a)
      }
    }

    def l : Parser[Seq[String]] = {
      last_name ^^ {
        a => Seq(null, a)
      }
    }

    def pi_name0 : Parser[Seq[String]] = {
      "Dr." ~> " " ~> pi_name0 | (fml ||| fl ||| lf ||| l) <~ title.* <~ " ".?
    }

    def pi_name : Parser[Seq[String]] = {
      "unknown" ^^ { _ => Seq("unknown", "unknown") } |
      "Pending" ^^ { _ => Seq("Pending", "Pending") } |
      pi_name0
    }

    def apply(input: String): Seq[String] =
      if(input == null) {
        Seq(null, null)
      }
      else {
        parseAll(pi_name, input) match {
          case Success(result, _) => result
          case failure: NoSuccess =>
            println("error parsing pi name " + input + ", " + failure.msg)
            Seq(null, input)
        }
      }

  }

  val parseName = udf(NameParser.apply _)

  object DSLParser extends RegexParsers {

    def n_a : Parser[AST] = {
      "n/a" ^^ { _ => N_A}
    }
    def field: Parser[AST] = {
      "[a-zA-Z_][a-zA-Z0-9_]*".r ^^ { str => Field(str) }
    }

    def value: Parser[AST] = {
      "(" ~> term <~ ")" |
      "extract_first_name" ~ value ^^ {
        case _ ~ field =>
          ExtractFirstName(field)
      } |
      "extract_last_name" ~ value ^^ {
        case _ ~ field =>
          ExtractLastName(field)
      } |
      field
    }

    def term: Parser[AST] = {
      value ~ rep("/" ~ term) ^^ {
        case field ~ list =>
          list.foldLeft(field) {
            case (a, _ ~ b) => Coalesce(a, b)
          }
      }
    }

    def expr : Parser[AST] = {
      n_a | term
    }

    def apply(input: String): AST = parseAll(expr, input) match {
      case Success(result, _) => result
      case failure: NoSuccess => scala.sys.error(failure.msg)
    }
  }

  def eval(df: DataFrame, ast: AST): Column =
    ast match {
      case N_A => scala.sys.error("n/a")
      case Field(a) => df.col(a)
      case ExtractFirstName(a) =>
        parseName(eval(df, a)).getItem(0)
      case ExtractLastName(a) =>
        parseName(eval(df, a)).getItem(1)
      case Coalesce(a, b) =>
        when(eval(df, a).isNull, eval(df, b)).otherwise(eval(df, a))
    }

  def fields(ast: AST): Seq[String] =
    ast match {
      case N_A => Seq()
      case Field(a) => Seq(a)
      case ExtractFirstName(a) =>
        fields(a)
      case ExtractLastName(a) =>
        fields(a)
      case Coalesce(a, b) =>
        fields(a).union(fields(b))
    }
}

