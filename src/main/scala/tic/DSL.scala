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


  object DSLParser extends RegexParsers {

    def n_a : Parser[AST] = {
      "n/a" ^^ { _ => N_A}
    }
    def field: Parser[AST] = {
      "[a-zA-Z_][a-zA-Z0-9_]*".r ^^ { str => Field(str) }
    }

    def value: Parser[AST] = {
      field |
        "extract_first_name" ~ value ^^ {
          case _ ~ field =>
            ExtractFirstName(field)
        } |
        "extract_last_name" ~ value ^^ {
          case _ ~ field =>
            ExtractLastName(field)
        }
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
        split(eval(df, a), "\\w+").getItem(0)
      case ExtractLastName(a) =>
        split(eval(df, a), "\\w+").getItem(1)
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

