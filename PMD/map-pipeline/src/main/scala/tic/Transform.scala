package tic
import org.apache.spark.sql.SparkSession
import scopt._
import tic.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.util.Properties

case class Config(
  mappingInputFile:String = "",
  dataInputFile:String="",
  outputDir:String="",
  jdbcUrl:Option[String]=None,
  jdbcUser:Option[String]=None,
  jdbcPassword:Option[String]=None,
  driverClass:Option[String]=None
)

object Transform {

  val parser = new OptionParser[Config]("Transform") {
    head("Transform", "0.1.0")
    opt[String]("mapping_input_file").required().action((x, c) => c.copy(mappingInputFile = x))
    opt[String]("data_input_file").required().action((x, c) => c.copy(dataInputFile = x))
    opt[String]("output_dir").required().action((x, c) => c.copy(outputDir = x))
  }



  def main(args : Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        val spark = SparkSession.builder.appName("Transform").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        val mapping = spark.read.format("csv").option("header", true).option("mode", "FAILFAST").load(config.mappingInputFile).distinct
        val data = spark.read.format("csv").option("header", true).option("mode", "FAILFAST").load(config.dataInputFile)

        import spark.implicits._
        def copyFilter(s:String) : Option[String] = 
          s.indexOf("___") match {
            case -1 =>
              Some(s)
            case i =>
              None
          }

        def unpivotFilter(s:String) : Option[(String, String)] = 
          s.indexOf("___") match {
            case -1 =>
              None
            case i =>
              Some((s, s.substring(0, i)))
          }

        val mappingCols = mapping.select("column").map(x => x.getString(0)).distinct.collect().toSeq
        val dataCols = data.columns.toSeq

        val columnsToCopy = dataCols.flatMap(copyFilter)
        val unpivotMap = dataCols.flatMap(unpivotFilter)
        val columnsToUnpivot = unpivotMap.map(_._2)
        val dataCols2 = columnsToCopy ++ columnsToUnpivot

        val unknown = dataCols2.diff(mappingCols).toDF("column")
        val missing = mappingCols.diff(dataCols2).toDF("colums")

        val hc = spark.sparkContext.hadoopConfiguration
        writeDataframe(hc, config.outputDir + "/unknown", unknown)
        writeDataframe(hc, config.outputDir + "/missing", missing)

        val stringify = udf((vs: Seq[String]) => vs match {
          case null => null
          case _    => s"""[${vs.mkString(",")}]"""
        })

        val columns = dataCols2.toSeq.toDF("column")
        val columnTables = columns.join(mapping, "column").filter($"table".isNotNull)

        val tables = columnTables.groupBy("table").agg(collect_list("column").as("columns"))
        val tables_string = tables.select($"table", stringify($"columns"))
        writeDataframe(hc, config.outputDir + "/tableschema", tables_string)


        val columnToCopyTables = columnsToCopy.toDF("column").join(mapping, "column").filter($"table".isNotNull).groupBy("table").agg(collect_list("column").as("columns"))
        val joinMap = udf { values: Seq[Map[String,Seq[String]]] => values.flatten.toMap }
        val columnToUnpivotTables = unpivotMap.toDF("column", "column2")
          .join(mapping.withColumnRenamed("column", "column2"), "column2")
          .filter($"table".isNotNull)
          .groupBy("table", "column2").agg(collect_list("column").as("columns"))
          .groupBy("table").agg(joinMap(collect_list(map($"column2", $"columns"))).as("columns2"))

        println("copy " + columnToCopyTables.select("table").collect() + " unpivot " + columnToUnpivotTables.select("table").collect())
        val tablesMap = columnToCopyTables.join(columnToUnpivotTables, Seq("table"), "outer").collect.map(r => (r.getString(r.fieldIndex("table")), Option(r.getSeq[String](r.fieldIndex("columns"))).getOrElse(Seq()), Option(r.getMap[String, Seq[String]](r.fieldIndex("columns2"))).map(_.toMap).getOrElse(Map())))

        def extractTable(columns:Seq[String], unpivots:Map[String, Seq[String]]) = {
          val df = data.select((columns ++ unpivots.values.flatten).map(data.col _) : _*).distinct()
          unpivots.foldLeft(df) {
            case (df, (column2, columns)) =>
              println("processing unpivot " + column2 + " from " + columns.mkString("[",",","]"))

              def toDense(selections : Seq[String]) : String =
                columns.zip(selections).filter{
                  case (_, selection) => selection == "1"
                }.map(_._1).mkString("[",",","]")

              val toDenseUDF = udf(toDense _)
              df.withColumn(column2, toDenseUDF(array(columns.map(df.col _) : _*))).drop(columns : _*)
          }

        }

        tablesMap.foreach {
          case (table, columnsToCopy, columnsToUnpivot) =>
            println("processing table " + table)
            println("copy columns " + columnsToCopy.mkString("[", ",", "]"))
            val df = extractTable(columnsToCopy, columnsToUnpivot)
            writeDataframe(hc, s"${config.outputDir}/tables/${table}", df, header = true)
        }

        spark.stop()
      case None =>
    }

  }
}
