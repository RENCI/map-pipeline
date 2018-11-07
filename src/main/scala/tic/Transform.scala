package tic
import org.apache.spark.sql.SparkSession
import scopt._
import tic.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Config(
  mappingInputFile:String = "",
  dataInputFile:String="",
  outputDir:String=""
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
        val mapping = spark.read.format("csv").option("header", true).load(config.mappingInputFile)
        val data = spark.read.format("csv").option("header", true).load(config.dataInputFile)
        import spark.implicits._
        val mappingCols = mapping.select("column").map(x => x.getString(0)).collect().toSeq
        val dataCols = data.columns.toSeq
        val unknown = dataCols.diff(mappingCols).toDF("column")
        val missing = mappingCols.diff(dataCols).toDF("colums")
        val hc = spark.sparkContext.hadoopConfiguration
        writeDataframe(hc, config.outputDir + "/unknown", unknown)
        writeDataframe(hc, config.outputDir + "/missing", missing)

        val stringify = udf((vs: Seq[String]) => vs match {
          case null => null
          case _    => s"""[${vs.mkString(",")}]"""
        })

        val tables = mapping.groupBy("table").agg(collect_list("column").as("columns"))
        val tables_string = tables.select(stringify(tables.col("columns")))
        writeDataframe(hc, config.outputDir + "/tableschema", tables_string)

        val tablesMap = tables.collect.map(r => (r.getString(0), r.getSeq[String](1)))

        tablesMap.foreach {
          case (table, columns) =>
            println("processing table " + table)
            val df = data.select(columns.intersect(dataCols).map(x => data.col(x)) : _*).distinct()
            writeDataframe(hc, config.outputDir + "/tables/" + table, df, header = true)
        }

        spark.stop()
      case None =>
    }

  }
}
