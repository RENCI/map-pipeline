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
    opt[String]("jdbc_url").action((x, c) => c.copy(jdbcUrl = Some(x)))
    opt[String]("jdbc_user").action((x, c) => c.copy(jdbcUser = Some(x)))
    opt[String]("jdbc_password").action((x, c) => c.copy(jdbcPassword = Some(x)))
    opt[String]("driver_class").action((x, c) => c.copy(driverClass = Some(x)))
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
        def code(s:String) = 
          s.indexOf("___") match {
            case -1 =>
              s
            case i =>
              s.substring(0, i)
          }

        val dataCols2 = dataCols.map(code)
        val unknown = dataCols2.diff(mappingCols).toDF("column")
        val missing = mappingCols.diff(dataCols).toDF("colums")
        val hc = spark.sparkContext.hadoopConfiguration
        writeDataframe(hc, config.outputDir + "/unknown", unknown)
        writeDataframe(hc, config.outputDir + "/missing", missing)

        val stringify = udf((vs: Seq[String]) => vs match {
          case null => null
          case _    => s"""[${vs.mkString(",")}]"""
        })

        val columns = dataCols.toDF("column0")
        val codeUdf = udf(code _)
        val columnTables = columns.withColumn("column", codeUdf($"column0").as("column")).join(mapping, "column").drop("column").withColumnRenamed("column0", "column").filter($"table".isNotNull)

        val tables = columnTables.groupBy("table").agg(collect_list("column").as("columns"))
          

        val tables_string = tables.select($"table", stringify($"columns"))
        writeDataframe(hc, config.outputDir + "/tableschema", tables_string)

        val tablesMap = tables.collect.map(r => (r.getString(0), r.getSeq[String](1)))
        def extractTable(columns:Seq[String]) =
          data.select(columns.intersect(dataCols).map(x => data.col(x)) : _*).distinct()

        config.jdbcUrl match {
          case Some(jdbcUrl) =>
            val driverClass = config.driverClass.get
            Class.forName(driverClass)
            val connectionProperties = new Properties()

            connectionProperties.put("user", s"${config.jdbcUser.get}")
            connectionProperties.put("password", s"${config.jdbcPassword.get}")
            connectionProperties.put("Driver", driverClass)
            tablesMap.foreach {
              case (table, columns) =>
                println("processing table " + table)
                val df = extractTable(columns)
                df.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, s"${table}", connectionProperties)
            }

          case None =>
            tablesMap.foreach {
              case (table, columns) =>
                println("processing table " + table)
                val df = extractTable(columns)
                writeDataframe(hc, s"${config.outputDir}/tables/${table}", df, header = true)
            }
        }

        spark.stop()
      case None =>
    }

  }
}
