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

        val hc = spark.sparkContext.hadoopConfiguration

        
        val dataCols2 = columnsToCopy ++ columnsToUnpivot

        val unknown = dataCols2.diff(mappingCols).toDF("column")
        val missing = mappingCols.diff(dataCols2).toDF("colums")

        writeDataframe(hc, config.outputDir + "/unknown", unknown)
        writeDataframe(hc, config.outputDir + "/missing", missing)
        

        val stringify = udf((vs: Seq[String]) => vs match {
          case null => null
          case _    => s"""[${vs.mkString(",")}]"""
        })

        /*
          val columns = dataCols.toSeq.toDF("column")
          val columnTables = columns.join(mapping, "column").filter($"table".isNotNull)

          val tables = columnTables.groupBy("table").agg(collect_list("column").as("columns"))
          val tables_string = tables.select($"table", stringify($"columns"))
          writeDataframe(hc, config.outputDir + "/tableschema", tables_string)
         */


        // columns to copy
        val columnToCopyTables = columnsToCopy.toDF("column").join(mapping, "column").filter($"table".isNotNull).groupBy("table").agg(collect_list("column").as("columns"))

        // columns to unpivot
        val joinMap = udf { values: Seq[Map[String,Seq[String]]] => values.flatten.toMap }
        val columnToUnpivotTables = unpivotMap.toDF("column", "column2")
          .join(mapping.withColumnRenamed("column", "column2"), "column2")
          .filter($"table".isNotNull)
          .groupBy("table", "column2").agg(collect_list("column").as("columns"))

        println("copy " + columnToCopyTables.select("table").collect() + " unpivot " + columnToUnpivotTables.select("table","column2").collect())
        val columnToCopyTablesMap = columnToCopyTables.collect.map(r => (r.getString(r.fieldIndex("table")), Option(r.getSeq[String](r.fieldIndex("columns"))).getOrElse(Seq())))
        val columnToUnpivotTablesMap = columnToUnpivotTables.collect.map(r => (r.getString(r.fieldIndex("table")), r.getString(r.fieldIndex("column2")), Option(r.getMap[String, Seq[String]](r.fieldIndex("columns"))).map(_.toMap).getOrElse(Map())))

        def extractColumnToCopyTable(columns: Seq[String]) =
          data.select(columns.map(data.col _) : _*).distinct()

        def extractColumnToUnpivotTable(primaryKeys: Seq[String], column2: String, unpivots: Seq[String]) = {
          val df = data.select(unpivots.map(data.col _) : _*).distinct()
          println("processing unpivot " + column2 + " from " + unpivots.mkString("[",",","]"))

          def toDense(selections : Seq[String]) : Seq[String] =
            unpivots.zip(selections).filter{
              case (_, selection) => selection == "1"
            }.map(_._1)

          val schema = StructType(
            primaryKeys.map(prikey => StructField(prikey, StringType, true)) :+ StructField(column2, StringType, true))

          spark.createDataFrame(df.rdd.flatMap(r => {
            val prikeyvals = primaryKeys.map(prikey => r.getString(r.fieldIndex(prikey)))
            val unpivotvals = unpivots.map(prikey => r.getString(r.fieldIndex(prikey)))
            val dense = toDense(unpivotvals)
            dense.map(selection =>
              Row.fromSeq(primaryKeys :+ selection))
          }), schema)

        }

        columnToCopyTablesMap.foreach {
          case (table, columnsToCopy) =>
            val file = s"${config.outputDir}/tables/${table}"
            if (fileExists(hc, file)) {
              println(file + " exists")
            } else {
              println("processing column to copy table " + table)
              println("copy columns " + columnsToCopy.mkString("[", ",", "]"))
              val df = extractColumnToCopyTable(columnsToCopy)
              writeDataframe(hc, file, df, header = true)
            }
        }

        columnToUnpivotTablesMap.foreach {
          case (table, column2, columnsToUnpivot) =>
            val file = s"${config.outputDir}/tables/${table}_${column2}"
            if (fileExists(hc, file)) {
              println(file + " exists")
            } else {
              println("processing column to unpivot table " + table + ", column " + column2)
              println("unpivoting columns " + columnsToUnpivot.mkString("[", ",", "]"))
              val df = extractColumnToUnpivotTable(Seq("proposal_id", "redcap_repeat_instance", "redcap_repeat_instrument"), column2, columnsToCopy)
              writeDataframe(hc, file, df, header = true)
            }
        }

        val extendColumnPrefix = "reviewer_name_"
        val reviewerOrganizationColumns = dataCols.filter(column => column.startsWith(extendColumnPrefix))
        println("processing table reviewer_organization extending columns " + reviewerOrganizationColumns)
        val df = reviewerOrganizationColumns.map(reviewOrganizationColumn => {
          val reviewers = data.select(data.col(reviewOrganizationColumn).as("reviewer")).filter($"reviewer".isNotNull).distinct
          val organization = reviewOrganizationColumn.drop(extendColumnPrefix.length)
          reviewers.withColumn("organization", lit(organization))
        }).reduce(_ union _)
        writeDataframe(hc, s"${config.outputDir}/tables/reviewer_organization", df, header = true)

        spark.stop()
      case None =>
    }

  }
}
