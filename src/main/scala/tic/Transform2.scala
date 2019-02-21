package tic
import org.apache.spark.sql.SparkSession
import scopt._
import tic.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.util.Properties
import scala.collection.mutable.Map
import tic.DSL._

case class Config2(
  mappingInputFile:String = "",
  dataInputFile:String="",
  outputDir:String=""
)

object Transform2 {

  val parser = new OptionParser[Config2]("Transform") {
    head("Transform", "0.2.0")
    opt[String]("mapping_input_file").required().action((x, c) => c.copy(mappingInputFile = x))
    opt[String]("data_input_file").required().action((x, c) => c.copy(dataInputFile = x))
    opt[String]("output_dir").required().action((x, c) => c.copy(outputDir = x))
  }



  def main(args : Array[String]) {
    parser.parse(args, Config2()) match {
      case Some(config) =>
        val spark = SparkSession.builder.appName("Transform").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        val hc = spark.sparkContext.hadoopConfiguration

        import spark.implicits._

        val mapping = spark.read.format("csv").option("header", true).option("mode", "FAILFAST").load(config.mappingInputFile).select($"Fieldname_HEAL", $"Fieldname_phase1", $"Data Type", $"Table_HEAL", $"Key")
        var data = spark.read.format("csv").option("header", true).option("mode", "FAILFAST").load(config.dataInputFile).filter($"redcap_repeat_instrument".isNull && $"redcap_repeat_instance".isNull)

        val pkMap = mapping
          .filter($"Key".like("%primary%"))
          .groupBy("Table_HEAL")
          .agg(collect_list(struct("Fieldname_phase1", "Fieldname_HEAL")).as("primaryKeys"))
          .map(r => (r.getString(0), r.getSeq[Row](1).map(x => (x.getString(0), x.getString(1)))))
          .collect()
          .toMap

        println("pkMap = " + pkMap)

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


        val dataCols = data.columns.toSeq
        val columnsToCopy = dataCols.flatMap(copyFilter)
        val unpivotMap = dataCols.flatMap(unpivotFilter)
        val columnsToUnpivot = unpivotMap.map(_._2)

        val dataCols2 = columnsToCopy ++ columnsToUnpivot

        val datatypes = mapping.select("Fieldname_phase1", "Data Type").distinct.map(r => (r.getString(0), r.getString(1))).collect.toSeq
        datatypes.filter(x => x._2 == "boolean").map(_._1).foreach {
          col =>
          if (dataCols.contains(col)) {
            data = data.withColumn("tmp", data.col(col).cast(BooleanType)).drop(col).withColumnRenamed("tmp", col)
          }
        }



        val mappingCols = mapping.distinct.select("Fieldname_phase1").map(x => fields(DSLParser(x.getString(0)))).collect().toSeq.flatten
        val unknown = dataCols2.diff(mappingCols).toDF("column")
        val missing = mappingCols.diff(dataCols2).toDF("colums")

        writeDataframe(hc, config.outputDir + "/unknown", unknown)
        writeDataframe(hc, config.outputDir + "/missing", missing)

        val containsColumnToCopy = udf((fieldName_phase1 : String) => fields(DSLParser(fieldName_phase1)).intersect(columnsToCopy).nonEmpty)

        // columns to copy
        val columnToCopyTables = mapping
          .filter(containsColumnToCopy($"Fieldname_phase1"))
          .filter($"Table_HEAL".isNotNull)
          .groupBy("Table_HEAL")
          .agg(collect_list(struct("Fieldname_phase1", "Fieldname_HEAL")).as("columns"))

        // columns to unpivot
        val joinMap = udf { values: Seq[Map[String,Seq[String]]] => values.flatten.toMap }
        val columnToUnpivotTables = unpivotMap.toDF("column", "Fieldname_phase1")
          .join(mapping, "Fieldname_phase1")
          .filter($"Table_HEAL".isNotNull)
          .groupBy("Table_HEAL", "Fieldname_HEAL")
          .agg(collect_list("column").as("columns"))

        println("copy " + columnToCopyTables.select("Table_HEAL").collect() + " unpivot " + columnToUnpivotTables.select("Table_HEAL","Fieldname_HEAL").collect())
        val columnToCopyTablesMap = columnToCopyTables.collect.map(r => (r.getString(r.fieldIndex("Table_HEAL")), Option(r.getSeq[Row](r.fieldIndex("columns")).map(x => (x.getString(0), x.getString(1)))).getOrElse(Seq())))

        val columnToUnpivotTablesMap = columnToUnpivotTables.collect.map(r => (r.getString(r.fieldIndex("Table_HEAL")), r.getString(r.fieldIndex("Fieldname_HEAL")), Option(r.getSeq[String](r.fieldIndex("columns"))).getOrElse(Seq())))

        val columnToUnpivotToSeparateTableTables = columnToUnpivotTables.groupBy("Table_HEAL").agg(count("Fieldname_HEAL").as("count"))
          .filter($"count" > 1).select("Table_HEAL").map(r => r.getString(0)).collect()

        columnToUnpivotToSeparateTableTables.foreach(r => println(r + " has > 1 unpivot fields"))

        assert(columnToUnpivotToSeparateTableTables.isEmpty)

        def extractColumnToCopyTable(columns: Seq[(String, String)]) =
          data.select( columns.map {
            case (fieldname_phase1, fieldname_HEAL) =>
              eval(data, DSLParser(fieldname_phase1)).as(fieldname_HEAL)
          } : _*).distinct()

        def extractColumnToUnpivotTable(primaryKeys: Seq[(String,String)], column2: String, unpivots: Seq[String]) = {
          val df = data.select((primaryKeys.map(_._1) ++ unpivots).map(data.col _) : _*).distinct()
          println("processing unpivot " + column2 + " from " + unpivots.mkString("[",",","]"))

          def toDense(selections : Seq[String]) : Seq[String] =
            unpivots.zip(selections).filter{
              case (_, selection) => selection == "1"
            }.map(_._1)

          val schema = StructType(
            primaryKeys.map(_._2).map(prikey => StructField(prikey, StringType, true)) :+ StructField(column2, StringType, true))

          spark.createDataFrame(df.rdd.flatMap(r => {
            val prikeyvals = primaryKeys.map(_._1).map(prikey => r.getString(r.fieldIndex(prikey)))
            val unpivotvals = unpivots.map {
              fieldname_phase1 => r.getString(r.fieldIndex(fieldname_phase1))
            }
            val dense = toDense(unpivotvals)
            dense.map(selection =>
              Row.fromSeq(primaryKeys.map(key => r.getString(r.fieldIndex(key._1))) :+ selection))
          }), schema)

        }

        val tableMap = Map[String, DataFrame]()

        columnToCopyTablesMap.foreach {
          case (table, columnsToCopy) =>
            val file = s"${config.outputDir}/tables/${table}"
            if (fileExists(hc, file)) {
              println(file + " exists")
            } else {
              println("processing column to copy table " + table)
              println("copy columns " + columnsToCopy.mkString("[", ",", "]"))
              val df = extractColumnToCopyTable(columnsToCopy)
              tableMap(table) = df
            }
        }

        columnToUnpivotTablesMap.foreach {
          case (table, column2, columnsToUnpivot) =>
            val file = s"${config.outputDir}/tables/${table}"
            if (fileExists(hc, file)) {
              println(file + " exists")
            } else {
              println("processing column to unpivot table " + table + ", column " + column2)
              println("unpivoting columns " + columnsToUnpivot.mkString("[", ",", "]"))
              val pks = pkMap(table).filter(x => x._1 != "n/a")
              val df = extractColumnToUnpivotTable(pks, column2, columnsToUnpivot)
              val df2 = df.join(tableMap(table), pks.map(_._2))
              println(tableMap(table).head())
              println(df.head())
              println("joining " + tableMap(table).count() + " rows to " + df.count() + " rows on " + pks + ". The result has " + df2.count() + " rows ")
              tableMap(table) = df2
            }
        }

        tableMap.foreach {
          case (table, df) =>
            val file = s"${config.outputDir}/tables/${table}"
            writeDataframe(hc, file, df, header = true)
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
