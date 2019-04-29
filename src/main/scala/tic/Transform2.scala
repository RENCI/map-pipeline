package tic
import org.apache.spark.sql.SparkSession
import scopt._
import tic.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.util.Properties
import scala.collection.mutable.Map
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import tic.DSL._
import tic.GetData.getData
import tic.GetDataDict.getDataDict

case class Config2(
  mappingInputFile:String = "",
  dataInputFile:String="",
  dataDictInputFile:String="",
  outputDir:String="",
  verbose:Boolean=false
)

object Transform2 {

  val parser = new OptionParser[Config2]("Transform") {
    head("Transform", "0.2.0")
    opt[String]("mapping_input_file").required().action((x, c) => c.copy(mappingInputFile = x))
    opt[String]("data_input_file").required().action((x, c) => c.copy(dataInputFile = x))
    opt[String]("data_dictionary_input_file").required().action((x, c) => c.copy(dataDictInputFile = x))
    opt[String]("output_dir").required().action((x, c) => c.copy(outputDir = x))
    opt[Unit]("verbose").action((_, c) => c.copy(verbose = true))
  }

  def main(args : Array[String]) {
    parser.parse(args, Config2()) match {
      case Some(config) =>
        val spark = SparkSession.builder.appName("Transform").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        val hc = spark.sparkContext.hadoopConfiguration

        import spark.implicits._

        val mapping = spark.read.format("csv").option("header", true).option("mode", "FAILFAST").load(config.mappingInputFile).filter($"InitializeField" === "yes").select($"Fieldname_HEAL", $"Fieldname_phase1", $"Data Type", $"Table_HEAL", $"Primary")

        val dataDict = spark.read.format("json").option("multiline", true).option("mode", "FAILFAST").load(config.dataDictInputFile)
        if(config.verbose) println("reading data")
        var data = spark.read.format("json").option("multiline", true).option("mode", "FAILFAST").load(config.dataInputFile)
        if(config.verbose) println(data.count + " rows read")


        val filterProposal = udf(
          (title : String, short_name: String, pi_firstname : String, pi_lastname : String) =>
            (title == "" || !title.contains(' ')) ||
              ((pi_firstname != "" && !NameParser.isWellFormedFirstName(pi_firstname.head +: pi_firstname.tail.toLowerCase)) &&
                (pi_lastname != "" && !NameParser.isWellFormedLastName(pi_lastname.head +: pi_lastname.tail.toLowerCase)))
        )

        val dataCols = data.columns.toSeq
        // val dataColsExceptProposalID = dataCols.filter(x => x != "proposal_id")
        // val dataColsExceptKnownRepeatedFields = dataCols.filter(x => !Seq("proposal_id", "redcap_repeat_instrument", "redcap_repeat_instance").contains(x))
        // var i = 0
        // val n = dataColsExceptKnownRepeatedFields.size
        // val redundantData = dataColsExceptKnownRepeatedFields.flatMap(x => {
        //   println("looking for repeat data in " + x + " " + i + "/" + n)
        //   i += 1
        //   val dataValueCountByProposalID = data.select("proposal_id", x).filter(col(x) !== "").groupBy("proposal_id").agg(collect_set(col(x)).as(x + "_set"))
        //   val y = dataValueCountByProposalID.filter(size(col(x + "_set")) > 1).map(r => (r.getString(0),r.getSeq[String](1))).collect
        //   println(y.mkString("\n"))
        //   if(y.nonEmpty) {
        //     Seq(y.map(y => (x, y._1, y._2)))
        //   } else {
        //     Seq()
        //   }
        // }).toDF("proposal_id", "column", "value")
        // writeDataframe(hc, config.outputDir + "/redundant", redundantData, header = true)

        if(config.verbose) println("filtering data")
        data = data.filter($"redcap_repeat_instrument" === "" && ($"redcap_repeat_instance".isNull || $"redcap_repeat_instance" === ""))
        if(config.verbose) println(data.count + " rows remaining")

        var negdata = data.filter(filterProposal($"proposal_title2", $"short_name", $"pi_firstname", $"pi_lastname"))
        writeDataframe(hc, config.outputDir + "/filtered", negdata, header = true)

        if(config.verbose) println("filtering data 2")
        data = data.filter(!filterProposal($"proposal_title2", $"short_name", $"pi_firstname", $"pi_lastname"))
        if(config.verbose) println(data.count + " rows remaining")


        val pkMap = mapping
          .filter($"Primary" === "yes")
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



        val mappingCols = mapping.select("Fieldname_phase1").distinct.map(x => DSLParser.fields(DSLParser(x.getString(0)))).collect().toSeq.flatten
        val unknown = dataCols2.diff(mappingCols).toDF("column")
        val missing = mappingCols.diff(dataCols2).toDF("colums")

        writeDataframe(hc, config.outputDir + "/unknown", unknown)
        writeDataframe(hc, config.outputDir + "/missing", missing)

        val generateIDCols = mapping.select("Fieldname_HEAL", "Fieldname_phase1").distinct.collect.flatMap(x => {
          val ast = DSLParser(x.getString(1))
          ast match {
            case GenerateID(as) => Some((x.getString(0), as))
            case _ => None
          }
        }).toSeq

        generateIDCols.foreach {
          case (col, as) =>
            println("generating ID for column " + col)
            val cols2 = as.zip((0 until as.size).map("col" + _))
            cols2.foreach {
              case (ast, col2) =>
                data = data.withColumn(col2, DSLParser.eval(data, col2, ast))
            }
            println("select columns " + as)
            val df2 = data.select(cols2.map({case (_, col2) => data.col(col2)}) : _*).distinct.withColumn(col, monotonically_increasing_id)
            data = data.join(df2, cols2.map({case (_, col2) => col2}), "left")
            cols2.foreach {
              case (_, col2) =>
                data = data.drop(col2)
            }
            println(data.count + " rows remaining")
        }

        // data.cache()

        val containsColumnToCopy = udf((fieldName_phase1 : String) => DSLParser.fields(DSLParser(fieldName_phase1)).intersect(columnsToCopy).nonEmpty)

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
              DSLParser.eval(data, fieldname_HEAL, DSLParser(fieldname_phase1)).as(fieldname_HEAL)
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
              if(config.verbose) println(df.count + " rows copied")
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
              if(config.verbose) println("joining " + tableMap(table).count() + " rows to " + df.count() + " rows on " + pks + ". The result has " + df2.count() + " rows ")
              tableMap(table) = df2
            }
        }

        tableMap.foreach {
          case (table, df) =>
            val file = s"${config.outputDir}/tables/${table}"
            writeDataframe(hc, file, df, header = true)
        }

        val file2 = s"${config.outputDir}/tables/reviewer_organization"
        if (fileExists(hc, file2)) {
          println(file2 + " exists")
        } else {
          val extendColumnPrefix = "reviewer_name_"
          val reviewerOrganizationColumns = dataCols.filter(column => column.startsWith(extendColumnPrefix))
          println("processing table reviewer_organization extending columns " + reviewerOrganizationColumns)
          val df = reviewerOrganizationColumns.map(reviewOrganizationColumn => {
            val reviewers = data.select(data.col(reviewOrganizationColumn).as("reviewer")).filter($"reviewer" =!= "").distinct
            val organization = reviewOrganizationColumn.drop(extendColumnPrefix.length)
            reviewers.withColumn("organization", lit(organization))
          }).reduce(_ union _)
          writeDataframe(hc, file2, df, header = true)
        }

        val file3 = s"${config.outputDir}/tables/name"
        if (fileExists(hc, file3)) {
          println(file3 + " exists")
        } else {
          val func1 = udf((x : String) => DSLParser.fields(DSLParser(x)) match {
            case Nil => null
            case a :: _ => a
          })
          val ddrdd = dataDict
            .join(mapping
              .withColumn("field_name", func1($"Fieldname_phase1"))
              , Seq("field_name"))
            .filter($"select_choices_or_calculations" =!= "")
            .select("field_name", "select_choices_or_calculations", "Fieldname_HEAL", "Table_HEAL")
            .rdd
            .flatMap(row => {
              val field_name = row.getString(0)
              val select_choices_or_calculations = row.getString(1)
              val field_name_HEAL = row.getString(2)
              val table_name = row.getString(3)
              MetadataParser(select_choices_or_calculations) match {
                case None => Seq()
                case Some(cs) =>
                  cs.map {
                    case Choice(i, d) =>
                      Row(field_name, table_name, field_name_HEAL, i, d)
                  }
              }
            })

          val ctsa = dataDict
            .filter($"field_name".rlike("^ctsa_[0-9]*$"))
            .select(lit("org_name").as("field_name"), lit("Submitter").as("table"), lit("submitterInstitution").as("column"), $"field_name".substr(lit(6), length($"field_name")).as("index"), $"field_label".as("description"))

          val df = spark.createDataFrame(ddrdd, StructType(Seq(
            StructField("field_name", StringType, true),
            StructField("table", StringType, true),
            StructField("column", StringType, true),
            StructField("index", StringType, true),
            StructField("description", StringType, true)
          ))).union(ctsa).withColumn("id", concat($"field_name", lit("___"), $"index")).drop("field_name")
          writeDataframe(hc, file3, df, header = true)
        }

        spark.stop()
        
      case None =>
    }

  }
}
