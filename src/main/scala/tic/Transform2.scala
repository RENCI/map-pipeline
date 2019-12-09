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
import java.io.File
import java.util.logging.Logger
import java.util.logging.Level
import java.util.logging.ConsoleHandler
import tic.DSL._
import tic.GetData.getData
import tic.GetDataDict.getDataDict

case class Config2(
  mappingInputFile:String = "",
  dataInputFile:String="",
  dataDictInputFile:String="",
  auxiliaryDir:String="",
  filterDir:String="",
  outputDir:String="",
  verbose:Boolean=false
)

object DataFilter {
  import Transform2._
  type SourceDataFilter = DataFrame => (DataFrame, Option[DataFrame])

  def comp(a : SourceDataFilter, b : SourceDataFilter) : SourceDataFilter = df => {
    val (df1, nd1) = a(df)
    val (df2, nd2) = b(df1)
    (df2, nd2 match {
      case None => nd1
      case _ => nd2
    })
  }

  val id : SourceDataFilter = df => (df, None)

  val testDataFilter : Boolean => SourceDataFilter = (verbose: Boolean) => (data : DataFrame) => {
    logger.info("filtering data 2")
    val filterProposal = udf(
      (title : String, short_name: String, pi_firstname : String, pi_lastname : String) =>
      (title == "" || !title.contains(' ')) ||
        ((pi_firstname != "" && !NameParser.isWellFormedFirstName(pi_firstname.head +: pi_firstname.tail.toLowerCase)) &&
          (pi_lastname != "" && !NameParser.isWellFormedLastName(pi_lastname.head +: pi_lastname.tail.toLowerCase)))
    )

    val f = filterProposal(data.col("proposal_title2"), data.col("short_name"), data.col("pi_firstname"), data.col("pi_lastname"))
    val negdata = data.filter(f)

    val data2 = data.filter(!f)
    if(verbose)
      logger.info(data.count + " rows remaining")
    (data2, Some(negdata))
  }

  val filter1 : Boolean => SourceDataFilter = (verbose: Boolean) => (data : DataFrame) => {
    logger.info("filtering data")
    val data2 = data.filter(data.col("redcap_repeat_instrument") === "" && data.col("redcap_repeat_instance").isNull)
    if(verbose)
      logger.info(data2.count + " rows remaining")
    (data2, None)
  }

  val auxDataFilter : (SparkSession, String, String) => SourceDataFilter = (spark, auxiliaryDir, joinType) => (data : DataFrame) => {
    val dataMappingDfs = new File(auxiliaryDir).listFiles.toSeq.map((f) => {
      logger.info("loading aux " + f.getAbsolutePath())
      spark.read.format("csv").option("header", true).option("mode", "FAILFAST").load(f.getAbsolutePath())
    })

    var data2 = data
    dataMappingDfs.foreach {
      case df =>
        val column = df.columns.head
        logger.info("joining dataframes")
        logger.info("data: " + data.columns.toSeq)
        logger.info("aux: " + df.columns.toSeq)
        logger.info("on " + column)
        logger.info("join type: " + joinType)
        data2 = data2.join(df, Seq(column), joinType)
    }
    (data2, None)
  }

}

import DataFilter._
object Transform2 {

  val logger = Logger.getLogger(this.getClass().getName())
  logger.setLevel(Level.FINEST)

  val ch = new ConsoleHandler()
  logger.addHandler(ch)
  ch.setLevel(Level.FINEST)
  val builder = OParser.builder[Config2]
  val parser =  {
    import builder._
    OParser.sequence(
      programName("Transform"),
      head("Transform", "0.2.2"),
      opt[String]("mapping_input_file").required().action((x, c) => c.copy(mappingInputFile = x)),
      opt[String]("data_input_file").required().action((x, c) => c.copy(dataInputFile = x)),
      opt[String]("data_dictionary_input_file").required().action((x, c) => c.copy(dataDictInputFile = x)),
      opt[String]("auxiliary_dir").required().action((x, c) => c.copy(auxiliaryDir = x)),
      opt[String]("filter_dir").required().action((x, c) => c.copy(filterDir = x)),
      opt[String]("output_dir").required().action((x, c) => c.copy(outputDir = x)),
      opt[Unit]("verbose").action((_, c) => c.copy(verbose = true)))
  }

  def convert(fieldType: String) : DataType =
    fieldType match {
      case "boolean" => BooleanType
      case "int" => IntegerType
      case "date" => DateType
      case "text" => StringType
      case _ => throw new RuntimeException("unsupported type " + fieldType)
    }

  def convertType(col : String, fieldType : String, data : DataFrame) : DataFrame =
    data.withColumn("tmp", data.col(col).cast( convert(fieldType))).drop(col).withColumnRenamed("tmp", col)

  def primaryKeyMap(spark : SparkSession, mapping : DataFrame) : scala.collection.immutable.Map[String, Seq[(String, String, String)]] = {
    import spark.implicits._
    val pkMap = mapping
      .filter($"Primary" === "yes")
      .groupBy("Table_HEAL")
      .agg(collect_list(struct("Fieldname_phase1", "Fieldname_HEAL", "Data Type")).as("primaryKeys"))
      .map(r => (r.getString(0), r.getSeq[Row](1).map(x => (x.getString(0), x.getString(1), x.getString(2)))))
      .collect()
      .toMap

    logger.info("pkMap = " + pkMap)
    pkMap
  }

  def allStringTypeSchema(spark: SparkSession, config : Config2, mapping: DataFrame): StructType = {
    val data0 = spark.read.format("json").option("multiline", true).option("mode", "FAILFAST").load(config.dataInputFile)
    val data0Cols = data0.columns
    StructType(data0Cols.map(x => {
      StructField(x, StringType, true)
    }))
  }

  def readData(spark : SparkSession, config: Config2, mapping: DataFrame): (DataFrame, DataFrame) = {
    import spark.implicits._
    val schema = allStringTypeSchema(spark, config, mapping)
    logger.info("reading data")
    var data = spark.read.format("json").option("multiline", true).option("mode", "FAILFAST").schema(schema).load(config.dataInputFile)
    if(config.verbose)
      logger.info(data.count + " rows read")


    val datatypes = ("redcap_repeat_instrument", "text") +: ("redcap_repeat_instance", "int") +: mapping.select("Fieldname_phase1", "Data Type").filter($"Fieldname_phase1" =!= "n/a").distinct.map(r => (r.getString(0), r.getString(1))).collect.toSeq
    val dataCols = data.columns.toSeq
    for(datatype <- datatypes) {
      val col = datatype._1
      val colType = datatype._2
      if (dataCols.contains(col))
        data = convertType(col, colType, data)
    }

    // val dataColsExceptProposalID = dataCols.filter(x => x != "proposal_id")
    // val dataColsExceptKnownRepeatedFields = dataCols.filter(x => !Seq("proposal_id", "redcap_repeat_instrument", "redcap_repeat_instance").contains(x))
    // var i = 0
    // val n = dataColsExceptKnownRepeatedFields.size
    // val redundantData = dataColsExceptKnownRepeatedFields.flatMap(x => {
    //   logger.info("looking for repeat data in " + x + " " + i + "/" + n)
    //   i += 1
    //   val dataValueCountByProposalID = data.select("proposal_id", x).filter(col(x) !== "").groupBy("proposal_id").agg(collect_set(col(x)).as(x + "_set"))
    //   val y = dataValueCountByProposalID.filter(size(col(x + "_set")) > 1).map(r => (r.getString(0),r.getSeq[String](1))).collect
    //   logger.info(y.mkString("\n"))
    //   if(y.nonEmpty) {
    //     Seq(y.map(y => (x, y._1, y._2)))
    //   } else {
    //     Seq()
    //   }
    // }).toDF("proposal_id", "column", "value")
    // writeDataframe(hc, config.outputDir + "/redundant", redundantData, header = true)

    val (data2, negdata) =
      comp(
        comp(
          comp(
            filter1(config.verbose),
            testDataFilter(config.verbose)
          ), if(config.auxiliaryDir == "") id else auxDataFilter(spark, config.auxiliaryDir, "left")
        ), if(config.filterDir == "") id else auxDataFilter(spark, config.filterDir, "inner")
      )(data)

    (data2, negdata.getOrElse(null))
  }

  def generateID(spark : SparkSession, config : Config2, mapping : DataFrame, data0 : DataFrame): DataFrame = {
    import spark.implicits._
    var data = data0
    val generateIDCols = mapping.select("Fieldname_HEAL", "Fieldname_phase1").distinct.collect.flatMap(x => {
      val ast = DSLParser(x.getString(1))
      ast match {
        case GenerateID(as) => Some((x.getString(0), as))
        case _ => None
      }
    }).toSeq

    generateIDCols.foreach {
      case (col, as) =>
        logger.info("generating ID for column " + col)
        val cols2 = as.zip((0 until as.size).map("col" + _))
        cols2.foreach {
          case (ast, col2) =>
            data = data.withColumn(col2, DSLParser.eval(data, col2, ast))
        }
        logger.info("select columns " + as)
        val df2 = data.select(cols2.map({case (_, col2) => data.col(col2)}) : _*).distinct.withColumn(col, monotonically_increasing_id)
        data = data.join(df2, cols2.map({case (_, col2) => col2}), "left")
        cols2.foreach {
          case (_, col2) =>
            data = data.drop(col2)
        }
        if(config.verbose)
          logger.info(data.count + " rows remaining")
    }
    data
  }

  def diff(spark : SparkSession, mapping : DataFrame, dataCols2 : Seq[String]) : (DataFrame, DataFrame) = {
    import spark.implicits._
    val mappingCols = mapping.select("Fieldname_phase1").distinct.map(x => DSLParser.fields(DSLParser(x.getString(0)))).collect().toSeq.flatten
    val unknown = dataCols2.diff(mappingCols).toDF("column")
    val missing = mappingCols.diff(dataCols2).toDF("colums")
    (unknown, missing)
  }

  def copy(spark: SparkSession, config: Config2, tableMap : Map[String, DataFrame], mapping : DataFrame, data: DataFrame) : Seq[String] = {
    import spark.implicits._
    def copyFilter(s:String) : Option[String] =
      s.indexOf("___") match {
        case -1 =>
          Some(s)
        case i =>
          None
      }
    val dataCols = data.columns.toSeq
    val columnsToCopy = dataCols.flatMap(copyFilter)

    val containsColumnToCopy = udf((fieldName_phase1 : String) => DSLParser.fields(DSLParser(fieldName_phase1)).intersect(columnsToCopy).nonEmpty)

    // columns to copy
    val columnToCopyTables = mapping
      .filter(containsColumnToCopy($"Fieldname_phase1"))
      .filter($"Table_HEAL".isNotNull)
      .groupBy("Table_HEAL")
      .agg(collect_list(struct("Fieldname_phase1", "Fieldname_HEAL")).as("columns"))

    logger.info("copy " + columnToCopyTables.select("Table_HEAL").collect().mkString(","))
    val columnToCopyTablesMap = columnToCopyTables.collect.map(r => (r.getString(r.fieldIndex("Table_HEAL")), Option(r.getSeq[Row](r.fieldIndex("columns")).map(x => (x.getString(0), x.getString(1)))).getOrElse(Seq())))

    def extractColumnToCopyTable(columns: Seq[(String, String)]) =
      data.select( columns.map {
        case (fieldname_phase1, fieldname_HEAL) =>
          DSLParser.eval(data, fieldname_HEAL, DSLParser(fieldname_phase1)).as(fieldname_HEAL)
      } : _*).distinct()

    columnToCopyTablesMap.foreach {
      case (table, columnsToCopy) =>
        logger.info("processing column to copy table " + table)
        logger.info("copy columns " + columnsToCopy.mkString("[", ",", "]"))
        val df = extractColumnToCopyTable(columnsToCopy)
        if(config.verbose)
          logger.info(df.count + " rows copied")
        tableMap(table) = df
    }
    columnsToCopy

  }

  def collect(spark: SparkSession, config: Config2, tableMap : Map[String, DataFrame], mapping : DataFrame, data : DataFrame):Seq[String] = {
    import spark.implicits._
    val pkMap = primaryKeyMap(spark, mapping)
    val dataCols = data.columns.toSeq
    def unpivotFilter(s:String) : Option[(String, String)] =
      s.indexOf("___") match {
        case -1 =>
          None
        case i =>
          Some((s, s.substring(0, i)))
      }


    val unpivotMap = dataCols.flatMap(unpivotFilter)
    val columnsToUnpivot = unpivotMap.map(_._2)

    // columns to unpivot
    val columnToUnpivotTables = unpivotMap.toDF("column", "Fieldname_phase1")
      .join(mapping, "Fieldname_phase1")
      .filter($"Table_HEAL".isNotNull)
      .groupBy("Table_HEAL", "Fieldname_HEAL", "Fieldname_phase1", "Data Type")
      .agg(collect_list("column").as("columns"))

    logger.info("unpivot " + columnToUnpivotTables.select("Table_HEAL","Fieldname_HEAL").collect().mkString(","))

    val columnToUnpivotTablesMap = columnToUnpivotTables.collect.map(r => (r.getString(r.fieldIndex("Table_HEAL")), r.getString(r.fieldIndex("Fieldname_phase1")), r.getString(r.fieldIndex("Fieldname_HEAL")), r.getString(r.fieldIndex("Data Type")), Option(r.getSeq[String](r.fieldIndex("columns"))).getOrElse(Seq())))

    val columnToUnpivotToSeparateTableTables = columnToUnpivotTables.groupBy("Table_HEAL").agg(count("Fieldname_HEAL").as("count"))
      .filter($"count" > 1).select("Table_HEAL").map(r => r.getString(0)).collect()

    columnToUnpivotToSeparateTableTables.foreach(r => logger.info(r + " has > 1 unpivot fields"))

    assert(columnToUnpivotToSeparateTableTables.isEmpty)

    def extractColumnToUnpivotTable(primaryKeys: Seq[(String,String,String)], column2: String, column2Type: String, unpivots: Seq[String]) = {
      val df = data.select((primaryKeys.map(_._1) ++ unpivots).map(data.col _) : _*).distinct()
      logger.info("processing unpivot " + column2 + " from " + unpivots.mkString("[",",","]"))

      def toDense(selections : Seq[Any]) : Seq[Any] =
        unpivots.zip(selections).filter{
          case (_, selection) => selection == "1" || selection == 1
        }.map(_._1)

      val schema = StructType(
        primaryKeys.map{
          case (_, prikeyHeal, dataType) => StructField(prikeyHeal, convert(dataType), true)
        } :+ StructField(column2, convert(column2Type), true)
      )

      spark.createDataFrame(df.rdd.flatMap(r => {
        val prikeyvals = primaryKeys.map(_._1).map(prikey => r.get(r.fieldIndex(prikey)))
        val unpivotvals = unpivots.map {
          fieldname_phase1 => r.get(r.fieldIndex(fieldname_phase1))
        }
        val dense = toDense(unpivotvals)
        dense.map(selection =>
          Row.fromSeq(primaryKeys.map(key => r.get(r.fieldIndex(key._1))) :+ selection))
      }), schema)

    }

    columnToUnpivotTablesMap.foreach {
      case (table, column0, column2, column2Type, columnsToUnpivot) =>
        val file = s"${config.outputDir}/tables/${table}"
        logger.info("processing column to unpivot table " + table + ", column " + column2)
        logger.info("unpivoting columns " + columnsToUnpivot.mkString("[", ",", "]"))
        val pks = pkMap(table).filter(x => x._1 != "n/a" && x._1 != column0)
        val df = extractColumnToUnpivotTable(pks, column2, column2Type, columnsToUnpivot)
        val df2 = df.join(tableMap(table), pks.map(_._2))
        if(config.verbose)
          logger.info("joining " + tableMap(table).count() + " rows to " + df.count() + " rows on " + pks + ". The result has " + df2.count() + " rows ")
        tableMap(table) = df2
    }
    columnsToUnpivot
  }

  def main(args : Array[String]) {
    OParser.parse(parser, args, Config2()) match {
      case Some(config) =>
        val spark = SparkSession.builder.appName("Transform").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        val hc = spark.sparkContext.hadoopConfiguration

        import spark.implicits._

        val mapping = spark.read.format("csv").option("header", true).option("mode", "FAILFAST").load(config.mappingInputFile).filter($"InitializeField" === "yes").select($"Fieldname_HEAL", $"Fieldname_phase1", $"Data Type", $"Table_HEAL", $"Primary")

        val dataDict = spark.read.format("json").option("multiline", true).option("mode", "FAILFAST").load(config.dataDictInputFile)

        val (data0, negdata) = readData(spark, config, mapping)
        var data = data0

        writeDataframe(hc, config.outputDir + "/filtered", negdata, header = true)
        val dataCols = data.columns.toSeq

        data = generateID(spark, config, mapping, data)

        val tableMap = Map[String, DataFrame]()
        val columnsToCopy = copy(spark, config, tableMap, mapping, data)
        val columnsToUnpivot = collect(spark, config, tableMap, mapping, data)

        val (unknown, missing) = diff(spark, mapping, columnsToCopy ++ columnsToUnpivot)
        writeDataframe(hc, config.outputDir + "/unknown", unknown)
        writeDataframe(hc, config.outputDir + "/missing", missing)

        // data.cache()
        tableMap.foreach {
          case (table, df) =>
            val file = s"${config.outputDir}/tables/${table}"
            writeDataframe(hc, file, df, header = true)
        }

        val file2 = s"${config.outputDir}/tables/reviewer_organization"
        val extendColumnPrefix = "reviewer_name_"
        val reviewerOrganizationColumns = dataCols.filter(column => column.startsWith(extendColumnPrefix))
        logger.info("processing table reviewer_organization extending columns " + reviewerOrganizationColumns)
        val df = reviewerOrganizationColumns.map(reviewOrganizationColumn => {
          val reviewers = data.select(data.col(reviewOrganizationColumn).as("reviewer")).filter($"reviewer" =!= "").distinct
          val organization = reviewOrganizationColumn.drop(extendColumnPrefix.length)
          reviewers.withColumn("organization", lit(organization))
        }).reduce(_ union _)
        writeDataframe(hc, file2, df, header = true)

        val file3 = s"${config.outputDir}/tables/name"
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

        val df2 = spark.createDataFrame(ddrdd, StructType(Seq(
          StructField("field_name", StringType, true),
          StructField("table", StringType, true),
          StructField("column", StringType, true),
          StructField("index", StringType, true),
          StructField("description", StringType, true)
        ))).union(ctsa).withColumn("id", concat($"field_name", lit("___"), $"index")).drop("field_name")
        writeDataframe(hc, file3, df2, header = true)

        spark.stop()
        
      case None =>
    }

  }
}
