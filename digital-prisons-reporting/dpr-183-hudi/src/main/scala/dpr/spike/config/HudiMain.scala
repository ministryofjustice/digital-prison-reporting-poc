package dpr.spike.config

import dpr.spike.service.SourceReferenceService
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object HudiMain {

  private val json_str = """{
    "offender_book_id": 92
    ,
    "booking_begin_date": "2008-09-09"
    ,
    "booking_end_date": null
    ,
    "booking_no": "9964"
    ,
    "offender_id": 8
    ,
    "agy_loc_id": "7"
    ,
    "living_unit_id": 7
    ,
    "disclosure_flag": null
    ,
    "in_out_status": "IN"
    ,
    "active_flag": "TRUE"
    ,
    "booking_status": "BOOKING"
    ,
    "youth_adult_code": "1"
    ,
    "finger_printed_staff_id": 7
    ,
    "search_staff_id": 9
    ,
    "photo_taking_staff_id": 8
    ,
    "assigned_staff_id": 4
    ,
    "create_agy_loc_id": "9"
    ,
    "booking_type": "0"
    ,
    "booking_created_date": "2008-09-09"
    ,
    "root_offender_id": null
    ,
    "agency_iml_id": null
    ,
    "service_fee_flag": null
    ,
    "earned_credit_level": null
    ,
    "ekstrand_credit_level": null
    ,
    "intake_agy_loc_id": null
    ,
    "activity_date": null
    ,
    "intake_caseload_id": null
    ,
    "intake_user_id": null
    ,
    "case_officer_id": null
    ,
    "case_date": null
    ,
    "case_time": null
    ,
    "community_active_flag": null
    ,
    "create_intake_agy_loc_id": null
    ,
    "comm_staff_id": null
    ,
    "comm_status": null
    ,
    "community_agy_loc_id": null
    ,
    "no_comm_agy_loc_id": null
    ,
    "comm_staff_role": null
    ,
    "agy_loc_id_list": null
    ,
    "status_reason": null
    ,
    "total_unexcused_absences": null
    ,
    "request_name": null
    ,
    "create_datetime": "2022-09-01T18:21:36.915241Z"
    ,
    "create_user_id": "6086"
    ,
    "modify_datetime": "2022-09-01T18:21:36.915241Z"
    ,
    "modify_user_id": null
    ,
    "record_user_id": null
    ,
    "intake_agy_loc_assign_date": null
    ,
    "audit_timestamp": "2022-09-01T18:21:36.915241Z"
    ,
    "audit_user_id": null
    ,
    "audit_module_name": null
    ,
    "audit_client_user_id": null
    ,
    "audit_client_ip_address": null
    ,
    "audit_client_workstation_name": null
    ,
    "audit_additional_info": null
    ,
    "booking_seq": 92
    ,
    "admission_reason": "naughty"
  }"""

//  private val json_data = """{"offender_book_id": 91, "booking_begin_date": "2008-10-20", "booking_end_date": null, "booking_no": "8729", "offender_id": 7,"agy_loc_id": "6","living_unit_id": 3,"disclosure_flag": null,"in_out_status": "IN","active_flag": "TRUE","booking_status": "BOOKING","youth_adult_code": "1","finger_printed_staff_id": 3,"search_staff_id": 2,"photo_taking_staff_id": 8,"assigned_staff_id": 2,"create_agy_loc_id": "1","booking_type": "1","booking_created_date": "2008-10-20","root_offender_id": null,"agency_iml_id": null,"service_fee_flag": null,"earned_credit_level": null,"ekstrand_credit_level": null,"intake_agy_loc_id": null,"activity_date": null,"intake_caseload_id": null,"intake_user_id": null,"case_officer_id": null,"case_date": null,"case_time": null,"community_active_flag": null,"create_intake_agy_loc_id": null,"comm_staff_id": null,"comm_status": null,"community_agy_loc_id": null,"no_comm_agy_loc_id": null,"comm_staff_role": null,"agy_loc_id_list": null,"status_reason": null,"total_unexcused_absences": null,"request_name": null,"create_datetime": "2022-09-01T18:21:36.915241Z","create_user_id": "7603","modify_datetime": "2022-09-01T18:21:36.915241Z","modify_user_id": null,"record_user_id": null,"intake_agy_loc_assign_date": null,"audit_timestamp": "2022-09-01T18:21:36.915241Z","audit_user_id": null,"audit_module_name": null,"audit_client_user_id": null,"audit_client_ip_address": null,"audit_client_workstation_name": null,"audit_additional_info": null,"booking_seq": 91, "admission_reason": "naughty" }"""

  val json_data: String = """{
                      "offender_book_id": 91,
                      "booking_begin_date": "2008-10-20",
                      "booking_end_date": null,
                      "booking_no": "8729",
                      "offender_id": 7,
                      "agy_loc_id": "6",
                      "living_unit_id": 3,
                      "disclosure_flag": null,
                      "in_out_status": "IN",
                      "active_flag": "TRUE",
                      "booking_status": "BOOKING",
                      "youth_adult_code": "1",
                      "finger_printed_staff_id": 3,
                      "search_staff_id": 2,
                      "photo_taking_staff_id": 8,
                      "assigned_staff_id": 2,
                      "create_agy_loc_id": "1",
                      "booking_type": "1",
                      "booking_created_date": "2008-10-20",
                      "root_offender_id": null,
                      "agency_iml_id": null,
                      "service_fee_flag": null,
                      "earned_credit_level": null,
                      "ekstrand_credit_level": null,
                      "intake_agy_loc_id": null,
                      "activity_date": null,
                      "intake_caseload_id": null,
                      "intake_user_id": null,
                      "case_officer_id": null,
                      "case_date": null,
                      "case_time": null,
                      "community_active_flag": null,
                      "create_intake_agy_loc_id": null,
                      "comm_staff_id": null,
                      "comm_status": null,
                      "community_agy_loc_id": null,
                      "no_comm_agy_loc_id": null,
                      "comm_staff_role": null,
                      "agy_loc_id_list": null,
                      "status_reason": null,
                      "total_unexcused_absences": null,
                      "request_name": null,
                      "create_datetime": "2022-09-01T18:21:36.915241Z",
                      "create_user_id": "7603",
                      "modify_datetime": "2022-09-01T18:21:36.915241Z",
                      "modify_user_id": null,
                      "record_user_id": null,
                      "intake_agy_loc_assign_date": null,
                      "audit_timestamp": "2022-09-01T18:21:36.915241Z",
                      "audit_user_id": null,
                      "audit_module_name": null,
                      "audit_client_user_id": null,
                      "audit_client_ip_address": null,
                      "audit_client_workstation_name": null,
                      "audit_additional_info": null,
                      "booking_seq": 91,
                      "admission_reason": "naughty"
                    }""".stripMargin
  // Main Method
  def main(args: Array[String]): Unit = {

    // prints Hello World
    println("Hello World!")

    val spark = SparkSession.builder.appName("TestApp")
      .master("local")
//      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.hive.convertMetastoreParquet", value = false)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      .getOrCreate()


//    spark.conf.set(
//      "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    import spark.implicits._
//    val df_1 = spark.read.json(Seq(json_str).toDS())
    val d_seq = Seq((1, json_str))
    val df_1 = d_seq.toDF("id","value")
//    val df_1 = spark.range(0, 5)


    SourceReferenceService.init()
    val schema = "public"
    val table = "offender_bookings"
//    val payloadSchema = SourceReferenceService.getSchema(schema + "." + table)
//    println(payloadSchema)
//    df_value.printSchema()
//    val df_1 = json_data.toDF
//    val jsonSchema = df_1.select(schema_of_json(df_1.select(col("value"))
//      .first.getString(0))).as[String].first
//    val jsonSchema = schema_of_json(lit(df_1.select(col("value")).as[String].first))
//    val jsonSchema: String = df_1.select(schema_of_json(df_1.select(col("value")))
//    val jsonSchema: String = df_1.select(schema_of_json(col("value"))).collect()(0)(0).toString
//    println(jsonSchema)
//    println(DataType.fromDDL(jsonSchema))
//    val temp_df = df_1
//      .withColumn("parsed", from_json(col("value"), DataType.fromDDL(jsonSchema)))
//      .withColumn("parsed", from_json(col("value"), jsonSchema, Map[String, String]().asJava))
      //.withColumn("parsed", from_json(col("value"), payloadSchema))
//      .drop("value")
//    temp_df.show(false)

//    val df_2 = Seq(
//      ("{\"id\":1,\"amount\":{\"value\":60,\"currency\":\"BRL\"}}"),
//      ("{\"id\":2,\"amount\":{\"value\":70,\"currency\":\"USD\"}}")
//    ).toDF("my_json_col")
//    val schema_test = StructType(Seq(
//      StructField("id", IntegerType),
//      StructField("amount", StructType(Seq(
//        StructField("value", IntegerType),
//        StructField("currency", StringType))))
//    ))
//    val parsedDf = df_2.withColumn("parsed", from_json(col("my_json_col"), schema_test))
//    parsedDf.show(false)

    val schema_test = StructType(Seq(
      StructField("offender_book_id", LongType, nullable = false),
      StructField("booking_begin_date", StringType, nullable = true),
      StructField("booking_end_date", StringType, nullable = true),
      StructField("booking_no", StringType, nullable = true),
      StructField("offender_id", LongType, nullable = true),
      StructField("agy_loc_id", StringType, nullable = true),
      StructField("living_unit_id", LongType, nullable = true),
      StructField("disclosure_flag", StringType, nullable = true),
      StructField("in_out_status", StringType, nullable = true),
      StructField("active_flag", StringType, nullable = true),
      StructField("booking_status", StringType, nullable = true),
      StructField("youth_adult_code", StringType, nullable = true),
      StructField("finger_printed_staff_id", LongType, nullable = true),
      StructField("search_staff_id", LongType, nullable = true),
      StructField("photo_taking_staff_id", LongType, nullable = true),
      StructField("assigned_staff_id", LongType, nullable = true),
      StructField("booking_type", StringType, nullable = true),
      StructField("booking_created_date", StringType, nullable = true),
      StructField("root_offender_id", LongType, nullable = true),
      StructField("agency_iml_id", LongType, nullable = true),
      StructField("service_fee_flag", StringType, nullable = true),
      StructField("earned_credit_level", StringType, nullable = true),
      StructField("ekstrand_credit_level", StringType, nullable = true),
      StructField("intake_agy_loc_id", StringType, nullable = true),
      StructField("activity_date", StringType, nullable = true),
      StructField("intake_caseload_id", StringType, nullable = true),
      StructField("intake_user_id", StringType, nullable = true),
      StructField("case_officer_id", LongType, nullable = true),
      StructField("case_date", StringType, nullable = true),
      StructField("case_time", StringType, nullable = true),
      StructField("community_active_flag", StringType, nullable = true),
      StructField("create_intake_agy_loc_id", StringType, nullable = true),
      StructField("comm_staff_id", LongType, nullable = true),
      StructField("comm_status", StringType, nullable = true),
      StructField("community_agy_loc_id", StringType, nullable = true),
      StructField("no_comm_agy_loc_id", LongType, nullable = true),
      StructField("comm_staff_role", StringType, nullable = true),
      StructField("agy_loc_id_list", StringType, nullable = true),
      StructField("status_reason", StringType, nullable = true),
      StructField("total_unexcused_absences", LongType, nullable = true),
      StructField("request_name", StringType, nullable = true),
      StructField("create_datetime", StringType, nullable = true),
      StructField("create_user_id", StringType, nullable = true),
      StructField("modify_datetime", StringType, nullable = true),
      StructField("modify_user_id", StringType, nullable = true),
      StructField("record_user_id", StringType, nullable = true),
      StructField("intake_agy_loc_assign_date", StringType, nullable = true),
      StructField("audit_timestamp", StringType, nullable = true),
      StructField("audit_user_id", StringType, nullable = true),
      StructField("audit_module_name", StringType, nullable = true),
      StructField("audit_client_user_id", StringType, nullable = true),
      StructField("audit_client_ip_address", StringType, nullable = true),
      StructField("audit_client_workstation_name", StringType, nullable = true),
      StructField("audit_additional_info", StringType, nullable = true),
      StructField("booking_seq", LongType, nullable = true),
      StructField("admission_reason", StringType, nullable = true)
    ))

    val data = df_1
        .withColumn("parsed", from_json(col("value"), schema_test))
        .drop("value")

    val clientOpts = Map(
      "hoodie.table.name" -> "offender_bookings",
      "hoodie.table.type" -> "MERGE_ON_WRITE",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.datasource.write.recordkey.field" -> "offender_book_id",
      //          "hoodie.datasource.write.precombine.field" -> "ts",
//      "hoodie.datasource.write.partitionpath.field" -> "offender_book_id",
//      "hoodie.datasource.write.hive_style_partitioning" -> "false",
      "hoodie.upsert.shuffle.parallelism" -> "8",
      "hoodie.insert.shuffle.parallelism" -> "8",
      "hoodie.delete.shuffle.parallelism" -> "8",
      "hoodie.compact.inline" -> "true",
      "hoodie.compact.inline.max.delta.commits" -> "10"
    )

    df_1.write.format("org.apache.hudi")
      .options(clientOpts)
      .mode("overwrite")
      .save("/tmp/hudi-table")

    val df_read = spark.read.format("delta").load("/tmp/delta-table")
    df_read.show()
  }
}
