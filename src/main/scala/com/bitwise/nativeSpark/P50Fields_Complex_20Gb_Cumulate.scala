package com.bitwise.nativeSpark

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by anshulsh on 1/17/2017.
  */
object P50Fields_Complex_20Gb_Cumulate {

  val tempTable = "tempTable"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("P50Fields_Complex_20Gb_Cumulate").master("yarn")
      .config("spark.sql.warehouse.dir", "file:///tmp")
      .config("spark.sql.shuffle.partitions", 200)
      .getOrCreate()

    import sparkSession.sql


    val schema = StructType(List(
      StructField("applyingFor", StringType, nullable = true),
      StructField("apptype", StringType, nullable = true),
      StructField("bookletType", StringType, nullable = true),
      StructField("givennameapplicant", StringType, nullable = true),
      StructField("surnameapplicant", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("alias", BooleanType, nullable = true),
      StructField("checkPrevName", BooleanType, nullable = true),
      StructField("dt_dob", DateType, nullable = true),
      StructField("validityRequired", BooleanType, nullable = true),
      StructField("checkPOBOutsideIndia", BooleanType, nullable = true),
      StructField("POB", StringType, nullable = true),
      StructField("country", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("stateProvince", StringType, nullable = true),
      StructField("maritalStatus", StringType, nullable = true),
      StructField("citizenshipOfIndiaBy", StringType, nullable = true),
      StructField("PAN", LongType, nullable = true),
      StructField("voteId", LongType, nullable = true),
      StructField("employmentType", StringType, nullable = true),
      StructField("organizationName", StringType, nullable = true),
      StructField("parentSpouseGovtEmp", BooleanType, nullable = true),
      StructField("educationalQualification", StringType, nullable = true),
      StructField("nonECR", BooleanType, nullable = true),
      StructField("aadhaarNumber", LongType, nullable = true),
      StructField("fatherGuardianFilePassportNumber", DataTypes.createDecimalType(7,3), nullable = true),
      StructField("fatherGuardiannationalityNotIndian", BooleanType, nullable = true),
      StructField("motherGuardianFilePassportNumber", LongType, nullable = true),
      StructField("motherGuardiannationalityNotIndian", BooleanType, nullable = true),
      StructField("checkTempVisit", BooleanType, nullable = true),
      StructField("residingSinceMonth", StringType, nullable = true),
      StructField("residingSinceYear", IntegerType, nullable = true),
      StructField("statePresentAdd", StringType, nullable = true),
      StructField("pin", LongType, nullable = true),
      StructField("mobileNumber", LongType, nullable = true),
      StructField("telephoneNumber", LongType, nullable = true),
      StructField("checkDiplomaaticOfficial", BooleanType, nullable = true),
      StructField("tf_DiplOfficialpassportNumber", LongType, nullable = true),
      StructField("dDissueDate", TimestampType, nullable = true),
      StructField("dDExpiryDate", TimestampType, nullable = true),
      StructField("checkappliedPP", BooleanType, nullable = true),
      StructField("fileNumber", DataTypes.createDecimalType(9,5), nullable = true),
      StructField("monthofApplying", StringType, nullable = true),
      StructField("yearofApplying", IntegerType, nullable = true),
      StructField("checkCriminal", BooleanType, nullable = true),
      StructField("checkOffence5years", BooleanType, nullable = true),
      StructField("checkPassportRefused", BooleanType, nullable = true),
      StructField("checkImpoundedRevoked", BooleanType, nullable = true),
      StructField("tf_passportNumber_revoke", LongType, nullable = true),
      StructField("ecDateofIssue", DateType, nullable = true)
    ))


    val readFile = sparkSession.read
      .option("delimiter", ",")
      .option("header", false)
      .option("charset", "ISO-8859-1")
      .schema(schema)
        .option("dateFormat","yyyy/MM/dd")
      .option("timestampFormat","yyyy/MM/dd hh:mm:ss")
      .csv("/user/HemantR/testData/benchmarking/input/20Gb_50Fields/10Gb_50Fields.txt")

    // for First Record
    val df1 = readFile

    df1.createOrReplaceTempView(tempTable)

//    var query=""
//
//    if(args{2}.equalsIgnoreCase("simple"))
//      query="select * , Row_Number() over (partition by applyingFor order by monthofApplying) as count1 from " + tempTable
//    else if (args{2}.equalsIgnoreCase("medium"))
//    query="select * , Row_Number() over (partition by applyingFor order by monthofApplying) as count1,Row_Number() over (partition by applyingFor order by monthofApplying) as count2 from " + tempTable
//    else if (args{2}.equalsIgnoreCase("complex"))
//      query="select * , Row_Number() over (partition by applyingFor order by monthofApplying) as count1,Row_Number() over (partition by applyingFor order by monthofApplying) as count2,sum(pin) over (partition by applyingFor order by pin) as sum from " + tempTable

      val res = sql("select * , Row_Number() over (partition by applyingFor order by pin) as count1,Row_Number() over (partition by applyingFor order by pin) as count2,Row_Number() over (partition by applyingFor order by pin) as count3,Row_Number() over (partition by applyingFor order by pin) as count4,sum(pin) over (partition by applyingFor order by pin) as sum from " + tempTable)



    res.write
      .option("delimiter", ",")
      .option("header", true)
      .option("charset", "ISO-8859-1")
      .option("dateFormat","yyyy/MM/dd")
      .option("timestampFormat","yyyy/MM/dd hh:mm:ss")
      .mode(SaveMode.Overwrite)
      .csv("testData/benchmarking/output/50Fields_Complex_20Gb_Cumulate")




  }

}
