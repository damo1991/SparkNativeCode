package com.bitwise.nativeSpark

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by vaijnathp on 1/17/2017.
  */
object P50Fields_Simple_20Gb_Filter {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession

    val df = spark.read.option("dateFormat","yyyy/MM/dd")
      .option("timestampFormat","yyyy/MM/dd hh:mm:ss")
      .schema(getSchema).csv("testData/benchmarking/input/20Gb_50Fields")

    df.createOrReplaceTempView("DataFrame")

    val used = spark.sql("SELECT  * FROM DataFrame WHERE applyingFor = 'Fresh' AND apptype='Tatkal' AND bookletType='60Pages'")
    val unused = spark.sql("SELECT  * FROM DataFrame WHERE applyingFor != 'Fresh' or apptype!='Tatkal' or bookletType!='60Pages'")

    used.write
      .option("timestampFormat","yyyy/MM/dd hh:mm:ss")
      .option("dateFormat","yyyy/MM/dd")
      .mode(SaveMode.Overwrite).csv("testData/benchmarking/output/50Fields_Simple_20Gb_Filter_out")
    unused.write
      .option("timestampFormat","yyyy/MM/dd hh:mm:ss")
      .option("dateFormat","yyyy/MM/dd")
      .mode(SaveMode.Overwrite).csv("testData/benchmarking/output/50Fields_Simple_20Gb_Filter_unused")
  }

  def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("50Fields_Simple_20Gb_Filter").master("yarn")
      .config("spark.sql.warehouse.dir", "file:///tmp")
      .config("spark.sql.shuffle.partitions", 200)
      .getOrCreate()
  }

  def getSchema: StructType = {
    StructType(List(
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
  }
}

