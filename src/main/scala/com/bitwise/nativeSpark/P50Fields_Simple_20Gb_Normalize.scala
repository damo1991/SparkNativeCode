package com.bitwise.nativeSpark

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by shivarajn on 1/30/2017.
  */
object P50Fields_Simple_20Gb_Normalize {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("P50Fields_Simple_20Gb_Normalize").master("yarn")
      .config("spark.sql.warehouse.dir", "file:///tmp")
      .config("spark.sql.shuffle.partitions", 200)
      .getOrCreate()

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

    val readFile = spark.read
      .option("delimiter", ",")
      .option("header", false)
      .option("charset", "ISO-8859-1")
      .option("dateFormat", "yyyy/MM/dd")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .schema(schema)
      .csv("testData/benchmarking/input/20Gb_50Fields")

    readFile.createOrReplaceTempView("Normalise")

    val df = spark.sql("From Normalise SELECT applyingFor,apptype,bookletType,givennameapplicant,surnameapplicant,gender,alias,checkPrevName,dt_dob,validityRequired,checkPOBOutsideIndia,POB,country,state,stateProvince,maritalStatus,citizenshipOfIndiaBy,PAN,voteId,employmentType,organizationName,parentSpouseGovtEmp,explode(split(educationalQualification,'And')) as educationalQualifications,nonECR,aadhaarNumber,fatherGuardianFilePassportNumber,fatherGuardiannationalityNotIndian,motherGuardianFilePassportNumber,motherGuardiannationalityNotIndian,checkTempVisit,residingSinceMonth,residingSinceYear,statePresentAdd,pin,mobileNumber,telephoneNumber,checkDiplomaaticOfficial,tf_DiplOfficialpassportNumber,dDissueDate,dDExpiryDate,checkappliedPP,fileNumber,monthofApplying,yearofApplying,checkCriminal,checkOffence5years,checkPassportRefused,checkImpoundedRevoked,tf_passportNumber_revoke,ecDateofIssue")

    df.write
      .option("delimiter", ",")
      .option("header", false)
      .option("charset", "ISO-8859-1")
      .option("dateFormat","yyyy/MM/dd")
      .option("timestampFormat","yyyy/MM/dd hh:mm:ss")
      .mode(SaveMode.Overwrite)
      .csv("testData/benchmarking/output/50Fields_Simple_20Gb_Normalize")
  }

}
