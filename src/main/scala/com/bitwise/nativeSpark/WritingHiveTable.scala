package com.bitwise.nativeSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by vaijnathp on 1/17/2017.
  */
object WritingHiveTable {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession

    val df = spark.read
        .option("spark.debug.maxToStringFields","50")
      .option("timestampFormat","yyyy-MM-dd HH:mm:ss")
      .option("dateFormat","yyyy-MM-dd")
      .schema(getSchema).csv("testData/benchmarking/input/20Gb_50Fields_Hive")
//      .schema(getSchema).csv("performanceB/input/input.txt")

    df.createOrReplaceTempView("tempTable")

//    spark.sql("set spark.debug.maxToStringFields")
    spark.sql("CREATE DATABASE IF NOT EXISTS databaseDemo")
    spark.sql("CREATE TABLE IF NOT EXISTS databaseDemo.tableBhsDemo (applyingFor STRING,apptype STRING,bookletType STRING,givennameapplicant STRING,surnameapplicant STRING,gender STRING,aliases BOOLEAN,checkPrevName BOOLEAN,dt_dob STRING,validityRequired BOOLEAN,checkPOBOutsideIndia BOOLEAN,POB STRING,country STRING,state STRING,stateProvince STRING,maritalStatus STRING,citizenshipOfIndiaBy STRING,PAN BIGINT,voteId BIGINT,employmentType STRING,organizationName STRING,parentSpouseGovtEmp BOOLEAN,educationalQualification STRING,nonECR BOOLEAN,aadhaarNumber BIGINT,fatherGuardianFilePassportNumber DECIMAL(7,3),fatherGuardiannationalityNotIndian BOOLEAN,motherGuardianFilePassportNumber BIGINT,motherGuardiannationalityNotIndian BOOLEAN,checkTempVisit BOOLEAN,residingSinceMonth STRING,residingSinceYear INT,statePresentAdd STRING,pin BIGINT,mobileNumber BIGINT,telephoneNumber BIGINT,checkDiplomaaticOfficial BOOLEAN,tf_DiplOfficialpassportNumber BIGINT,dDissueDate STRING,dDExpiryDate STRING,checkappliedPP BOOLEAN,fileNumber DECIMAL(9,5),monthofApplying STRING,yearofApplying INT,checkCriminal BOOLEAN,checkOffence5years BOOLEAN,checkPassportRefused BOOLEAN,checkImpoundedRevoked BOOLEAN,tf_passportNumber_revoke BIGINT,ecDateofIssue STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("INSERT OVERWRITE TABLE databaseDemo.tableBhsDemo select applyingFor,apptype,bookletType,givennameapplicant,surnameapplicant,gender,isalias,checkPrevName,dt_dob,validityRequired,checkPOBOutsideIndia,POB,country,state,stateProvince,maritalStatus,citizenshipOfIndiaBy,PAN,voteId,employmentType,organizationName,parentSpouseGovtEmp,educationalQualification,nonECR,aadhaarNumber,fatherGuardianFilePassportNumber,fatherGuardiannationalityNotIndian,motherGuardianFilePassportNumber,motherGuardiannationalityNotIndian,checkTempVisit,residingSinceMonth,residingSinceYear,statePresentAdd,pin,mobileNumber,telephoneNumber,checkDiplomaaticOfficial,tf_DiplOfficialpassportNumber,dDissueDate,dDExpiryDate,checkappliedPP,fileNumber,monthofApplying,yearofApplying,checkCriminal,checkOffence5years,checkPassportRefused,checkImpoundedRevoked,tf_passportNumber_revoke,ecDateofIssue from tempTable")

//    val used = spark.sql("SELECT  * FROM DataFrame WHERE applyingFor = 'Fresh' AND apptype = 'Tatkal' AND bookletType='60Pages' AND educationalQualification='10thPassAndAbove' AND nonECR = 'True' AND cast(ecDateofIssue as string) > '2008/01/01' AND givennameapplicant RLIKE '^[a|A|k|K|b|B].*' AND surnameapplicant RLIKE '^[a|A|k|c|X|E|v|s|H|K|b|B].*' AND cast(dt_dob as string) < '1990/01/01' AND gender != 'Transgender'")
//    val unused = spark.sql("SELECT  * FROM DataFrame WHERE applyingFor != 'Fresh' OR apptype !='Tatkal' OR bookletType!='60Pages' OR educationalQualification!='10thPassAndAbove' OR nonECR != 'True' OR cast(ecDateofIssue as string) < '2008/01/01' OR givennameapplicant RLIKE '^(?![a|A|k|K|b|B]).*' OR surnameapplicant RLIKE '^(?![a|A|k|c|X|E|v|s|H|K|b|B]).*' OR cast(dt_dob as string) > '1990/01/01' OR gender = 'Transgender' ")
//    val query="SELECT  * FROM DataFrame WHERE  dt_dob < cast('"+new SimpleDateFormat("1990/01/01")+"' as string) "
//    val used = spark.sql(query)
//    val unused = spark.sql("SELECT  * FROM DataFrame WHERE cast(dt_dob as string) > '1990/01/01'")



  }

  def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("WritingHiveTable").master("local")
      .config("spark.sql.warehouse.dir", "file:///tmp")
      .config("spark.sql.shuffle.partitions", 200)
        .enableHiveSupport()
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
      StructField("isalias", BooleanType, nullable = true),
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

