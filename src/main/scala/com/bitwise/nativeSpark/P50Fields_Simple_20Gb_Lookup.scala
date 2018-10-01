package com.bitwise.nativeSpark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
/**
  * Created by AniruddhaS on 1/18/2017.
  */
object P50Fields_Simple_20Gb_Lookup {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("P50Fields_Simple_20Gb_Lookup").master("yarn")
      .config("sparkSession.sql.warehouse.dir", "file:///tmp")
      .config("sparkSession.sql.shuffle.partitions",200)
      .getOrCreate()


    val inputSchema = StructType(List(
      StructField("applyingFor", StringType, nullable = true),
      StructField("apptype", StringType, nullable = true),
      StructField("bookletType", StringType, nullable = true),
      StructField("givennameapplicant", StringType, nullable = true),
      StructField("surnameapplicant", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("alias", BooleanType, nullable = true),
      StructField("checkPrevName", BooleanType, nullable = true),
      StructField("dt_dob", DateType ,nullable = true),
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
      StructField("fatherGuardianFilePassportNumber",DataTypes.createDecimalType(7,3), nullable = true),
      StructField("fatherGuardiannationalityNotIndian", BooleanType, nullable = true),
      StructField("motherGuardianFilePassportNumber", StringType, nullable = true),
      StructField("motherGuardiannationalityNotIndian", StringType, nullable = true),
      StructField("checkTempVisit", BooleanType, nullable = true),
      StructField("residingSinceMonth", StringType, nullable = true),
      StructField("residingSinceYear", StringType, nullable = true),
      StructField("statePresentAdd", StringType, nullable = true),
      StructField("pin", LongType, nullable = true),
      StructField("mobileNumber", LongType, nullable = true),
      StructField("telephoneNumber", StringType, nullable = true),
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
      StructField("checkImpoundedRevoked",BooleanType, nullable = true),
      StructField("tf_passportNumber_revoke", LongType, nullable = true),
      StructField("ecDateofIssue", DateType, nullable = true)
    ))
    val lookupSchema = StructType(List(
      StructField("givennameapplicant", StringType, nullable = true),
      StructField("surnameapplicant", StringType, nullable = true),
      StructField("alias1givenname", StringType, nullable = true),
      StructField("PAN", LongType, nullable = true),
      StructField("organizationName", StringType, nullable = true),
      StructField("alias1Surname", StringType, nullable = true),
      StructField("alias2givenname", StringType, nullable = true),
      StructField("prev1GivenName", StringType, nullable = true),
      StructField("prev1Surname", StringType, nullable = true),
      StructField("prev2GivenName", StringType, nullable = true),
      StructField("prev2Surname", StringType, nullable = true),
      StructField("districtPOB", StringType, nullable = true),
      StructField("districtOthers", StringType, nullable = true),
      StructField("visibleDistinguishingMark", StringType, nullable = true),
      StructField("fatherGivenName", StringType, nullable = true),
      StructField("fatherSurname", StringType, nullable = true),
      StructField("legalGuardianGivenName", StringType, nullable = true),
      StructField("legalGuardianSurName", StringType, nullable = true),
      StructField("motherGivenName", StringType, nullable = true),
      StructField("motherSurname", StringType, nullable = true),
      StructField("spouseGivenName", StringType, nullable = true),
      StructField("spouseSurname", StringType, nullable = true),
      StructField("alias2Surname", StringType, nullable = true),
      StructField("houseNoStreetName", StringType, nullable = true)
    ))

    val readFile1 = sparkSession.read
      .option("delimiter", ",")
      .option("header", false)
      .option("charset", "ISO-8859-1")
      .option("dateFormat","yyyy/MM/dd")
      .option("timestampFormat","yyyy/MM/dd hh:mm:ss")
      .schema(inputSchema)
      .csv("testData/benchmarking/input/20Gb_50Fields")

    val readFile2 = sparkSession.read
      .option("delimiter", ",")
      .option("header", false)
      .option("charset", "ISO-8859-1")
      .schema(lookupSchema)
      .csv("testData/benchmarking/input/1.5Gb_Schema1.txt")

    val otherFields: Array[Column] = readFile2.columns.filter(col => !col.equalsIgnoreCase("PAN")).map(str => first(str).as(str))
    val broadCastDF = broadcast(readFile2.groupBy("PAN").agg(lit(null),otherFields:_*))


    readFile1.createOrReplaceTempView("inputtable0");
    broadCastDF.createOrReplaceTempView("inputtable1");

    val joinquery = sparkSession
      .sql("SELECT in0.PAN AS new_pan,in0.applyingFor,in0.apptype,in0.bookletType,in0.givennameapplicant,in0.gender,in0.alias,in0.checkPrevName,in0.dt_dob,in0.validityRequired,in0.checkPOBOutsideIndia,in0.POB,in0.country,in0.state,in0.stateProvince,in0.maritalStatus,in0.citizenshipOfIndiaBy,in0.voteId,in0.employmentType,in0.parentSpouseGovtEmp,in0.educationalQualification,in0.nonECR,in0.aadhaarNumber,in0.fatherGuardianFilePassportNumber,in0.fatherGuardiannationalityNotIndian,in0.motherGuardianFilePassportNumber,in0.motherGuardiannationalityNotIndian,in0.checkTempVisit,in0.residingSinceMonth,in0.residingSinceYear,in0.statePresentAdd,in0.pin,in0.mobileNumber,in0.telephoneNumber,in0.checkDiplomaaticOfficial,in0.tf_DiplOfficialpassportNumber,in0.dDissueDate,in0.dDExpiryDate,in0.checkappliedPP,in0.fileNumber,in0.monthofApplying,in0.yearofApplying,in0.checkCriminal,in0.checkOffence5years,in0.checkPassportRefused,in0.checkImpoundedRevoked,in0.tf_passportNumber_revoke,in0.ecDateofIssue,in1.surnameapplicant,in1.organizationName,in1.alias1givenname,in1.alias1Surname,in1.alias2givenname,in1.alias2Surname,in1.prev1GivenName,in1.prev1Surname,in1.prev2GivenName,in1.prev2Surname,in1.districtPOB,in1.districtOthers,in1.visibleDistinguishingMark,in1.fatherGivenName,in1.fatherSurname,in1.legalGuardianGivenName,in1.legalGuardianSurName,in1.motherGivenName,in1.motherSurname,in1.spouseGivenName,in1.spouseSurname,in1.houseNoStreetName FROM inputtable0 in0 LEFT OUTER JOIN inputtable1 in1 ON in0.PAN = in1.PAN")

    val outwardSchema:Array[Column] = Array[Column](
      col("new_pan"),
      col("applyingFor"),
      col("apptype"),
      col("bookletType"),
      col("givennameapplicant"),
      col("surnameapplicant"),
      col("gender"),
      col("alias"),
      col("checkPrevName"),
      col("dt_dob"),
      col("validityRequired"),
      col("checkPOBOutsideIndia"),
      col("POB"),
      col("country"),
      col("state"),
      col("stateProvince"),
      col("maritalStatus"),
      col("citizenshipOfIndiaBy"),
      col("voteId"),
      col("employmentType"),
      col("parentSpouseGovtEmp"),
      col("educationalQualification"),
      col("nonECR"),
      col("aadhaarNumber"),
      col("fatherGuardianFilePassportNumber"),
      col("fatherGuardiannationalityNotIndian"),
      col("motherGuardianFilePassportNumber"),
      col("motherGuardiannationalityNotIndian"),
      col("checkTempVisit"),
      col("residingSinceMonth"),
      col("residingSinceYear"),
      col("statePresentAdd"),
      col("pin"),
      col("mobileNumber"),
      col("telephoneNumber"),
      col("checkDiplomaaticOfficial"),
      col("tf_DiplOfficialpassportNumber"),
      col("dDissueDate"),
      col("dDExpiryDate"),
      col("checkappliedPP"),
      col("fileNumber"),
      col("monthofApplying"),
      col("yearofApplying"),
      col("checkCriminal"),
      col("checkOffence5years"),
      col("checkPassportRefused"),
      col("checkImpoundedRevoked"),
      col("tf_passportNumber_revoke"),
      col("ecDateofIssue"),
      col("alias1givenname"),
      col("alias1Surname"),
      col("alias2givenname"),
      col("alias2Surname"),
      col("prev1GivenName"),
      col("prev1Surname"),
      col("prev2GivenName"),
      col("prev2Surname"),
      col("districtPOB"),
      col("districtOthers"),
      col("visibleDistinguishingMark"),
      col("fatherGivenName"),
      col("fatherSurname"),
      col("legalGuardianGivenName"),
      col("legalGuardianSurName"),
      col("motherGivenName"),
      col("motherSurname"),
      col("spouseGivenName"),
      col("spouseSurname"),
      col("houseNoStreetName")
    )


    joinquery.select(outwardSchema: _*).write
      .option("delimiter", ",")
      .option("header", false)
      .option("charset", "ISO-8859-1")
      .option("dateFormat","yyyy/MM/dd")
      .option("timestampFormat","yyyy/MM/dd HH:mm:ss")
      .mode(SaveMode.Overwrite)
      .csv("testData/benchmarking/output/50Fields_Simple_20Gb_Lookup")
  }
}
