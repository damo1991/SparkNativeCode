package com.bitwise.nativeSpark

import org.apache.spark.sql.SparkSession

/**
  * Created by vaijnathp on 1/17/2017.
  */
object P50Fields_Complex_20Gb_HiveInputAndOutputTable {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession

    val df=spark.sql("select * from databaseDemo.tableBhsDemo")

    df.createOrReplaceTempView("tempTable")

    spark.sql("CREATE TABLE IF NOT EXISTS databaseDemo.tableHydrographDemo (applyingFor STRING,apptype STRING,bookletType STRING,givennameapplicant STRING,surnameapplicant STRING,gender STRING,aliases BOOLEAN,checkPrevName BOOLEAN,dt_dob STRING,validityRequired BOOLEAN,checkPOBOutsideIndia BOOLEAN,POB STRING,country STRING,state STRING,stateProvince STRING,maritalStatus STRING,citizenshipOfIndiaBy STRING,PAN BIGINT,voteId BIGINT,employmentType STRING,organizationName STRING,parentSpouseGovtEmp BOOLEAN,educationalQualification STRING,nonECR BOOLEAN,aadhaarNumber BIGINT,fatherGuardianFilePassportNumber DECIMAL(7,3),fatherGuardiannationalityNotIndian BOOLEAN,motherGuardianFilePassportNumber BIGINT,motherGuardiannationalityNotIndian BOOLEAN,checkTempVisit BOOLEAN,residingSinceMonth STRING,residingSinceYear INT,statePresentAdd STRING,pin BIGINT,mobileNumber BIGINT,telephoneNumber BIGINT,checkDiplomaaticOfficial BOOLEAN,tf_DiplOfficialpassportNumber BIGINT,dDissueDate STRING,dDExpiryDate STRING,checkappliedPP BOOLEAN,fileNumber DECIMAL(9,5),monthofApplying STRING,yearofApplying INT,checkCriminal BOOLEAN,checkOffence5years BOOLEAN,checkPassportRefused BOOLEAN,checkImpoundedRevoked BOOLEAN,tf_passportNumber_revoke BIGINT,ecDateofIssue STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("INSERT OVERWRITE TABLE databaseDemo.tableHydrographDemo select applyingFor,apptype,bookletType,givennameapplicant,surnameapplicant,gender,aliases,checkPrevName,dt_dob,validityRequired,checkPOBOutsideIndia,POB,country,state,stateProvince,maritalStatus,citizenshipOfIndiaBy,PAN,voteId,employmentType,organizationName,parentSpouseGovtEmp,educationalQualification,nonECR,aadhaarNumber,fatherGuardianFilePassportNumber,fatherGuardiannationalityNotIndian,motherGuardianFilePassportNumber,motherGuardiannationalityNotIndian,checkTempVisit,residingSinceMonth,residingSinceYear,statePresentAdd,pin,mobileNumber,telephoneNumber,checkDiplomaaticOfficial,tf_DiplOfficialpassportNumber,dDissueDate,dDExpiryDate,checkappliedPP,fileNumber,monthofApplying,yearofApplying,checkCriminal,checkOffence5years,checkPassportRefused,checkImpoundedRevoked,tf_passportNumber_revoke,ecDateofIssue from tempTable")





  }

  def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("P50Fields_Complex_20Gb_HiveInputAndOutputTable").master("local")
      .config("spark.sql.warehouse.dir", "file:///tmp")
      .config("spark.sql.shuffle.partitions", 200)
        .enableHiveSupport()
      .getOrCreate()
  }


}

