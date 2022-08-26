package main.com.mendix.assignment

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object exportgitcommits extends Logging {

  def main(args: Array[String]): Unit = {

    val DATEFORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss")

    val JOB_START_TIME: String = LocalDateTime.now().format(DATEFORMATTER)

    logInfo(s"Job Start time is: $JOB_START_TIME")

    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .config("spark.master", "local[2]")
        .getOrCreate()
    )

    // reading the input data from hdfs path
    logInfo("reading the input messages")
    val inputCommitsDF: DataFrame = sparkSession.read.option("multiline", "true").option("mode", "permissive").option("mode", "dropmalformed").json("/inputpath/git-git_commit_log_iso.json")

    val transformDf: DataFrame = inputCommitsDF
      .withColumn("commit_date_ts",col("created_at"))
      .withColumn("commit_date",col("created_at").substr(1,10))
      .withColumn("commit_event",col("event"))
      .select("commit_id", "commit_date_ts", "commit_date", "commit_event")
      .filter(col("commit_id").isNotNull)
      .dropDuplicates("commit_id")

    logInfo("writing the input data to hdfs path")
    transformDf.write.format("orc").mode(SaveMode.Append).save("outputPath")


    val JOB_END_TIME: String = LocalDateTime.now().format(DATEFORMATTER)

    logInfo(s"Job Start time is: $JOB_END_TIME")

  }
}
