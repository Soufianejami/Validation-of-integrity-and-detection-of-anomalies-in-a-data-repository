package com.example
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.anomalydetection.RelativeRateOfChangeStrategy
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.google.common.io.Files
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

import java.io.File





object anomaly_detection {
  def main(args: Array[String]){
    val (spark1, df1, collectionName1) = datacore.client_p_a_a(args)
    val (spark2, df2, collectionName2) = datacore.client_b_a_a(args)
    val (spark3, df3, collectionName3) = datacore.client_c_a_a(args)
    val (spark4, df4, collectionName4) = datacore.client_c_a_a(args)


    val a = (spark1, df1, collectionName1)
    val b = (spark2, df2, collectionName2)
    val c = (spark3, df3, collectionName3)
    val d = (spark4, df4, collectionName4)

    // A json file in which the computed metrics will be stored
    val metricsFile = new File(Files.createTempDir(), "metrics.json")

    // The repository which we will use to stored and load computed metrics; we use the local disk,
    val repository: MetricsRepository =
    FileSystemMetricsRepository(spark1, metricsFile.getAbsolutePath)




    // The key under which we store the results, needs a timestamp and supports arbitrary
    // tags in the form of key-value pairs
    var metricsDf1 = spark1.createDataFrame(Seq.empty[(String, String, String, String, String, String)]).toDF("constraint", "constraint_obj", "message", "status", "metric", "collectionname")

    for ((table, collectionName) <- Seq((a, collectionName1), (b, collectionName2), (c, collectionName3))) {
      val (sparkTable, df, _) = table
      val resultKey1 = ResultKey(System.currentTimeMillis(), Map("collection Name" -> collectionName))


      val yesterdaysKey = ResultKey(System.currentTimeMillis() - 24 * 60 * 1000, Map("collection Name" -> collectionName))
      VerificationSuite()
        .onData(df4)
        .useRepository(repository)
        .saveOrAppendResult(yesterdaysKey)
        .addAnomalyCheck(
          RelativeRateOfChangeStrategy(maxRateIncrease = Some(3.0)),
          Size()
        )
        .run()

      val verificationResult = VerificationSuite()
        .onData(df)
        .useRepository(repository)
        .saveOrAppendResult(resultKey1)
        .addAnomalyCheck(
          RelativeRateOfChangeStrategy(maxRateIncrease = Some(3.0)),
          Size()
        )
        .run()


      val json = repository.load()
        .after(System.currentTimeMillis() - 10000)
        .getSuccessMetricsAsJson()

      println(s"Metrics from the last 10 minutes:\n$json")

      // Load metrics from all collections
      val metricsDf2 = repository.load()
        .withTagValues(Map("collection Name" -> collectionName))
        .getSuccessMetricsAsDataFrame(spark1)
        .withColumn("collectionname", lit(collectionName))

      metricsDf1 = metricsDf1.unionAll(metricsDf2)
    }
    //Anomaly detection
    repository
      .load()
      .forAnalyzers(Seq(Size()))
      .getSuccessMetricsAsDataFrame(spark1)
      .show()

    // Show the combined data frame
    metricsDf1.show()

//     Save the metrics DataFrame to MongoDB
    metricsDf1.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode(SaveMode.Overwrite  )
      .option("uri", s"mongodb+srv://soufianejami123:Fuckoff.10@cluster0.ixzabjx.mongodb.net/result.Anomaly Detection")
      .option("database", "result")
      .option("collection", "Anomaly Detection")
      .save()


  }
}





