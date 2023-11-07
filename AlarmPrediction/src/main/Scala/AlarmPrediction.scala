import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AlarmPrediction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AlarmPrediction").enableHiveSupport().getOrCreate()
    val df = spark.table("dwd.fact_machine_learning_data")
//      .withColumn("dwd_insert_time_unix",unix_timestamp(col("dwd_insert_time")))

    val indexer = new StringIndexer().setInputCols(
      Array("machine_record_pmc_code"))
      .setOutputCols(Array("machine_record_pmc_code_int"))

    val indexedData = indexer.fit(df).transform(df)
    val encoder = new OneHotEncoder()
      .setInputCols(Array("machine_record_pmc_code_int"))
      .setOutputCols(Array( "machine_record_pmc_code_int_oneHot"))
    val oneHotEncodedData = encoder.fit(indexedData).transform(indexedData)
    oneHotEncodedData.show()
    val featureCols  = oneHotEncodedData.columns.filter(x=>{
      x.!=("machine_record_pmc_code")&&x.!=("machine_record_date_int")&& x.!=("machine_record_date_int_oneHot")&&x.!=("machine_record_date_unix")&&x.!=("machine_record_date")&&x.!=("dwd_insert_user")&&x.!=("dwd_insert_time")&&x.!=("dwd_modify_user")&&x.!=("dwd_modify_time")&&x.!=("machine_record_id")&&x.!=("dwd_modify_time")&&x.!=("machine_record_run_time")&&x.!=("machine_record_feed_speed")&&x.!=("machine_record_mainshaft_multiplerate")
    })

    println(featureCols.mkString(","),"特征列表--------------------------------------------")
    var featureANs=List[(String,Long)]()
    // todo 需要进行分箱，特征唯一值太大
    //(List((machine_id,30), (machine_record_state,1),
    // (machine_record_mainshaft_speed,3000), (machine_record_mainshaft_load,100),
    // (machine_record_feed_multiplerate,1000), (machine_record_circle_time,300),
    // (machine_record_effective_shaft,10), (machine_record_amount_process,200),
    // (machine_record_use_memory,2048), (machine_record_free_memory,1024),
    // (machine_record_amount_use_code,20), (machine_record_amount_free_code,10),
    // (dwd_insert_time_unix,1), (dwd_modify_time_unix,1), (machine_record_date_unix,164128),
    // (machine_record_pmc_code_int,1000), (machine_record_date_int,164128), (machine_record_pmc_code_int_oneHot,1000),
    // (machine_record_date_int_oneHot,164128)),------------------------------------------------)
    // 遍历每个分类特征列，计算唯一值数量
//    for (colName <- featureCols) {
//      val uniqueValuesCount = oneHotEncodedData.select(colName).distinct().count()
//      var t=(colName,uniqueValuesCount)
//      featureANs=featureANs :+t
//    }
//    println(featureANs,"------------------------------------------------")

    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val labeledData = assembler.transform(oneHotEncodedData).select("features", "machine_record_state")
    labeledData.show()
    val Array(train,test): Array[Dataset[Row]] = labeledData.randomSplit(Array(0.7, 0.3), 123)
    val rf = new RandomForestClassifier().setMaxBins(3200).setLabelCol("machine_record_state").setFeaturesCol("features")
    val model = rf.fit(train)
    val predictions = model.transform(test)
    val accuracy = new MulticlassClassificationEvaluator().setLabelCol("machine_record_state").setPredictionCol("prediction").setMetricName("accuracy")
    println(s"Test set accuracy = $accuracy")



  }

}
