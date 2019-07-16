package main

import java.io.File

import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}
import spark.SparkUtils

import scala.collection.mutable

object Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf = SparkUtils.initSparkConf
    val sc = SparkUtils.initSparkContext(sparkConf.set("spark.hadoop.validateOutputSpecs", "false"))


    sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<Corps_PRM>")
    sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</Corps_PRM>")

    val xmlFile = "C:\\Users\\abhishek.ravi\\Desktop\\abhishek\\TransformBigXML\\ENEDIS_R17_17X100A100F0019Z_GRD-F003_02209_00001_00001.xml"
    val records = sc.newAPIHadoopFile(
      xmlFile,
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text]
    )

    println(records.getNumPartitions)

    val params = new mutable.HashMap[String, String]()

    params.put("fichier",new File(xmlFile).getName)


    val resultRDD = SparkUtils.convertXMLtoCSV(records, params)

    resultRDD.cache()


    //resultRDD.collect().foreach(println)

    //resultRDD.saveAsTextFile("C:\\Users\\AR6A848N\\Desktop\\TransformBigXML\\transform-result.csv")

    resultRDD.repartition(1000)
    println(resultRDD.count())
    println(resultRDD.getNumPartitions)

    val finalRDD = resultRDD.filter( row => row.trim != "\n")
    finalRDD.coalesce(1).saveAsTextFile("C:\\Users\\abhishek.ravi\\Desktop\\abhishek\\TransformBigXML\\result")
  }

}
