package spark

import java.io.File

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import utils.TransformUtils

import scala.collection.mutable

object SparkUtils {

  def initSparkConf = {
    val sparkConf = new SparkConf()
      .setAppName("CSV-Validator")
      .setMaster("local[*]")

    sparkConf
  }

  def initSparkContext(sparkConf: SparkConf) = {
    val sparkContext = new SparkContext(sparkConf)
    sparkContext
  }

  def initSparKSession() = {
    val spark = SparkSession.builder().appName("Test-SPARK-Functions").master("local[*]").getOrCreate()
    spark
  }

  def convertXMLtoCSV(records: RDD[(LongWritable, Text)],params: mutable.Map[String,String]) = {
     records.map{ record =>
      val xsl = new File("C:\\Users\\abhishek.ravi\\Desktop\\abhishek\\TransformBigXML\\R17V7-CONSO-PAR-TYPE-MESURE-V3-2.xsl")
      val xml = record._2
      val result = TransformUtils.transform(xsl, xml.getBytes, xml.getLength, params)
       result
    }

  }

}
