package utils

import java.io.{ByteArrayInputStream, File, StringWriter}

import javax.xml.transform.TransformerFactory
import javax.xml.transform.stream.{StreamResult, StreamSource}

import scala.collection.mutable

object TransformUtils {

  def transform(stylesheet: File, input: Array[Byte], inputLen: Int, params: mutable.Map[String,String]) = {
    val writer = new StringWriter()

    val transformerFactory = TransformerFactory.newInstance()
    val transformer = transformerFactory.newTransformer(new StreamSource(stylesheet))

    for(key <- params.keySet){
      transformer.setParameter(key, params.get(key).getOrElse(""))
    }

    transformer.transform(new StreamSource(new ByteArrayInputStream(input, 0, inputLen)), new StreamResult(writer))

    writer.toString
  }

}
