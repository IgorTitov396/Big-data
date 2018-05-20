import java.lang
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, StringTokenizer}

import Main.OutputValueMapperClass
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, Text, UTF8}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


object Main extends App {
  type InputKeyMapperClass = AnyRef
  type InputValueMapperClass = Text
  type OutputKeyMapperClass = Text
  type OutputValueMapperClass = Text
  type OutputKeyReducerClass = Text
  type OutputValueReducerClass = Text
  type MapperType = Mapper[InputKeyMapperClass, InputValueMapperClass, OutputKeyMapperClass, OutputValueMapperClass]
  type ReducerType = Reducer[OutputKeyMapperClass, OutputValueMapperClass, OutputKeyReducerClass, OutputValueReducerClass]

  class TokenizerMapper extends MapperType {
    override def map(key: AnyRef, value: Text, context: MapperType#Context): Unit = {
      Try {
        val itr = new StringTokenizer(value.toString, "\t")
        val id = itr.nextToken
        val hour = {
          val rawHour = Utils.getJavaDate(itr.nextToken).getHours
          if (rawHour.toString.length == 1) "0" + rawHour else rawHour.toString
        }
        val domain = Utils.getDomain(itr.nextToken)
        context.write(new OutputKeyMapperClass(domain), Utils.stringPairAsText(hour.toString, id))
      }.getOrElse {
        context.write(null, null)
      }

    }
  }

  class MyReducer extends ReducerType {
    override def reduce(key: OutputKeyMapperClass, values: lang.Iterable[OutputValueMapperClass], context: ReducerType#Context): Unit = {

        if (values.nonEmpty) {
          val peakHour =
            values.toList
              .map(Utils.textAsStringPair)
              .distinct
              .groupBy(_._1)
              .map(p => (p._1, p._2.length))
              .maxBy(_._2)
              ._1

          context.write(new OutputKeyReducerClass(key.toString + ":"), new OutputValueReducerClass(peakHour))
        }
    }
  }

  val conf = new Configuration
  val job = new Job(conf, "word count")
  job.setJarByClass(Main.getClass)
  job.setMapperClass(classOf[Main.TokenizerMapper])
  job.setCombinerClass(classOf[Main.MyReducer])
  job.setReducerClass(classOf[Main.MyReducer])
  job.setOutputKeyClass(classOf[OutputKeyReducerClass])
  job.setOutputValueClass(classOf[OutputValueReducerClass])
  FileInputFormat.addInputPath(job, new Path(args(0)))
  FileOutputFormat.setOutputPath(job, new Path(args(1)))
  System.exit(if (job.waitForCompletion(true)) 0
  else 1)
}

object Utils {
  def getDomain(url: String): String = {
    val uri = Try(new URI(url)).getOrElse(urlFromStringWithIllegalCharacters(url))
    val host = uri.getHost
    if (host.startsWith("www.")) host.drop(4) else host
  }

  def getJavaDate(time: String): Date = {
    val formatter = new SimpleDateFormat("yyyy MM dd HH:mm:ss.SSS")
    formatter.parse(time)
  }

  def stringPairAsText(v1: String, v2: String): Text = {
    val str = v1 + separator + v2
    new Text(str.getBytes)
  }

  def textAsStringPair(text: OutputValueMapperClass): (String, String) = {
    val arr = text.toString.split(Utils.separator)
    (arr(0), arr(1))
  }

  private def urlFromStringWithIllegalCharacters(str: String): URI = {
    val ponetnialUri = str.dropRight(1)
    Try(new URI(ponetnialUri)) match {
      case Success(uri) => new URI(ponetnialUri)
      case Failure(_: java.net.URISyntaxException) => urlFromStringWithIllegalCharacters(ponetnialUri)
      case _ => new URI("")
    }
  }

  val separator = "  "

}