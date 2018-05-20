import java.lang
import java.util.StringTokenizer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


object WordCount extends App {

  object TokenizerMapper {
    private val one = new IntWritable(1)
  }

  class TokenizerMapper extends Mapper[AnyRef, Text, Text, IntWritable] {
    private val word = new Text

    override def map(key: AnyRef, value: Text, context: Mapper[AnyRef, Text, Text, IntWritable]#Context): Unit = {
      val itr = new StringTokenizer(value.toString)
      while ( {
        itr.hasMoreTokens
      }) {
        word.set(itr.nextToken)
        context.write(word, TokenizerMapper.one)
      }
    }
  }

  class IntSumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    private val result = new IntWritable

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0
      import scala.collection.JavaConversions._
      for (value <- values) {
        sum += value.get
      }
      result.set(sum)
      context.write(key, result)
    }
  }

    val conf = new Configuration
    val job = new Job(conf, "word count")
    job.setJarByClass(WordCount.getClass)
    job.setMapperClass(classOf[WordCount.TokenizerMapper])
    job.setCombinerClass(classOf[WordCount.IntSumReducer])
    job.setReducerClass(classOf[WordCount.IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if (job.waitForCompletion(true)) 0
    else 1)
}