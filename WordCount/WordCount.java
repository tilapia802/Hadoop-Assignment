//package org.apache.hadoop.examples;

import java.util.Map;
import java.util.TreeMap;
import java.lang.Long;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper
  extends Mapper<Object, Text, NullWritable,Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private TreeMap<Long, Text> Top10 = new TreeMap <Long, Text>();


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new Str ingTokenizer(value.toString());
      Long number;
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
	      Text tmp = new Text(word.toString());
        String number_str = word.toString();	
        number=Long.valueOf(number_str);
        Top10.put(number, tmp);

        if (Top10.size() > 10) {
          Top10.remove(Top10.firstKey());
        }
      }
      for (Text t : Top10.values()) {
        context.write(NullWritable.get(), t);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<NullWritable,Text,NullWritable,Text> {
    private IntWritable result = new IntWritable();
    private TreeMap<Long, Text> Top10 = new TreeMap <Long, Text>();
    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Long number;
     // TreeMap<Long, Text> Top10 = new TreeMap <Long, Text>();
      for (Text  val : values) {
	Text tmp = new Text(val.toString());
        String number_str = val.toString();
        number=Long.valueOf(number_str);
        Top10.put(number, tmp);
        if (Top10.size() > 10) {
         Top10.remove(Top10.firstKey());
        }

      }
      for(Text t:Top10.descendingMap().values()){
        context.write(NullWritable.get(), t);			
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


