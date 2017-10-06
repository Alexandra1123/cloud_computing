import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FileCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    Text filename = new Text();
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
      filename = new Text(filenameStr);


      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, filename);
      }
    }
  }

  public static class FileReducer
       extends Reducer<Text,Text,Text,Text> {
    //private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException {
    StringBuilder listFiles = new StringBuilder();
    Set<Text> vals=new HashSet<Text>();

    boolean sym=true;

      for (Text value : values) {
        if (sym){
          listFiles.append(" : ");
          sym=false;
        }
        boolean added=vals.add(value);
        if (added){
              listFiles.append(value.toString());
              listFiles.append(" ");
        }
    }

    context.write(key, new Text(listFiles.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.setNumReduceTask(1);
    Job job = Job.getInstance(conf, "Inverted Index");
    job.setJarByClass(FileCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(FileReducer.class);
    job.setReducerClass(FileReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]))
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
