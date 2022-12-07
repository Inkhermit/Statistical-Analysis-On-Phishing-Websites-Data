import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Project_1 {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Project_1 <input path> <output path>");
      System.exit(-1);
    }
    Job job = new Job();
    job.setJarByClass(Project_1.class);
    job.setJobName("Project_1");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(ProjectMapper_1.class);
    job.setReducerClass(ProjectReducer_1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setNumReduceTasks(1);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}