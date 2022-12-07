import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang3.StringUtils;
public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] variables = value.toString().split(",", -1);
    String id = variables[0];
    if (!(id.equals("rec_id"))) context.write(new Text("Records"), new IntWritable(1));
  }
}