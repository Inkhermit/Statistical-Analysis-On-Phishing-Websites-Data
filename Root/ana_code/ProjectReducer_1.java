import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class ProjectReducer_1 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
  @Override
  public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
    int total = 0;
    int num = 0;
    for (FloatWritable value : values) {
      total += value.get();
      num += 1;
    }
    context.write(key, new FloatWritable((float)total/num));
  }
}
