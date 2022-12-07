import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang3.StringUtils;
import java.lang.Float;
public class ProjectMapper_1 extends Mapper<LongWritable, Text, Text, FloatWritable> {
 
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] variables = value.toString().split(",", -1);
    if (variables.length == 22 && Float.parseFloat(variables[21])==1){
      for (int i=2; i<=20; i++) variables[i] = variables[i].trim();
      context.write(new Text("Safe_Connect"), new FloatWritable(Float.parseFloat(variables[2])));
      context.write(new Text("URL_length"), new FloatWritable(Float.parseFloat(variables[3])));
      context.write(new Text("Count_at"), new FloatWritable(Float.parseFloat(variables[4])));
      context.write(new Text("Count_dot"), new FloatWritable(Float.parseFloat(variables[5])));
      context.write(new Text("Count_hyphen"), new FloatWritable(Float.parseFloat(variables[6])));
      context.write(new Text("Count_underline"), new FloatWritable(Float.parseFloat(variables[7])));
      context.write(new Text("Count_slash"), new FloatWritable(Float.parseFloat(variables[8])));
      context.write(new Text("Count_question"), new FloatWritable(Float.parseFloat(variables[9])));
      context.write(new Text("Count_equal"), new FloatWritable(Float.parseFloat(variables[10])));
      context.write(new Text("Count_and"), new FloatWritable(Float.parseFloat(variables[11])));
      context.write(new Text("Count_excla"), new FloatWritable(Float.parseFloat(variables[12])));
      context.write(new Text("Count_space"), new FloatWritable(Float.parseFloat(variables[13])));
      context.write(new Text("Count_tilde"), new FloatWritable(Float.parseFloat(variables[14])));
      context.write(new Text("Count_comma"), new FloatWritable(Float.parseFloat(variables[15])));
      context.write(new Text("Count_plus"), new FloatWritable(Float.parseFloat(variables[16])));
      context.write(new Text("Count_aster"), new FloatWritable(Float.parseFloat(variables[17])));
      context.write(new Text("Count_hash"), new FloatWritable(Float.parseFloat(variables[18])));
      context.write(new Text("Count_dollar"), new FloatWritable(Float.parseFloat(variables[19])));
      context.write(new Text("Count_percent"), new FloatWritable(Float.parseFloat(variables[20])));
    }
  }
}

