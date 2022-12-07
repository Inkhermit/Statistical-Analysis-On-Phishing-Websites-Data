import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.lang.Integer;
public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] line = value.toString().split(",", -1);

    if (line.length == 5){

    if (!line[0].equals("rec_id")){
    

    String id = line[0].trim();
    String url = line[1].trim();
    String result = line[3].trim();

    if (id.length()+url.length()+result.length() != 0){
      String https = "0";
      if (url.contains("https")) https = "1";
      String url_l = Integer.toString(url.length());
      String c_at = Integer.toString(count_url('@', url));
      String c_dot = Integer.toString(count_url('.', url));
      String c_hyphen = Integer.toString(count_url('-', url));
      String c_underline = Integer.toString(count_url('_', url));
      String c_slash = Integer.toString(count_url('/', url));
      String c_question = Integer.toString(count_url('?', url));
      String c_equal = Integer.toString(count_url('=', url));
      String c_and = Integer.toString(count_url('&', url));
      String c_excla = Integer.toString(count_url('!', url));
      String c_space = Integer.toString(count_url(' ', url));
      String c_tilde = Integer.toString(count_url('~', url));
      String c_comma = Integer.toString(count_url(',', url));
      String c_plus = Integer.toString(count_url('+', url));
      String c_aster = Integer.toString(count_url('*', url));
      String c_hash = Integer.toString(count_url('#', url));
      String c_dollar = Integer.toString(count_url('$', url));
      String c_percent = Integer.toString(count_url('%', url));
      
      context.write(new Text(id+","+url+","+https+","+url_l+","+c_at+","+c_dot+","+c_hyphen+","+c_underline+","+c_slash+","+c_question+","+c_equal+","+c_and+","+c_excla+","+c_space+","+c_tilde+","+c_comma+","+c_plus+","+c_aster+","+c_hash+","+c_dollar+","+c_percent+","+result), new Text(""));
    }
    }
    }
  }

  public int count_url(char c, String url){
    int count = 0;
    for (int i = 0; i < url.length(); i++) {
      if (url.charAt(i) == c) {
        count++;
      }
    }
    return count;
  }
}
