package biz.itcons.hive.maskdata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

@Description(
        name = "Randomize date",
        value = "returns 'date x randomized by 1h', where x is whatever date give it (STRING)",
        extended = "SELECT randomDate('world', 'yyyy-MM-dd HH:mm:ss', 60) from foo limit 1;"
)
public class RandomDate extends UDF {

    Random rand = new Random();

    public Text evaluate(Text input, Text pattern, int seconds) {
        if (input == null) {
            return null;
        }
        Date newDate = null;
        SimpleDateFormat sdf = new SimpleDateFormat(pattern.toString());
        try {
            Date date = sdf.parse(input.toString());
            int operator = rand.nextInt(2);
            if (operator==0){
                operator=-1;
            }
            newDate = new Date(date.getTime() + operator*rand.nextInt(seconds) * 1000);
        } catch (ParseException ex) {
            return null;
        }

        return new Text(sdf.format(newDate));
    }
}
