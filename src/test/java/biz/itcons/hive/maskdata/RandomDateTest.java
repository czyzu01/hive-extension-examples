package biz.itcons.hive.maskdata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import junit.framework.Assert;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class RandomDateTest {
  
  @Test
  public void testUDF() throws ParseException {
    RandomDate example = new RandomDate();
    String FORMAT = "yyyy-MM-dd HH:mm:ss";
    int epsilon = 60;
    SimpleDateFormat sdf = new SimpleDateFormat (FORMAT);
    Date testDate = sdf.parse("2017-06-15 15:04:15");

    String value = example.evaluate(new Text(sdf.format(testDate)), new Text(FORMAT), epsilon).toString();
    Date retDate = sdf.parse(value);
    Assert.assertTrue(Math.abs(testDate.getTime()-retDate.getTime())<epsilon*1000);
  }
  
//  @Test
//  public void testUDFNullCheck() {
//    RandomDate example = new RandomDate();
//    String FORMAT = "yyyy-MM-dd HH:mm:ss";
//    int epsilon = 60;
//    String value = example.evaluate(new Text(null), new Text(FORMAT), new IntWritable(epsilon));
//    Assert.assertNull(value);
//  }
}