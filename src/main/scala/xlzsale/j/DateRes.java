package xlzsale.j;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateRes extends UDF {
    public static  String evaluate(int input) {
        SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd");
        Calendar rightNow = Calendar.getInstance();
        rightNow.set(1970, 0, 1);
        rightNow.add(rightNow.DATE, input);
        String res=dfs.format(rightNow.getTime());
        return res;
    }

    public static void main(String[] args) {
    }
}
