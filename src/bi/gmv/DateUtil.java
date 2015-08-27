package bi.gmv;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 2015/8/6.
 */
public class DateUtil {

    public static Date strToDate(String stringDate) throws ParseException {
        String strFormat = "yyyyMMdd";
        if (stringDate.length() == 10){
            strFormat = "yyyy-MM-dd";
        }else if (stringDate.length() == 8){
            strFormat = "yyyyMMdd";
        }
        return DateUtil.strToDate(stringDate, strFormat);
    }

    public static Date strToDate(String string, String strFormat) throws ParseException {
        SimpleDateFormat df=new SimpleDateFormat(strFormat);
        Date date = df.parse(string);
        Calendar calendar = Calendar.getInstance();//日历对象
        calendar.setTime(date);//设置当前日期
        return calendar.getTime();
    }


    public static String dateToStr(Date date){
        String defaultDateFormat = "%Y%m%d";
        return DateUtil.dateToStr(date, defaultDateFormat);
    }

    public static String dateToStr(Date date, String strFormat){
        Calendar calendar = Calendar.getInstance();//日历对象
        calendar.setTime(date);//设置日期
        SimpleDateFormat df=new SimpleDateFormat(strFormat);
        return df.format(calendar.getTime());
    }





    public static long dateDiff(String strDate1, String strDate2) throws ParseException {
        String strFormat = "yyyyMMdd";
        if (strDate1.length() == 10 && strDate2.length() == 10){
            strFormat = "yyyy-MM-dd";
        }else if (strDate1.length() == 8 && strDate2.length() == 8){
            strFormat = "yyyyMMdd";
        }
        return DateUtil.dateDiff(strDate1, strDate2, strFormat);
    }

    public static long dateDiff(String strDate1, String strDate2, String strFormat) throws ParseException {
        Date date1 = DateUtil.strToDate(strDate1, strFormat);
        Date date2 = DateUtil.strToDate(strDate2, strFormat);
        return DateUtil.dateDiff(date1, date2);
    }

    public static long dateDiff(Date date1, Date date2){
        long day = 0;
        day = (date1.getTime() - date2.getTime()) / (24 * 60 * 60 * 1000);
        return day;
    }


    public static String getDate(String s_day, int i) throws ParseException {
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
        Date date = df.parse(s_day);//当前日期
        Calendar calendar = Calendar.getInstance();//日历对象
        calendar.setTime(date);//设置当前日期
        calendar.add(Calendar.DATE, -i);//日期份减i
        return df.format(calendar.getTime());
    }

    public static void main(String[] args) throws ParseException {

        System.out.println("hello");
        Date t = DateUtil.strToDate("20150805");
        System.out.println(t);

        System.out.println(DateUtil.dateDiff("2015-12-31", "2015-07-01"));
    }




}
