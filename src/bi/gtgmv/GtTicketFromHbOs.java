package bi.gtgmv;
/**
 * Created by Administrator on 2015/7/20.
 */

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.sql.DriverManager.getConnection;


public class GtTicketFromHbOs {



    protected String gtSourceDbName = "81gtgj";
    protected String biDbName = "91bi";


    public Map<String,Float> getGtTicketFromHb(String s_day) throws Exception {

        ComboPooledDataSource dsSourceGt = new ComboPooledDataSource(this.gtSourceDbName);
        Connection conSourceGt = dsSourceGt.getConnection();


        String sql = "select count(*) order_num, sum(ticket_count) ticket_num, sum(amount) amount, " +
                "sum(case when hbgj_order_12306.p_info like '%ios%' then 1 end) order_num_ios, " +
                "sum(case when hbgj_order_12306.p_info like '%ios%' then ticket_count end) ticket_num_ios, " +
                "sum(case when hbgj_order_12306.p_info like '%ios%' then amount end) amount_ios, " +
                "sum(case when hbgj_order_12306.p_info like '%android%' then 1 end) order_num_android, " +
                "sum(case when hbgj_order_12306.p_info like '%android%' then ticket_count end) ticket_num_android, " +
                "sum(case when hbgj_order_12306.p_info like '%android%' then amount end) amount_android " +
                "FROM hbgj_order_12306 " +
                "LEFT JOIN user_order on hbgj_order_12306.order_id = user_order.order_id " +
                "where hbgj_order_12306.create_time >=?  " +
                "and hbgj_order_12306.create_time <=?  " +
                "and (hbgj_order_12306.status = 1 or hbgj_order_12306.status=2) " ;

        PreparedStatement pstmt = conSourceGt.prepareStatement(sql);

        String s_day_in_start = s_day + " " + "00:00:00";
        String s_day_in_end = s_day + " " + "23:59:59";

        pstmt.setTimestamp(1, Timestamp.valueOf(s_day_in_start));
        pstmt.setTimestamp(2, Timestamp.valueOf(s_day_in_end));


        ResultSet rs = pstmt.executeQuery();
        Map<String,Float> map = new HashMap<String,Float>();
        while (rs.next()) {
            Float order_num = rs.getFloat("order_num");
            Float ticket_num = rs.getFloat("ticket_num");
            Float amount = rs.getFloat("amount");

            Float order_num_ios = rs.getFloat("order_num_ios");
            Float ticket_num_ios = rs.getFloat("ticket_num_ios");
            Float amount_ios = rs.getFloat("amount_ios");

            Float order_num_android = rs.getFloat("order_num_android");
            Float ticket_num_android = rs.getFloat("ticket_num_android");
            Float amount_android = rs.getFloat("amount_android");


            map.put("order_num", order_num);
            map.put("ticket_num", ticket_num);
            map.put("amount", amount);

            map.put("order_num_ios", order_num_ios);
            map.put("ticket_num_ios", ticket_num_ios);
            map.put("amount_ios", amount_ios);

            map.put("order_num_android", order_num_android);
            map.put("ticket_num_android", ticket_num_android);
            map.put("amount_android", amount_android);


//            System.out.println(order_num.toString() + ticket_num.toString());
        }
//        System.out.println(map.toString());
//        System.out.println(sql);
        pstmt.close();
        conSourceGt.close();

        return map;
    }

    public int updateGtTicketFromHb(String s_day, Map<String,Float> map) throws ClassNotFoundException, SQLException {
        ComboPooledDataSource dsBi = new ComboPooledDataSource(this.biDbName);
        Connection conBi = dsBi.getConnection();


        PreparedStatement pstmt =
                conBi.prepareStatement("insert gtgj_ticket_from_hb_new (s_day,order_num,ticket_num,amount,order_num_ios,ticket_num_ios,amount_ios,order_num_android,ticket_num_android,amount_android,createtime,updatetime) " +
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now()) ");

        pstmt.setString(1, s_day);
        pstmt.setFloat(2, map.get("order_num"));
        pstmt.setFloat(3, map.get("ticket_num"));
        pstmt.setFloat(4, map.get("amount"));

        pstmt.setFloat(5, map.get("order_num_ios"));
        pstmt.setFloat(6, map.get("ticket_num_ios"));
        pstmt.setFloat(7, map.get("amount_ios"));

        pstmt.setFloat(8, map.get("order_num_android"));
        pstmt.setFloat(9, map.get("ticket_num_android"));
        pstmt.setFloat(10, map.get("amount_android"));

        try {
            int result = pstmt.executeUpdate();
            return result;
        } catch (Exception e) {
//            pstmt = conn_bi.prepareStatement("insert gtgj_amount_daily (s_day,success_amount,change_amount,createtime,updatetime) values (?, ?, ?, now(), now()) ");

            pstmt = conBi.prepareStatement("update gtgj_ticket_from_hb_new set order_num=?, ticket_num=?, amount=?, order_num_ios=?, ticket_num_ios=?, amount_ios=?, order_num_android=?, ticket_num_android=?, amount_android=?, " +
                    "updatetime=now() where s_day = ? " ) ;
            pstmt.setFloat(1, map.get("order_num"));
            pstmt.setFloat(2, map.get("ticket_num"));
            pstmt.setFloat(3, map.get("amount"));

            pstmt.setFloat(4, map.get("order_num_ios"));
            pstmt.setFloat(5, map.get("ticket_num_ios"));
            pstmt.setFloat(6, map.get("amount_ios"));

            pstmt.setFloat(7, map.get("order_num_android"));
            pstmt.setFloat(8, map.get("ticket_num_android"));
            pstmt.setFloat(9, map.get("amount_android"));

            pstmt.setString(10, s_day);
            int result = pstmt.executeUpdate();
            pstmt.close();
            conBi.close();
            return result;
        }
    }


    public int update(String s_day) throws Exception {
        Map<String, Float> map = this.getGtTicketFromHb(s_day);
        System.out.println(s_day + "\t" + map.toString());
        Integer result = this.updateGtTicketFromHb(s_day, map);
        return result;
    }


    public static String getDate(String s_day, int i) throws ParseException {
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
        Date date = df.parse(s_day);//当前日期
        Calendar calendar = Calendar.getInstance();//日历对象
        calendar.setTime(date);//设置当前日期
        calendar.add(Calendar.DATE, -i);//日期份减i
        return df.format(calendar.getTime());
    }

    public static void main(String[] args) throws Exception {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new Date());
        String s_yestoday = GtTicketFromHbOs.getDate(s_today, 1);
        GtTicketFromHbOs ticket = new GtTicketFromHbOs();
        ticket.update(s_yestoday);

    }

}
