package bi.gtgmv;
/**
 * Created by Administrator on 2015/7/20.
 */

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static java.sql.DriverManager.getConnection;


public class GtTicketFromHb {

    public Map<String,Float> getGtTicketFromHb(String s_day) throws Exception {

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://58.83.130.89:3306/gtgj?&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull";
        String user = "querynew";
        String password = "query1216";
        Class.forName(driver);
        Connection conn = getConnection(url, user, password);


        String sql = "select count(*) order_num, sum(ticket_count) ticket_num, sum(amount) amount " +
                "FROM hbgj_order_12306 " +
                "LEFT JOIN user_order on hbgj_order_12306.order_id = user_order.order_id " +
                "where hbgj_order_12306.create_time >=?  " +
                "and hbgj_order_12306.create_time <=?  " +
                "and (hbgj_order_12306.status = 1 or hbgj_order_12306.status=2) " ;

        PreparedStatement pstmt = conn.prepareStatement(sql);

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

            map.put("order_num", order_num);
            map.put("ticket_num", ticket_num);
            map.put("amount", amount);
//            System.out.println(order_num.toString() + ticket_num.toString());
        }
//        System.out.println(map.toString());
//        System.out.println(sql);

        return map;
    }

    public int updateGtTicketFromHb(String s_day, Map<String,Float> map) throws ClassNotFoundException, SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String url_bi = "jdbc:mysql://58.83.130.91:3306/bi";
        String user_bi = "bi";
        String password_bi = "bIbi_0820";
        Class.forName(driver);
        Connection conn_bi = getConnection(url_bi, user_bi, password_bi);


        PreparedStatement pstmt = conn_bi.prepareStatement("insert gtgj_ticket_from_hb (s_day,order_num,ticket_num,amount,createtime,updatetime) values (?, ?, ?, ?, now(), now()) ");

        pstmt.setString(1, s_day);
        pstmt.setFloat(2, map.get("order_num"));
        pstmt.setFloat(3, map.get("ticket_num"));
        pstmt.setFloat(4, map.get("amount"));
        try {
            int result = pstmt.executeUpdate();
            return result;
        } catch (Exception e) {
//            pstmt = conn_bi.prepareStatement("insert gtgj_amount_daily (s_day,success_amount,change_amount,createtime,updatetime) values (?, ?, ?, now(), now()) ");

            pstmt = conn_bi.prepareStatement("update gtgj_ticket_from_hb set order_num=?, ticket_num=?, amount=?, updatetime=now() where s_day = ? " ) ;
            pstmt.setFloat(1, map.get("order_num"));
            pstmt.setFloat(2, map.get("ticket_num"));
            pstmt.setFloat(3, map.get("amount"));
            pstmt.setString(4, s_day);
            int result = pstmt.executeUpdate();
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
        String s_yestoday = GtTicketFromHb.getDate(s_today, 1);
        GtTicketFromHb ticket = new GtTicketFromHb();
        ticket.update(s_yestoday);


//        ticket.update("2015-07-18");
//        ticket.update("2015-07-17");
//        ticket.update("2015-07-16");
//        ticket.update("2015-07-15");
//        ticket.update("2015-07-14");
//        ticket.update("2015-07-13");
//        System.out.println();

    }

}
