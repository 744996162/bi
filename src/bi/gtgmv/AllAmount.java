package bi.gtgmv;

/**
 * Created by Administrator on 2015/7/20.
 */

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static java.sql.DriverManager.getConnection;


public class AllAmount {


    public Map<String, Float> getHbAmount(String s_day) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://58.83.130.79:3308/skyhotel?&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull";
        String user = "query";
        String password = "qureybyno";
        Class.forName(driver);
        Connection conn = getConnection(url, user, password);


        String sql = "select sum(A.price) hb_amount " +
                "from  " +
                "(select createtime,totalprice price " +
                "FROM gift_order " +
                "where PRODUCTID=11 " +
                "and (status=1 or status=3) " +
                "and DATE_FORMAT(createtime,'%Y-%m-%d')=? " +
                "UNION " +
                "select createtime,PAYPRICE price " +
                "from TICKET_ORDER " +
                "where " +
                "ORDERSTATUE not in (2,12,21,51,75)  " +
                "and DATE_FORMAT(createtime,'%Y-%m-%d')=? " +
                "and (ORDERTYPE!='GRAB' or ordertype is NULL) " +
                ") as A" ;

        PreparedStatement pstmt = conn.prepareStatement(sql);

        pstmt.setString(1, s_day);
        pstmt.setString(2, s_day);


        ResultSet rs = pstmt.executeQuery();
        Map<String,Float> map = new HashMap<String,Float>();
        while (rs.next()) {
            Float hb_amount = rs.getFloat("hb_amount");
            map.put("hb_amount", hb_amount);

        }

        return map;
    }

    public Map<String, Float> getGtAmount(String s_day) throws Exception{
        String driver = "com.mysql.jdbc.Driver";
        String url_bi = "jdbc:mysql://58.83.130.91:3306/bi";
        String user_bi = "bi";
        String password_bi = "bIbi_0820";
        Class.forName(driver);
        Connection conn_bi = getConnection(url_bi, user_bi, password_bi);

        String sql = "SELECT sum(success_amount+change_amount) gt_amount " +
                     "FROM gtgj_amount_daily " +
                     "where s_day = ? ";

        PreparedStatement pstmt = conn_bi.prepareStatement(sql);

        pstmt.setString(1, s_day);


        ResultSet rs = pstmt.executeQuery();
        Map<String,Float> map = new HashMap<String,Float>();
        while (rs.next()) {
            Float gt_amount = rs.getFloat("gt_amount");
            map.put("gt_amount", gt_amount);
        }

        return map;
    }
//
//
    public Map<String, Float> getHotelAmount(String s_day) throws Exception{
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://58.83.130.79:3308/skyhotel?&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull";
        String user = "query";
        String password = "qureybyno";
        Class.forName(driver);
        Connection conn = getConnection(url, user, password);


        String sql = "select sum(totalprice) hotel_amount " +
                    "from hotelorder " +
                    "where DATE_FORMAT(createtime,'%Y-%m-%d')=? " +
                    "and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null) " ;

        PreparedStatement pstmt = conn.prepareStatement(sql);

        pstmt.setString(1, s_day);


        ResultSet rs = pstmt.executeQuery();
        Map<String,Float> map = new HashMap<String,Float>();
        while (rs.next()) {
            Float hotel_amount = rs.getFloat("hotel_amount");
            map.put("hotel_amount", hotel_amount);

        }

        return map;

    }

    public Map<String, Float> getAllAmount(String s_day) throws Exception{
        Map<String,Float>  map_hb = this.getHbAmount(s_day);
        Map<String,Float>  map_gt = this.getGtAmount(s_day);
        Map<String,Float>  map_hotel = this.getHotelAmount(s_day);
        Map<String,Float> map_all = new HashMap<String, Float>();

        float hb_amount = map_hb.get("hb_amount");
        float gt_amount = map_gt.get("gt_amount");
        float hotel_amount = map_hotel.get("hotel_amount");
        float all_amount = hb_amount + gt_amount + hotel_amount;

        map_all.put("hb_amount", hb_amount);
        map_all.put("gt_amount", gt_amount);
        map_all.put("hotel_amount", hotel_amount);
        map_all.put("all_amount", all_amount);
        return map_all;

    }


    public int updateAllAmount(String s_day, Map<String,Float> map) throws ClassNotFoundException, SQLException {
    /*
        每天更新 'gmv_predict' 表


      */
        String driver = "com.mysql.jdbc.Driver";
        String url_bi = "jdbc:mysql://58.83.130.91:3306/bi";
        String user_bi = "bi";
        String password_bi = "bIbi_0820";
        Class.forName(driver);
        Connection conn_bi = getConnection(url_bi, user_bi, password_bi);


        PreparedStatement pstmt = conn_bi.prepareStatement("update gmv_predict set actual_gmv= ?, updatetime=now() where s_day = ? ");
        pstmt.setFloat(1, map.get("all_amount"));
        pstmt.setString(2, s_day);
        int result = pstmt.executeUpdate();
        return result;
    }


    public int update(String s_day) throws Exception {
        Map<String, Float> map = this.getAllAmount(s_day);
        System.out.println(s_day + "\t" + map.toString());
        Integer result = this.updateAllAmount(s_day, map);
        return result;
    }


    public int updateNewAllAmount(String s_day, Map<String,Float> map) throws ClassNotFoundException, SQLException {

    /*
        每周更新 'gmv_predict_new' 表
        更新过去七天actual_gmv的值
        7-14天的15biliion 和 20 billion值设置为Null

    */
        String driver = "com.mysql.jdbc.Driver";
        String url_bi = "jdbc:mysql://58.83.130.91:3306/bi";
        String user_bi = "bi";
        String password_bi = "bIbi_0820";
        Class.forName(driver);
        Connection conn_bi = getConnection(url_bi, user_bi, password_bi);

        PreparedStatement pstmt = conn_bi.prepareStatement("update gmv_predict_new set actual_gmv= ?, updatetime=now() where s_day = ? ");
        pstmt.setFloat(1, map.get("all_amount"));
        pstmt.setString(2, s_day);
        int result = pstmt.executeUpdate();
        pstmt.close();
        conn_bi.close();
        return result;
    }

    public int updateNew(String s_day) throws Exception {
        Map<String, Float> map = this.getAllAmount(s_day);
        System.out.println(s_day + "\t" + map.toString());
        Integer result = this.updateNewAllAmount(s_day, map);
        return result;
    }


    public int setNull(String s_day) throws ClassNotFoundException, SQLException {

        String driver = "com.mysql.jdbc.Driver";
        String url_bi = "jdbc:mysql://58.83.130.91:3306/bi";
        String user_bi = "bi";
        String password_bi = "bIbi_0820";
        Class.forName(driver);
        Connection conn_bi = getConnection(url_bi, user_bi, password_bi);

        PreparedStatement pstmt = conn_bi.prepareStatement("update gmv_predict_new set 15billion=NULL, 20billion=NULL, updatetime=now() where s_day = ? ");
        pstmt.setString(1, s_day);
        int result = pstmt.executeUpdate();
        pstmt.close();
        conn_bi.close();
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


    public void updateMain() throws Exception {

        //每天更新
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new Date());
        String s_yestoday = AllAmount.getDate(s_today, 1);
        this.update(s_yestoday);

        //每周一，更新表gmv_predict_new

        Calendar c = Calendar.getInstance();
        int s = c.get(Calendar.DAY_OF_WEEK);
        if (s == 1){
            //更新过去七天 actual_amount 的值
            for(int i=1; i<=7; i++){
                String s_day = AllAmount.getDate(s_today, 1);
                this.updateNew(s_day);
            }
            //设置8-14天 15billion 和 20billion的值为Null
            for (int i=8; i<=14; i++){
                String s_day = AllAmount.getDate(s_today, i);
                this.setNull(s_day);
            }

        }

    }

    public static void main(String[] args) throws Exception {

//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//        String s_today = df.format(new Date());
//        String s_yestoday = AllAmount.getDate(s_today, 1);
        AllAmount allAmount = new AllAmount();
        allAmount.updateMain();

    }

}