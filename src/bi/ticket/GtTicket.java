package bi.ticket;

import bi.gmv.DateUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2015/8/20.
 *
 * 新的高铁订单 订票 交易额 统计
 * 7.1 开始 计入全部订单和订票
 * 结果存入 gtgj_gmv_daily
 * gtgj_order_daily (订单)
 *
 */
public class GtTicket {

    public GtTicket() {

//        System.setProperty("com.mchange.v2.c3p0.cfg.xml", "src/configs/c3p0-config.xml");
    }


    private Map<String, Map<String, Long>> getHisData() throws SQLException {


        ComboPooledDataSource ds81Gtgj = new ComboPooledDataSource("81gtgj");
        Connection con81 = ds81Gtgj.getConnection();



        String sql = "select  DATE_FORMAT(create_time,'%Y-%m-%d') s_day, " +
                "count(*) order_num, " +
                "sum(ticket_count) ticket_num, " +
                "sum(amount) gmv, " +
                "sum(case when p_info like \"%,ios,%\" then 1 else 0 end) order_num_ios," +
                "sum(case when p_info like \"%,ios,%\" then ticket_count else 0 end) ticket_num_ios, " +
                "sum(case when p_info like \"%,ios,%\" then amount else 0 end) gmv_ios " +
                "from user_order  " +
                "where create_time>=? " +
                "group by s_day " +
                "order by create_time " ;


        PreparedStatement pstmt = con81.prepareStatement(sql);

        pstmt.setString(1, "2015-07-01");
        ResultSet rs = pstmt.executeQuery();
        Map<String, Map<String,Long>> resultMap = new HashMap<String, Map<String, Long>>();
        while (rs.next()){
            Map<String,Long> tempMap = new HashMap<String, Long>();
            tempMap.put("order_num", rs.getLong("order_num"));
            tempMap.put("ticket_num", rs.getLong("ticket_num"));
            tempMap.put("gmv", rs.getLong("gmv"));

            tempMap.put("order_num_ios", rs.getLong("order_num_ios"));
            tempMap.put("ticket_num_ios", rs.getLong("ticket_num_ios"));
            tempMap.put("gmv_ios", rs.getLong("gmv_ios"));

            tempMap.put("order_num_android", rs.getLong("order_num") - rs.getLong("order_num_ios"));
            tempMap.put("ticket_num_android", rs.getLong("ticket_num") - rs.getLong("ticket_num_ios"));
            tempMap.put("gmv_android", rs.getLong("gmv") - rs.getLong("gmv_ios"));

            resultMap.put(rs.getString("s_day"), tempMap);
        }
        pstmt.close();
        con81.close();
        return resultMap;

    }


    private void updateHistory() throws SQLException, ParseException, ClassNotFoundException {


        Map<String, Map<String, Long>> hisData = this.getHisData();

        ComboPooledDataSource ds91bi = new ComboPooledDataSource("91bi");
        Connection con91 = ds91bi.getConnection();
        String sYestoday = DateUtil.getDate("2015-08-20",1);

        String sql = "update gtgj_order_daily " +
                "set ticket_num=?,order_num=?,gmv=?, " +
                "ticket_num_ios=?,order_num_ios=?,gmv_ios=?, " +
                "ticket_num_android=?,order_num_android=?,gmv_android=?, updatetime=now() " +
                "where s_day = ? ";


        PreparedStatement ps = con91.prepareStatement(sql);

        for (String sDay:hisData.keySet()){

            ps.setLong(1, hisData.get(sDay).get("ticket_num"));
            ps.setLong(2, hisData.get(sDay).get("order_num"));
            ps.setLong(3, hisData.get(sDay).get("gmv"));

            ps.setLong(4, hisData.get(sDay).get("ticket_num_ios"));
            ps.setLong(5, hisData.get(sDay).get("order_num_ios"));
            ps.setLong(6, hisData.get(sDay).get("gmv_ios"));

            ps.setLong(7, hisData.get(sDay).get("ticket_num_android"));
            ps.setLong(8, hisData.get(sDay).get("order_num_android"));
            ps.setLong(9, hisData.get(sDay).get("gmv_android"));

            ps.setString(10, sDay);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
        con91.close();

    }


    private Map<String, Map<String, Long>> getTicketYestodayAndToday(String sYestoday) throws SQLException {
        ComboPooledDataSource ds81Gtgj = new ComboPooledDataSource("81gtgj");
        Connection con81 = ds81Gtgj.getConnection();

        String sql = "select  DATE_FORMAT(create_time,'%Y-%m-%d') s_day, " +
                "count(distinct order_id) order_num, " +
                "sum(ticket_count) ticket_num, " +
                "sum(amount) gmv, " +
                "sum(case when p_info like \"%,ios,%\" then 1 else 0 end) order_num_ios," +
                "sum(case when p_info like \"%,ios,%\" then ticket_count else 0 end) ticket_num_ios, " +
                "sum(case when p_info like \"%,ios,%\" then amount else 0 end) gmv_ios " +
                "from user_order  " +
                "where create_time>=? " +
                "group by s_day " +
                "order by create_time " ;


        PreparedStatement pstmt = con81.prepareStatement(sql);

        pstmt.setString(1, "2015-07-01");
        ResultSet rs = pstmt.executeQuery();
        Map<String, Map<String,Long>> resultMap = new HashMap<String, Map<String, Long>>();
        while (rs.next()){
            Map<String,Long> tempMap = new HashMap<String, Long>();
            tempMap.put("order_num", rs.getLong("order_num"));
            tempMap.put("ticket_num", rs.getLong("ticket_num"));
            tempMap.put("gmv", rs.getLong("gmv"));

            tempMap.put("order_num_ios", rs.getLong("order_num_ios"));
            tempMap.put("ticket_num_ios", rs.getLong("ticket_num_ios"));
            tempMap.put("gmv_ios", rs.getLong("gmv_ios"));

            tempMap.put("order_num_android", rs.getLong("order_num") - rs.getLong("order_num_ios"));
            tempMap.put("ticket_num_android", rs.getLong("ticket_num") - rs.getLong("ticket_num_ios"));
            tempMap.put("gmv_android", rs.getLong("gmv") - rs.getLong("gmv_ios"));

            resultMap.put(rs.getString("s_day"), tempMap);
        }
        pstmt.close();
        con81.close();
        return resultMap;
    }


    public void update(String sToday) throws SQLException, ParseException, ClassNotFoundException {

        String sYestoday = DateUtil.getDate(sToday,1);


        Map<String, Map<String, Long>> yestodayAndTodayData = this.getTicketYestodayAndToday(sYestoday);

        ComboPooledDataSource ds91bi = new ComboPooledDataSource("91bi");
        Connection con91 = ds91bi.getConnection();


        String sql = "update gtgj_order_daily " +
                "set ticket_num=?,order_num=?,gmv=?, " +
                "ticket_num_ios=?,order_num_ios=?,gmv_ios=?, " +
                "ticket_num_android=?,order_num_android=?,gmv_android=?, updatetime=now() " +
                "where s_day = ? ";


        PreparedStatement ps = con91.prepareStatement(sql);

        for (String sDay:yestodayAndTodayData.keySet()){

            ps.setLong(1, yestodayAndTodayData.get(sDay).get("ticket_num"));
            ps.setLong(2, yestodayAndTodayData.get(sDay).get("order_num"));
            ps.setLong(3, yestodayAndTodayData.get(sDay).get("gmv"));

            ps.setLong(4, yestodayAndTodayData.get(sDay).get("ticket_num_ios"));
            ps.setLong(5, yestodayAndTodayData.get(sDay).get("order_num_ios"));
            ps.setLong(6, yestodayAndTodayData.get(sDay).get("gmv_ios"));

            ps.setLong(7, yestodayAndTodayData.get(sDay).get("ticket_num_android"));
            ps.setLong(8, yestodayAndTodayData.get(sDay).get("order_num_android"));
            ps.setLong(9, yestodayAndTodayData.get(sDay).get("gmv_android"));

            ps.setString(10, sDay);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
        con91.close();

    }


    public void updateMain() throws ParseException, SQLException, ClassNotFoundException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new java.util.Date());

        this.update(s_today);

    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {
        System.setProperty("com.mchange.v2.c3p0.cfg.xml", "src/configs/c3p0-config.xml");
        GtTicket gt = new GtTicket();
        gt.updateMain();
//        gt.updateMain();
//        System.out.println(t);
    }

}

