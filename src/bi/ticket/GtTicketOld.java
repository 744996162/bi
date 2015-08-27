package bi.ticket;

import bi.gmv.DateUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2015/8/20.
 */
public class GtTicketOld {

    public GtTicketOld() {

//        System.setProperty("com.mchange.v2.c3p0.cfg.xml", "src/configs/c3p0-config.xml");
    }


    private Map<String, Map<String, Long>> getHisData() throws SQLException {


        ComboPooledDataSource ds81Gtgj = new ComboPooledDataSource("81gtgj");
        Connection con81 = ds81Gtgj.getConnection();

        String sql = "SELECT s_day, " +
                "sum(succ_orders) order_num,sum(succ_tickets) ticket_num,sum(succ_amount) amount_num, " +
                "sum(case when source in ('91PGZS','91ZS','appstore','ent299','juwan','kuaiyong','appstorepro','PPZS','TBT','appstore32') then succ_orders End) order_num_ios, " +
                "sum(case when source in ('91PGZS','91ZS','appstore','ent299','juwan','kuaiyong','appstorepro','PPZS','TBT','appstore32') then succ_tickets end) ticket_num_ios, " +
                "sum(case when source in ('91PGZS','91ZS','appstore','ent299','juwan','kuaiyong','appstorepro','PPZS','TBT','appstore32') then succ_amount end) amount_num_ios " +
                "from global_statistics " +
                "where s_day>=? " +
                "GROUP BY s_day ";

        PreparedStatement pstmt = con81.prepareStatement(sql);

        pstmt.setString(1, "2013-01-01");
        ResultSet rs = pstmt.executeQuery();
        Map<String, Map<String,Long>> resultMap = new HashMap<String, Map<String, Long>>();
        while (rs.next()){
            Map<String,Long> tempMap = new HashMap<String, Long>();
            tempMap.put("order_num", rs.getLong("order_num"));
            tempMap.put("ticket_num", rs.getLong("ticket_num"));
            tempMap.put("gmv", rs.getLong("amount_num"));

            tempMap.put("order_num_ios", rs.getLong("order_num_ios"));
            tempMap.put("ticket_num_ios", rs.getLong("ticket_num_ios"));
            tempMap.put("gmv_ios", rs.getLong("amount_num_ios"));

            tempMap.put("order_num_android", rs.getLong("order_num") - rs.getLong("order_num_ios"));
            tempMap.put("ticket_num_android", rs.getLong("ticket_num") - rs.getLong("ticket_num_ios"));
            tempMap.put("gmv_android", rs.getLong("amount_num") - rs.getLong("amount_num_ios"));

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

//        String sqlInsert = "insert into gtgj_order_daily" +
//                "(s_day,createtime) values(?,now()) ";
//        PreparedStatement psInsert = con91.prepareStatement(sqlInsert);
//        for (int i=0;i<=850;i++){
//            String tempDay = DateUtil.getDate("2015-08-20",i);
//            psInsert.setString(1, tempDay);
//            psInsert.addBatch();
//        }
//        psInsert.executeBatch();

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


    private Map<String, Map<String, Long>> getTicketYestoday(String sYestoday) throws SQLException {
        ComboPooledDataSource ds81Gtgj = new ComboPooledDataSource("81gtgj");
        Connection con81 = ds81Gtgj.getConnection();

        String sql = "SELECT s_day, " +
                "sum(succ_orders) order_num,sum(succ_tickets) ticket_num,sum(succ_amount) amount_num, " +
                "sum(case when source in ('91PGZS','91ZS','appstore','ent299','juwan','kuaiyong','appstorepro','PPZS','TBT','appstore32') then succ_orders End) order_num_ios, " +
                "sum(case when source in ('91PGZS','91ZS','appstore','ent299','juwan','kuaiyong','appstorepro','PPZS','TBT','appstore32') then succ_tickets end) ticket_num_ios, " +
                "sum(case when source in ('91PGZS','91ZS','appstore','ent299','juwan','kuaiyong','appstorepro','PPZS','TBT','appstore32') then succ_amount end) amount_num_ios " +
                "from global_statistics " +
                "where s_day=? " ;

        PreparedStatement pstmt = con81.prepareStatement(sql);
        pstmt.setString(1, sYestoday);

        ResultSet rs = pstmt.executeQuery();
        Map<String, Map<String,Long>> resultMap = new HashMap<String, Map<String, Long>>();
        while (rs.next()){
            Map<String,Long> tempMap = new HashMap<String, Long>();
            tempMap.put("order_num", rs.getLong("order_num"));
            tempMap.put("ticket_num", rs.getLong("ticket_num"));
            tempMap.put("gmv", rs.getLong("amount_num"));

            tempMap.put("order_num_ios", rs.getLong("order_num_ios"));
            tempMap.put("ticket_num_ios", rs.getLong("ticket_num_ios"));
            tempMap.put("gmv_ios", rs.getLong("amount_num_ios"));

            tempMap.put("order_num_android", rs.getLong("order_num") - rs.getLong("order_num_ios"));
            tempMap.put("ticket_num_android", rs.getLong("ticket_num") - rs.getLong("ticket_num_ios"));
            tempMap.put("gmv_android", rs.getLong("amount_num") - rs.getLong("amount_num_ios"));

            resultMap.put(rs.getString("s_day"), tempMap);
        }

        pstmt.close();
        con81.close();
        return resultMap;
    }

    private Map<String, Map<String, Long>> getTicketToday(String sToday) throws ClassNotFoundException, SQLException {
        ComboPooledDataSource ds81gtgj = new ComboPooledDataSource("81gtgj");

        Connection con81 = ds81gtgj.getConnection();
        String sql = " select  DATE_FORMAT(pay_time,'%Y-%m-%d') s_day,  " +
                "count(*) order_num,  " +
                "sum(ticket_count) ticket_num,  " +
                "sum(amount) gmv " +
                "from user_order  " +
                "where pay_time>=? " +
                "and i_status=3";

        PreparedStatement pstmt = con81.prepareStatement(sql);

        pstmt.setString(1, sToday);

        ResultSet rs = pstmt.executeQuery();
        Map<String,Map<String,Long>> map = new HashMap<String,Map<String,Long>>();
        while (rs.next()) {
            Map<String,Long> tempMap = new HashMap<String, Long>();
            tempMap.put("order_num", rs.getLong("order_num"));
            tempMap.put("ticket_num", rs.getLong("ticket_num"));
            tempMap.put("gmv", rs.getLong("gmv"));

            map.put(rs.getString("s_day"), tempMap);
        }

        pstmt.close();
        con81.close();

        return map;
    }


    public int[] update(String sToday) throws SQLException, ParseException, ClassNotFoundException {

        String sYestoday = DateUtil.getDate(sToday,1);
        String sHis2Day = DateUtil.getDate(sToday,2);

        Map<String,Map<String,Long>> todayResult = this.getTicketToday(sToday);
        Map<String,Map<String,Long>> yestodayResult = this.getTicketYestoday(sYestoday);

        ComboPooledDataSource ds91bi = new ComboPooledDataSource("91bi");
        Connection con91 = ds91bi.getConnection();


        String sqlYestoday = "update gtgj_order_daily " +
                "set ticket_num=?,order_num=?,gmv=?, " +
                "ticket_num_ios=?,order_num_ios=?,gmv_ios=?, " +
                "ticket_num_android=?,order_num_android=?,gmv_android=?, " +
                "ticket_num_new=?,order_num_new=?,gmv_new=?,updatetime=now() " +
                "where s_day = ? " ;

        String sqlNull = "update gtgj_order_daily " +
                "set ticket_num_new=?,order_num_new=?,gmv_new=?,updatetime=now() " +
                "where s_day = ? " ;


        String sqlInsert = "insert into gtgj_order_daily" +
                "(s_day,createtime) values(?,now()) ";
        
        PreparedStatement ps = con91.prepareStatement(sqlYestoday);
        PreparedStatement psNull = con91.prepareStatement(sqlNull);

        try {
            PreparedStatement pstmtInsert = con91.prepareStatement(sqlInsert);
            pstmtInsert.setString(1, sToday);
            pstmtInsert.execute();
        } catch (Exception e){
            System.out.println();
        }


        //today

        ps.setLong(1, todayResult.get(sToday).get("ticket_num"));
        ps.setLong(2, todayResult.get(sToday).get("order_num"));
        ps.setLong(3, todayResult.get(sToday).get("gmv"));
        ps.setNull(4, Types.INTEGER);
        ps.setNull(5, Types.INTEGER);
        ps.setNull(6, Types.INTEGER);
        ps.setNull(7, Types.INTEGER);
        ps.setNull(8, Types.INTEGER);
        ps.setNull(9, Types.INTEGER);

        ps.setLong(10, todayResult.get(sToday).get("ticket_num"));
        ps.setLong(11, todayResult.get(sToday).get("order_num"));
        ps.setLong(12, todayResult.get(sToday).get("gmv"));
        ps.setString(13, sToday);
        ps.addBatch();


        //yestoday
        ps.setLong(1, yestodayResult.get(sYestoday).get("ticket_num"));
        ps.setLong(2, yestodayResult.get(sYestoday).get("order_num"));
        ps.setLong(3, yestodayResult.get(sYestoday).get("gmv"));

        ps.setLong(4, yestodayResult.get(sYestoday).get("ticket_num_ios"));
        ps.setLong(5, yestodayResult.get(sYestoday).get("order_num_ios"));
        ps.setLong(6, yestodayResult.get(sYestoday).get("gmv_ios"));

        ps.setLong(7, yestodayResult.get(sYestoday).get("ticket_num_android"));
        ps.setLong(8, yestodayResult.get(sYestoday).get("order_num_android"));
        ps.setLong(9, yestodayResult.get(sYestoday).get("gmv_android"));

        ps.setNull(10, Types.INTEGER);
        ps.setNull(11, Types.INTEGER);
        ps.setNull(12, Types.INTEGER);

        ps.setString(13, sYestoday);
        ps.addBatch();

        //two days before
        psNull.setNull(1, Types.INTEGER);
        psNull.setNull(2, Types.INTEGER);
        psNull.setNull(3, Types.INTEGER);
        psNull.setString(4, sHis2Day);
        psNull.addBatch();

        int[] result = ps.executeBatch();
        psNull.executeBatch();

        ps.close();
        psNull.close();
        con91.close();
        return result;

    }


    public void updateMain() throws ParseException, SQLException, ClassNotFoundException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new java.util.Date());
//        String s_yestoday = DateUtil.getDate(s_today, 1);
        this.update(s_today);


    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {
        System.setProperty("com.mchange.v2.c3p0.cfg.xml", "src/configs/c3p0-config.xml");

        GtTicketOld gt = new GtTicketOld();
//        gt.updateMain();

//        gt.update("2015-08-20");
//        gt.update("2015-08-21");
        gt.update("2015-08-22");
//        gt.update("2015-08-23");
//        gt.update("2015-08-24");



//        System.out.println(t);
    }

}

