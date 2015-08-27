package bi.ticket;

import bi.gmv.DateUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import static java.sql.DriverManager.getConnection;

/**
 * Created by Administrator on 2015/8/20.
 */
public class HbTicket {

    public HbTicket() {

//        System.setProperty("com.mchange.v2.c3p0.cfg.xml", "src/configs/c3p0-config.xml");
    }


    private Map<String, Object> getTicketYestoday(String sYestoday) throws SQLException {
        ComboPooledDataSource ds91bi = new ComboPooledDataSource("91bi");
        Connection con91 = ds91bi.getConnection();

        String sql = "SELECT s_day,ticket_num,order_num,ticket_num_gt,order_num_gt " +
                    "FROM hbgj_order_daily " +
                    "where s_day=? " ;

        PreparedStatement pstmt = con91.prepareStatement(sql);

        pstmt.setString(1, sYestoday);

        ResultSet rs = pstmt.executeQuery();
        Map<String,Object> map = new HashMap<String,Object>();
        while (rs.next()) {
            map.put("s_day", rs.getString("s_day"));
            map.put("ticket_num", rs.getInt("ticket_num"));
            map.put("order_num", rs.getInt("order_num"));
            map.put("ticket_num_gt", rs.getInt("ticket_num_gt"));
            map.put("order_num_gt", rs.getInt("order_num_gt"));
        }
        pstmt.close();
        con91.close();
        System.out.println(map);
        return map;

    }

    private Map<String,Object> getTicketToday(String sToday) throws ClassNotFoundException, SQLException {
        ComboPooledDataSource ds79Skyhotel = new ComboPooledDataSource("79skyhotel");

        Connection con79 = ds79Skyhotel.getConnection();
        String sql = "SELECT DATE_FORMAT(CREATETIME,'%Y-%m-%d') s_day, " +
                "count(A.ORDERID) ticket_num, " +
                "count(distinct A.ORDERID) order_num, " +
                "count(case when A.p like '%gtgj%' then A.ORDERID END) ticket_num_gt, " +
                "count(DISTINCT case when A.p like '%gtgj%' then A.ORDERID END) order_num_gt " +
                "from " +
                "(select TICKET_ORDERDETAIL.createtime as CREATETIME,TICKET_ORDERDETAIL.ORDERID as ORDERID,TICKET_ORDER.p AS  p " +
                "from TICKET_ORDERDETAIL INNER JOIN  TICKET_ORDER ON TICKET_ORDER.ORDERID = TICKET_ORDERDETAIL.ORDERID " +
                "where TICKET_ORDER.ORDERSTATUE not in (2,12,21,51,75) " +
                "and TICKET_ORDER.createtime >=? " +
                ") as A ";

        PreparedStatement pstmt = con79.prepareStatement(sql);

        pstmt.setString(1, sToday);

        ResultSet rs = pstmt.executeQuery();
        Map<String,Object> map = new HashMap<String,Object>();
        while (rs.next()) {
            map.put("s_day", rs.getString("s_day"));
            map.put("ticket_num", rs.getInt("ticket_num"));
            map.put("order_num", rs.getInt("order_num"));
            map.put("ticket_num_gt", rs.getInt("ticket_num_gt"));
            map.put("order_num_gt", rs.getInt("order_num_gt"));
        }
        pstmt.close();
        con79.close();
        System.out.println(map);
        return map;
    }


    public int[] update(String sToday) throws SQLException, ParseException, ClassNotFoundException {

        String sYestoday = DateUtil.getDate(sToday,1);
        String sHis2Day = DateUtil.getDate(sToday,2);

        Map<String,Object> todayResult = this.getTicketToday(sToday);
        Map<String,Object> yestodayResult = this.getTicketYestoday(sYestoday);


        ComboPooledDataSource ds91bi = new ComboPooledDataSource("91bi");
        Connection con91 = ds91bi.getConnection();


        String sql = "update hbgj_order_daily " +
                "set ticket_num_new= ?,order_num_new=?,ticket_num_gt_new=?, order_num_gt_new=?,updatetime=now() " +
                "where s_day = ? " ;

        String sqlInsert = "insert into hbgj_order_daily" +
                "(s_day,createtime) values(?,now()) ";



        PreparedStatement pstmt = con91.prepareStatement(sql);

        try {
            PreparedStatement pstmtInsert = con91.prepareStatement(sqlInsert);
            pstmtInsert.setString(1, sToday);
            pstmtInsert.execute();
        } catch (Exception e){
            System.out.println();
        }


        //today
        pstmt.setInt(1, (Integer) todayResult.get("ticket_num"));
        pstmt.setInt(2, (Integer) todayResult.get("order_num"));
        pstmt.setInt(3, (Integer) todayResult.get("ticket_num_gt"));
        pstmt.setInt(4, (Integer) todayResult.get("order_num_gt"));
        pstmt.setString(5, (String) todayResult.get("s_day"));
        pstmt.addBatch();

        //yestoday
        pstmt.setInt(1, (Integer) yestodayResult.get("ticket_num"));
        pstmt.setInt(2, (Integer) yestodayResult.get("order_num"));
        pstmt.setInt(3, (Integer) yestodayResult.get("ticket_num_gt"));
        pstmt.setInt(4, (Integer) yestodayResult.get("order_num_gt"));
        pstmt.setString(5, (String) yestodayResult.get("s_day"));
        pstmt.addBatch();

        //two days before
        pstmt.setNull(1, Types.INTEGER);
        pstmt.setNull(2, Types.INTEGER);
        pstmt.setNull(3, Types.INTEGER);
        pstmt.setNull(4, Types.INTEGER);
        pstmt.setString(5, sHis2Day);
        pstmt.addBatch();

        int[] result = pstmt.executeBatch();
        pstmt.close();
        con91.close();
        return result;

    }




    public int[] updateNew(String sToday) throws SQLException, ParseException, ClassNotFoundException {

        String sYestoday = DateUtil.getDate(sToday,1);
        String sHis2Day = DateUtil.getDate(sToday,2);

        Map<String,Object> todayResult = this.getTicketToday(sToday);
        Map<String,Object> yestodayResult = this.getTicketYestoday(sYestoday);


        ComboPooledDataSource ds91bi = new ComboPooledDataSource("91bi");
        Connection con91 = ds91bi.getConnection();


        String sql = "update hbgj_order_daily " +
                "set ticket_num= ?,order_num=?,ticket_num_gt=?, order_num_gt=?," +
                "ticket_num_new= ?,order_num_new=?,ticket_num_gt_new=?, order_num_gt_new=?, updatetime=now() " +
                "where s_day = ? " ;

        String sqlInsert = "insert into hbgj_order_daily" +
                "(s_day,createtime) values(?,now()) ";



        PreparedStatement pstmt = con91.prepareStatement(sql);

        try {
            PreparedStatement pstmtInsert = con91.prepareStatement(sqlInsert);
            pstmtInsert.setString(1, sToday);
            pstmtInsert.execute();
        } catch (Exception e){
            System.out.println();
        }


        //today
        pstmt.setInt(1, (Integer) todayResult.get("ticket_num"));
        pstmt.setInt(2, (Integer) todayResult.get("order_num"));
        pstmt.setInt(3, (Integer) todayResult.get("ticket_num_gt"));
        pstmt.setInt(4, (Integer) todayResult.get("order_num_gt"));
        pstmt.setInt(5, (Integer) todayResult.get("ticket_num"));
        pstmt.setInt(6, (Integer) todayResult.get("order_num"));
        pstmt.setInt(7, (Integer) todayResult.get("ticket_num_gt"));
        pstmt.setInt(8, (Integer) todayResult.get("order_num_gt"));
        pstmt.setString(9, (String) todayResult.get("s_day"));
        pstmt.addBatch();

        //yestoday
        pstmt.setInt(1, (Integer) yestodayResult.get("ticket_num"));
        pstmt.setInt(2, (Integer) yestodayResult.get("order_num"));
        pstmt.setInt(3, (Integer) yestodayResult.get("ticket_num_gt"));
        pstmt.setInt(4, (Integer) yestodayResult.get("order_num_gt"));

        pstmt.setNull(5, Types.INTEGER);
        pstmt.setNull(6, Types.INTEGER);
        pstmt.setNull(7, Types.INTEGER);
        pstmt.setNull(8, Types.INTEGER);

        pstmt.setString(9, (String) yestodayResult.get("s_day"));
        pstmt.addBatch();


        int[] result = pstmt.executeBatch();
        pstmt.close();
        con91.close();
        return result;

    }


    public void updateMain() throws ParseException, SQLException, ClassNotFoundException {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new java.util.Date());
//        String s_yestoday = DateUtil.getDate(s_today, 1);
        this.updateNew(s_today);
    }





    public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {
        System.setProperty("com.mchange.v2.c3p0.cfg.xml", "src/configs/c3p0-config.xml");
        HbTicket hb = new HbTicket();
        hb.updateMain();
    }

}

