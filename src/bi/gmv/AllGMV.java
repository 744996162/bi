package bi.gmv;



/**
 * Created by Administrator on 2015/7/20.
 */

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.sql.DriverManager.getConnection;


public class AllGMV {


    protected String hbSourceDbName = "79skyhotel";
    protected String biDbName = "91bi";

    public Map<String, Float> getHbGMV(String s_day) throws Exception {

        ComboPooledDataSource dsSourceHb = new ComboPooledDataSource(this.hbSourceDbName);
        Connection conSourceHb = dsSourceHb.getConnection();

        String sql = "select sum(A.price) hb_GMV " +
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

        PreparedStatement pstmt = conSourceHb.prepareStatement(sql);

        pstmt.setString(1, s_day);
        pstmt.setString(2, s_day);


        ResultSet rs = pstmt.executeQuery();
        Map<String,Float> map = new HashMap<String,Float>();
        while (rs.next()) {
            Float hb_GMV = rs.getFloat("hb_GMV");
            map.put("hb_GMV", hb_GMV);

        }
        pstmt.close();
        conSourceHb.close();

        return map;
    }

    public Map<String, Float> getGtGMV(String s_day) throws Exception{


        ComboPooledDataSource dsBi = new ComboPooledDataSource(this.biDbName);
        Connection conBi = dsBi.getConnection();


        String sql = "SELECT sum(success_amount + change_amount + create_amount) gt_GMV " +
                     "FROM gtgj_amount_daily " +
                     "where s_day = ? ";

        PreparedStatement pstmt = conBi.prepareStatement(sql);

        pstmt.setString(1, s_day);


        ResultSet rs = pstmt.executeQuery();
        Map<String,Float> map = new HashMap<String,Float>();
        while (rs.next()) {
            Float gt_GMV = rs.getFloat("gt_GMV");
            map.put("gt_GMV", gt_GMV);
        }

        pstmt.close();
        conBi.close();

        return map;
    }
//
//
    public Map<String, Float> getHotelGMV(String s_day) throws Exception{
        ComboPooledDataSource dsSourceHb = new ComboPooledDataSource(this.hbSourceDbName);
        Connection conHbSource = dsSourceHb.getConnection();


        String sql = "select sum(totalprice) hotel_GMV " +
                    "from hotelorder " +
                    "where DATE_FORMAT(createtime,'%Y-%m-%d')=? " +
                    "and (gdsdesc in ('已成单','已结账','已确认','已入住','已取消') or hotelorderid is not null) " ;

        PreparedStatement pstmt = conHbSource.prepareStatement(sql);

        pstmt.setString(1, s_day);


        ResultSet rs = pstmt.executeQuery();
        Map<String,Float> map = new HashMap<String,Float>();
        while (rs.next()) {
            Float hotel_GMV = rs.getFloat("hotel_GMV");
            map.put("hotel_GMV", hotel_GMV);
        }
        pstmt.close();
        conHbSource.close();
        return map;
    }

    public Map<String, Float> getAllGMV(String s_day) throws Exception{
        Map<String,Float>  map_hb = this.getHbGMV(s_day);
        Map<String,Float>  map_gt = this.getGtGMV(s_day);
        Map<String,Float>  map_hotel = this.getHotelGMV(s_day);
        Map<String,Float> map_all = new HashMap<String, Float>();

        float hb_GMV = map_hb.get("hb_GMV");
        float gt_GMV = map_gt.get("gt_GMV");
        float hotel_GMV = map_hotel.get("hotel_GMV");
        float all_GMV = hb_GMV + gt_GMV + hotel_GMV;

        map_all.put("hb_GMV", hb_GMV);
        map_all.put("gt_GMV", gt_GMV);
        map_all.put("hotel_GMV", hotel_GMV);
        map_all.put("all_GMV", all_GMV);
        return map_all;

    }

    public Map<String,Double> getActualGMV() throws ClassNotFoundException, SQLException {

        ComboPooledDataSource dsBi = new ComboPooledDataSource(this.biDbName);
        Connection conBi = dsBi.getConnection();

        String sql = "SELECT s_day, actual_gmv FROM gmv_base ";

        PreparedStatement pstmt = conBi.prepareStatement(sql);

        ResultSet rs = pstmt.executeQuery();
        Map<String,Double> map = new HashMap<String, Double>();
        while (rs.next()) {
            String sDay = rs.getString("s_day");
            double actualGMV = rs.getDouble("actual_gmv");
            map.put(sDay, actualGMV);
        }

        pstmt.close();
        conBi.close();

        return map;
    }

    public Map<String,Double> getFifteenBillionGMV() throws ClassNotFoundException, SQLException {
        ComboPooledDataSource dsBi = new ComboPooledDataSource(this.biDbName);
        Connection conBi = dsBi.getConnection();

        String sql = "SELECT s_day, 15billion FROM gmv_base ";

        PreparedStatement pstmt = conBi.prepareStatement(sql);

        ResultSet rs = pstmt.executeQuery();
        Map<String,Double> map = new HashMap<String, Double>();
        while (rs.next()) {
            String sDay = rs.getString("s_day");
            double fifteenBillionGMV = rs.getDouble("15billion");
            map.put(sDay, fifteenBillionGMV);
        }

        pstmt.close();
        conBi.close();

        return map;
    }

    public Map<String,Double> getFifteenBillionGMVNew() throws ClassNotFoundException, SQLException {
        ComboPooledDataSource dsBi = new ComboPooledDataSource(this.biDbName);
        Connection conBi = dsBi.getConnection();

        String sql = "SELECT s_day, 15billion_new FROM gmv_base ";

        PreparedStatement pstmt = conBi.prepareStatement(sql);

        ResultSet rs = pstmt.executeQuery();
        Map<String,Double> map = new HashMap<String, Double>();
        while (rs.next()) {
            String sDay = rs.getString("s_day");
            double fifteenBillionGMV = rs.getDouble("15billion_new");
            map.put(sDay, fifteenBillionGMV);
        }

        pstmt.close();
        conBi.close();

        return map;
    }


    public int updateAllGMV(String s_day, Map<String,Float> map) throws ClassNotFoundException, SQLException {

      /*
        每天更新 'gmv_base' 表

      */

        ComboPooledDataSource dsBi = new ComboPooledDataSource(this.biDbName);
        Connection conBi = dsBi.getConnection();

        PreparedStatement pstmt = conBi.prepareStatement("update gmv_base set actual_gmv= ?, updatetime=now() where s_day = ? ");
        pstmt.setFloat(1, map.get("all_GMV"));
        pstmt.setString(2, s_day);
        int result = pstmt.executeUpdate();
        pstmt.close();
        conBi.close();
        return result;
    }


    public int update(String s_day) throws Exception {
        Map<String, Float> map = this.getAllGMV(s_day);
//        System.out.println(s_day + "\t" + map.toString());
        Integer result = this.updateAllGMV(s_day, map);
        return result;
    }


    public static String getDate(String s_day, int i) throws ParseException {
        SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
        Date date = df.parse(s_day);//当前日期
        Calendar calendar = Calendar.getInstance();//日历对象
        calendar.setTime(date);//设置当前日期
        calendar.add(Calendar.DATE, -i);//日期减i
        return df.format(calendar.getTime());
    }


    public void updateMain() throws Exception {
        //每天更新
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new Date());
        String s_yestoday = AllGMV.getDate(s_today, 1);
        this.update(s_yestoday);
    }


    public void updateHis() throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new Date());
        for (int i=1; i<=238; i++){
            String s_tempDay = AllGMV.getDate(s_today, i);
            System.out.println(s_tempDay);
            this.update(s_tempDay);
        }


    }
    public static void main(String[] args) throws Exception {

        AllGMV allGMV = new AllGMV();
        allGMV.updateMain();
    }

}