package bi.gtgmv;

/**
 * Created by Administrator on 2015/7/10.
 */

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


import static java.sql.DriverManager.*;

public class GtAmount {
    protected String gtSourceDbName = "81gtgj";
    protected String biDbName = "91bi";

    public  float getChangeAmount(String s_day) throws Exception{

        ComboPooledDataSource dsGt = new ComboPooledDataSource(this.gtSourceDbName);
        Connection conGt = dsGt.getConnection();

        float money_back_amount = 0L;
        float money_add_amount = 0L;

        float change_amount = 0L;

        int money_back_count = 0;
        int money_equal_count = 0;
        int money_add_conut = 0;
        int count = 0;

        PreparedStatement ordersPstmt = conGt.prepareStatement("select card_no, change_time,refund_time, status, price from user_sub_order where order_id=? ");
        PreparedStatement pstmt = conGt.prepareStatement("select distinct(order_id) oid from user_sub_order where create_time >= ? and create_time <= ? and ( status='改签票' or ( status='已退票' and change_time is not null and change_time!=refund_time )) ");


        String s_day_in_start = s_day + " " + "00:00:00";
//        String s_day_in_end = s_day + " " + "07:10:00";
        String s_day_in_end = s_day + " " + "23:59:59";


        pstmt.setTimestamp(1, Timestamp.valueOf(s_day_in_start));
        pstmt.setTimestamp(2, Timestamp.valueOf(s_day_in_end));
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            String changeOid = rs.getString("oid");
            ordersPstmt.setString(1, changeOid);
            ResultSet rs2 = ordersPstmt.executeQuery();
            // 保存结果集
            List<String[]> result = new ArrayList<String[]>(1000);
            while (rs2.next()) {
                String cardNo = rs2.getString("card_no");
                String changetime = rs2.getString("change_time");

                String price = rs2.getString("price");
                String status = rs2.getString("status");
                String refund_time = rs2.getString("refund_time");
                String[] strs = new String[]{cardNo, changetime, price, status, refund_time};
                result.add(strs);
            }
            // 按改签时间(表示一次改签)把子订单分组
            Map<String, Set<String>> timeMap = new HashMap<String, Set<String>>(5);
            for (String[] strs : result) {
                // 把改签票选出来
                if (strs[3].equals("改签票") || (strs[3].equals("已退票")  && strs[1] != null && !strs[1].equals(strs[4]))) {
                    Set<String> list = timeMap.get(strs[1]);
                    // 按时间分组，判断是一起改签的票
                    if (list == null) {
                        list = new HashSet<String>(5);
                        timeMap.put(strs[1], list);
                    }
                    list.add(strs[0]);
                }
            }

            // 遍历查找每一组改签的新、老票价
            for (Map.Entry<String, Set<String>> entry : timeMap.entrySet()) {
                float oldprice = 0F;
                float newprice = 0F;
                for (String[] strs : result) {
                    // 根据证件号匹配
                    if (entry.getValue().contains(strs[0])) {
                        if (strs[3].equals("改签票") || (strs[3].equals("已退票") && strs[1] != null && !strs[1].equals(strs[4]) )) {
                            // 新票
                            newprice = newprice + Float.parseFloat(strs[2]);
                        } else {
                            // 原票
                            oldprice = oldprice + Float.parseFloat(strs[2]);
                        }
                    }

                }

                if (oldprice == newprice){
                    money_equal_count += 1;
                }
                if (oldprice > newprice){
                    money_back_amount += (oldprice-newprice);
                    money_back_count += 1;
                }
                if (oldprice < newprice){
                    money_add_amount += (newprice-oldprice);
                    money_add_conut += 1;

                    change_amount += newprice;
                }
                count += 1;


            }

        }
//        System.out.println("all_count" + "," + count);
//        System.out.println("equal_count" + "," + money_equal_count  );
//        System.out.println("money_back_count" + "," + money_back_count + "," + money_back_amount);
//        System.out.println("money_add_count" + "," + money_add_conut + "," + money_add_amount);
        System.out.println(s_day + "," + "changeAmount" + "," + change_amount);

//                float[] f = new float[2];
        Map map = new  HashMap();
        map.put("change_amount",change_amount);
        map.put("money_back_amount",money_back_amount);
        map.put("money_add_amount",money_add_amount);
        map.put("s_day", s_day);

        return change_amount;
    }


    public Long getSuccAmount(String s_day) throws Exception {
        ComboPooledDataSource dsGt = new ComboPooledDataSource(this.gtSourceDbName);
        Connection conGt = dsGt.getConnection();


        PreparedStatement pstmt = conGt.prepareStatement("select sum(succ_amount) amount from global_statistics where s_day = ? ");

        pstmt.setString(1, s_day);
        ResultSet rs = pstmt.executeQuery();

        if (rs.next()){
//            Long amount = rs.getLong("amount");
            return rs.getLong("amount");
        }

        pstmt.close();
        conGt.close();
        return 0L;
    }

    public int updataGtAmount(String s_day, float succAmount, float changeAmount) throws ClassNotFoundException, SQLException {
        ComboPooledDataSource dsBi = new ComboPooledDataSource(this.biDbName);
        Connection conBi = dsBi.getConnection();

        PreparedStatement pstmt = conBi.prepareStatement("insert gtgj_amount_daily (s_day,success_amount,change_amount,createtime,updatetime) values (?, ?, ?, now(), now()) ");

        pstmt.setString(1, s_day);
        pstmt.setFloat(2, succAmount);
        pstmt.setFloat(3, changeAmount);
        try {
            int result = pstmt.executeUpdate();
            return result;
        } catch (Exception e) {
//            pstmt = conn_bi.prepareStatement("insert gtgj_amount_daily (s_day,success_amount,change_amount,createtime,updatetime) values (?, ?, ?, now(), now()) ");

            pstmt = conBi.prepareStatement("update gtgj_amount_daily set success_amount=?, change_amount=?, updatetime=now() where s_day = ? " ) ;
            pstmt.setFloat(1, succAmount);
            pstmt.setFloat(2, changeAmount);
            pstmt.setString(3, s_day);
            int result = pstmt.executeUpdate();
            return result;
        }
    }

    public int update_amount(String s_day) throws Exception {
        float changeAmount = this.getChangeAmount(s_day);
        System.out.println(changeAmount);
        float successAmount = this.getSuccAmount(s_day);
        int state = this.updataGtAmount(s_day, successAmount, changeAmount);
        return state;
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
//        GtAmount gt = new GtAmount();

//        int state = gt.update_amount("2015-07-08");
//        int state1 = gt.update_amount("2015-07-07");
//        int state2 = gt.update_amount("2015-07-06");
//        int state3 = gt.update_amount("2015-07-05");
//        int state4 = gt.update_amount("2015-07-04");
//        int state5 = gt.update_amount("2015-07-03");
//        int state = gt.update_amount("2015-07-14");

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new Date());
        String s_yestoday = GtAmount.getDate(s_today, 1);


        GtAmount gt = new GtAmount();
        int state = gt.update_amount(s_yestoday);
        System.out.println(state);

//        System.out.println(s_today + "\t" + s_yestoday);


    }
}
