package bi.gmv;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.sql.DriverManager.getConnection;

/**
 * Created by Administrator on 2015/8/14.
 *
 *
 * update table gmv_predict_daily
 */




public class GMVPlan extends AllGMV {
    public static long goal = 15000000000L;

    private Map<String, Double> actualMap;
    Map<String, Double> fifteenBillionGMV;

    public GMVPlan() throws SQLException, ClassNotFoundException {
        actualMap = this.getActualGMV();
        fifteenBillionGMV = this.getFifteenBillionGMV();
    }


    public Map<String, Double> getGMVPlanMap(String sDay) throws SQLException, ClassNotFoundException, ParseException {

        Map<String, Double> resultGMVPlanMap = new HashMap<String, Double>();
//
        double sumHisGMV = 0;

        for (Map.Entry<String, Double> entry :actualMap.entrySet()){
            if (entry.getValue() != 0 ){
                sumHisGMV += entry.getValue();
                resultGMVPlanMap.put(entry.getKey(),entry.getValue());
            }
        }



        double leftGMV = goal - sumHisGMV;

        List<String> sDayList = GMVPlan.getLeftDayList(sDay);


        //计算剩下的15billion的总交易额
        double leftFiftenGMV = 0 ;
        for (String tempDay:sDayList){
            Double fifteenValue = fifteenBillionGMV.get(tempDay);
            leftFiftenGMV += fifteenValue;
        }


        //计算剩下的每天计划值
        for (String tempDay:sDayList){
            Double fifteenValue = fifteenBillionGMV.get(tempDay);
            Double fifteenPercent = fifteenValue / leftFiftenGMV;
            Double tempPlanValue = fifteenPercent * leftGMV;
            resultGMVPlanMap.put(tempDay, tempPlanValue);
        }

        return resultGMVPlanMap;

    }


    //返回从sDay到年底的排序号的日期list

    public static List<String> getLeftDayList(String sDay) throws ParseException {
        String endDay = "2015-12-31";
        List<String> sDayList = new LinkedList<String>();
        long t = DateUtil.dateDiff(endDay, sDay);
        for (int i = 0; i<=t; i++){
            String tempDay = AllGMV_gtAll.getDate(sDay, -i);
            sDayList.add(tempDay);
        }
        Collections.sort(sDayList);
        return sDayList;
    }



    public int[] updatePlan(String sDay) throws SQLException, ClassNotFoundException, ParseException {

        Map<String, Double> gmvPlanMap = this.getGMVPlanMap(sDay);

        ComboPooledDataSource dsBi = new ComboPooledDataSource(biDbName);
        Connection conBi = dsBi.getConnection();

        PreparedStatement pstmt = conBi.prepareStatement("update gmv_predict_daily set plan_gmv= ?, plan_gmv_complete=?, updatetime=now() where s_day = ? ");

//        将一组参数添加到此 PreparedStatement 对象的批处理命令中。
        for(Map.Entry<String, Double> entry: gmvPlanMap.entrySet()) {
            //数据的日期大于等于昨天日期，数据更新，历史数据设为null
            if (DateUtil.dateDiff(sDay, entry.getKey())<=1){
                pstmt.setDouble(1, entry.getValue());
            }else{
                pstmt.setNull(1, Types.DOUBLE);
            }
            pstmt.setDouble(2, entry.getValue());
            pstmt.setString(3, entry.getKey());
            pstmt.addBatch();
        }

        int[] result = pstmt.executeBatch();

        pstmt.close();
        conBi.close();
        return result;
    }


    public void updateMain() throws ParseException, SQLException, ClassNotFoundException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String sToday = df.format(new Date());
        this.updatePlan(sToday);
    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {
        GMVPlan gmv = new GMVPlan();
        gmv.updateMain();

    }

}
