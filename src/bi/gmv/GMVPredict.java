package bi.gmv;



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
 */



public class GMVPredict extends AllGMV {

    /**返回日期list sDay为截止日期，i为之前几个星期
    例如 sDay='2015-08-17' weekCount=1 返回'2015-08-10'到'2015-08-16'之间的日期(含)

     */

    private  Map<String, Double> actualGMV;
    private  Map<String, Double> fifteenBillionGMVNew;

    public GMVPredict() throws SQLException, ClassNotFoundException {
        actualGMV = this.getActualGMV();
        System.out.println(actualGMV);
        fifteenBillionGMVNew = this.getFifteenBillionGMVNew();
    }


    public static List<String> getWeekDayList(String sDay, int weekCount) throws ParseException {
        List <String> resultList = new LinkedList<String>();
        for (int i=1;i<=7;i++){
            int dateDiff = (weekCount-1)*7 + i;
            String tempDay = DateUtil.getDate(sDay, dateDiff);
            resultList.add(tempDay);
        }
        Collections.sort(resultList);
        return resultList;
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


    public Map<String, Double> getWeekSumGMV(String sDay, int weekCount) throws ParseException {
        List<String> sWeekDayList = GMVPredict.getWeekDayList(sDay, weekCount);
        Double actualSumGMV = (double) 0;
        Double fifteenBillionSumGMV = (double) 0;

        for (String tempDay:sWeekDayList){
            actualSumGMV += this.actualGMV.get(tempDay);
            fifteenBillionSumGMV += this.fifteenBillionGMVNew.get(tempDay);
        }
        Map<String, Double> resultMap = new HashMap<String, Double>();
        resultMap.put("actualSumGMV", actualSumGMV);
        resultMap.put("fifteenBillionSumGMV",fifteenBillionSumGMV);
        resultMap.put("ratio", actualSumGMV/fifteenBillionSumGMV);
//        System.out.println(weekCount);
//        System.out.println(resultMap);
        return resultMap;
    }

    //返回预测交易额需要的
    public Double getRatio(String sDay) throws ParseException {
        Double week1Ratio, week2Ratio, week3Ratio, week4Ratio;
        week1Ratio = this.getWeekSumGMV(sDay, 1).get("ratio");
        week2Ratio = this.getWeekSumGMV(sDay, 2).get("ratio");
        week3Ratio = this.getWeekSumGMV(sDay, 3).get("ratio");
        week4Ratio = this.getWeekSumGMV(sDay, 4).get("ratio");
//        Double ratio = week1Ratio * 0.1 + week2Ratio * 0.2 + week3Ratio * 0.3 + week4Ratio * 0.4;
        return week1Ratio * 0.4 + week2Ratio * 0.3 + week3Ratio * 0.2 + week4Ratio * 0.1;
    }


    public int[] updateActualGMV(Map<String,Double> map) throws ClassNotFoundException, SQLException {

        String driver = "com.mysql.jdbc.Driver";
        String url_bi = "jdbc:mysql://58.83.130.91:3306/bi";
        String user_bi = "bi";
        String password_bi = "bIbi_0820";
        Class.forName(driver);
        Connection conn_bi = getConnection(url_bi, user_bi, password_bi);


        PreparedStatement pstmt = conn_bi.prepareStatement("update gmv_predict_daily set actual_gmv= ?, updatetime=now() where s_day = ? ");


        for(Map.Entry<String, Double> entry: map.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
            if (entry.getValue() <= 0.01){
                pstmt.setNull(1, Types.DOUBLE);
            }
            else {
                pstmt.setDouble(1, entry.getValue());
            }
            pstmt.setString(2, entry.getKey());
            pstmt.addBatch();
        }
        int[] result = pstmt.executeBatch();
        pstmt.close();
        conn_bi.close();
        return result;
    }



    public Map<String, Double> getPredictMap(String sDay) throws SQLException, ClassNotFoundException, ParseException {
        Map<String,Double> predictMap = new HashMap<String, Double>();
        predictMap.putAll(this.actualGMV);

        Double ratio = this.getRatio(sDay);
        List<String> leftDayList = GMVPredict.getLeftDayList(sDay);
        for(String tempDay:leftDayList){
            Double tempValue = this.fifteenBillionGMVNew.get(tempDay) * ratio ;
            predictMap.put(tempDay, tempValue);
        }
        return predictMap;
    }


    public int[] updatePredict(String sDay) throws SQLException, ClassNotFoundException, ParseException {

        Map<String, Double> predictMap = this.getPredictMap(sDay);

        String driver = "com.mysql.jdbc.Driver";
        String url_bi = "jdbc:mysql://58.83.130.91:3306/bi";
        String user_bi = "bi";
        String password_bi = "bIbi_0820";
        Class.forName(driver);
        Connection conn_bi = getConnection(url_bi, user_bi, password_bi);

        PreparedStatement pstmt = conn_bi.prepareStatement("update gmv_predict_daily set predict_gmv= ?, predict_gmv_complete=?, updatetime=now() where s_day = ? ");

//        将一组参数添加到此 PreparedStatement 对象的批处理命令中。
        for(Map.Entry<String, Double> entry: predictMap.entrySet()) {

            System.out.println(entry.getKey() + "\t" + entry.getValue());
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
        conn_bi.close();
        return result;
    }



    public void updateMain() throws ParseException, SQLException, ClassNotFoundException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String sToday = df.format(new Date());
        this.updateActualGMV(this.actualGMV);
        System.out.println(this.actualGMV);
        this.updatePredict(sToday);


        GMVPlan gmvPlan = new GMVPlan();
        gmvPlan.updatePlan(sToday);
    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {

        GMVPredict gmvPredict = new GMVPredict();
        gmvPredict.updateMain();

    }

}
