package bi.gmv;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import static java.sql.DriverManager.getConnection;

/**
 * Created by Administrator on 2015/8/14.
 */
public class GMVCompare extends AllGMV {


    public int updateCompare(String sDay) throws SQLException, ClassNotFoundException {

        Map<String, Double> actualGMVMap = this.getActualGMV();
//        Double actualGMV = actualGMVMap.get(sDay);

        String driver = "com.mysql.jdbc.Driver";
        String url_bi = "jdbc:mysql://58.83.130.91:3306/bi";
        String user_bi = "bi";
        String password_bi = "bIbi_0820";
        Class.forName(driver);
        Connection conn_bi = getConnection(url_bi, user_bi, password_bi);

        PreparedStatement pstmt = conn_bi.prepareStatement("update gmv_compare_weekly set actual_gmv= ?, updatetime=now() where s_day = ? ");
        pstmt.setDouble(1, actualGMVMap.get(sDay));
        pstmt.setString(2, sDay);
        int result = pstmt.executeUpdate();
        pstmt.close();
        conn_bi.close();
        return result;
    }

    public void updateMain() throws ParseException, SQLException, ClassNotFoundException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new Date());

        Calendar c = Calendar.getInstance();
        int s = c.get(Calendar.DAY_OF_WEEK);

//        System.out.println(c);
//        System.out.println(s);
        //周一更新过去一周的数据
        if (s == 2) for (int i = 1; i <= 7; i++) {
            String tempDay = AllGMV_gtAll.getDate(s_today, i);
            this.updateCompare(tempDay);
        }
    }


    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException {
        GMVCompare gc = new GMVCompare();
        gc.updateMain();
    }
}