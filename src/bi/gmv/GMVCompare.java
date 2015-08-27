package bi.gmv;

import com.mchange.v2.c3p0.ComboPooledDataSource;

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
 *
 *
 * update table gmv_compare_weekly
 */
public class GMVCompare extends AllGMV {


    public int updateCompare(String sDay) throws SQLException, ClassNotFoundException {

        Map<String, Double> actualGMVMap = this.getActualGMV();

        ComboPooledDataSource dsBi = new ComboPooledDataSource(this.biDbName);
        Connection conBi = dsBi.getConnection();

        PreparedStatement pstmt = conBi.prepareStatement("update gmv_compare_weekly set actual_gmv= ?, updatetime=now() where s_day = ? ");
        pstmt.setDouble(1, actualGMVMap.get(sDay));
        pstmt.setString(2, sDay);
        int result = pstmt.executeUpdate();
        pstmt.close();
        conBi.close();
        return result;
    }

    public void updateMain() throws ParseException, SQLException, ClassNotFoundException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s_today = df.format(new Date());

        Calendar c = Calendar.getInstance();
        int s = c.get(Calendar.DAY_OF_WEEK);

//        System.out.println(c);
//        System.out.println(s);
//        周一更新过去一周的数据
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