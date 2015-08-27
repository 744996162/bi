import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2015/8/20.
 */
public class C3test {


    public static void main(String[] args){
        System.setProperty("com.mchange.v2.c3p0.cfg.xml","src/configs/c3p0-config.xml");
        ComboPooledDataSource ds91Bi = new ComboPooledDataSource("91bi");
        Connection conn = null;
        try {
            conn = ds91Bi.getConnection();
            String sql = "SELECT sum(success_amount + change_amount) gt_GMV " +
                    "FROM gtgj_amount_daily " +
                    "where s_day = ? ";
            PreparedStatement pstmt = conn.prepareStatement(sql);

            pstmt.setString(1, "2015-08-19");

            ResultSet rs = pstmt.executeQuery();
            Map<String,Float> map = new HashMap<String,Float>();
            while (rs.next()) {
                Float gt_GMV = rs.getFloat("gt_GMV");
                map.put("gt_GMV", gt_GMV);
            }

            System.out.println(map);
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
