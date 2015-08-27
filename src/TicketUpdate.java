/**
 * Created by Administrator on 2015/8/21.
 */

import bi.ticket.HbTicket;
import bi.ticket.GtTicket;

import java.sql.SQLException;
import java.text.ParseException;

public class TicketUpdate {

    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException {

        System.setProperty("com.mchange.v2.c3p0.cfg.xml", "src/configs/c3p0-config.xml");
        //机票更新
        HbTicket hb = new HbTicket();
        hb.updateMain();

        //火车票更新
        GtTicket gt = new GtTicket();
        gt.updateMain();


    }

}
