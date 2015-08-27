import bi.gmv.AllGMV;
import bi.gmv.GMVCompare;
import bi.gmv.GMVPredict;

/**
 * Created by Administrator on 2015/8/17.
 */



public class GMVUpdate {


    public static void main(String[] args) throws Exception {

        //更新0 gmv_base
        AllGMV allGMV = new AllGMV();
        allGMV.updateMain();

        //更新1 gmv_compare_weekly
        GMVCompare gmvCompare = new GMVCompare();
        gmvCompare.updateMain();

        //更新2 gmv_predict_daily
        GMVPredict gmvPredict = new GMVPredict();
        gmvPredict.updateMain();

    }


}
