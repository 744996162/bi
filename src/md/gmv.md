## 高铁交易额

1.计算每天的改签票增加的交易额，插入到数据库
2.抽取原每天的交易额到数据库
3.新的交易额为原交易额加上新交易额


## 总交易额模拟

### 数据库设计
1. "gmv_base" 存储15billion,20billion,actual_gmv的值，每天更新actual_gmv的值，为其他业务提供数据支持
2. "gmv_compare_weekly" 为"交易额比对"提供数据,15billion,actual_gmv,每周更新上一周的actual_GMV的值，15billion数据从6.1开始
3. "gmv_predict_daily" 为"交易额预测"提供数据,s_day,predict_gmv,每天更新，交易额数据每天变动
4. "gmv_predict_daily" 为"每天交易额计划"提供数据, s_day,plan_gmv, 每天更新，计算值依据15billion的每天情况计算


### 1. 实际交易额与15billion交易额对比
(15billion交易额从6.1开始，以“周”为周期)

程序需要每周一更新上周的交易额数据

表"gmv_compare_weekly"

类名: GMVCompare

### 2. 按照目前的交易额走势，年底的交易额能达到多少(交易额预测)
gmv_predict_daily

(预测以后每一天的交易额)
按比例  实际交易额/15billion交易额
权重:前四周权重分别为
0.1   0.2 0.3 0.4


### 3. 根据截止到目前的交易额，以后每天完成多少能达到15billion(给每天定目标)
(给每天定目标)
表"gmv_predict_daily" plan_gmv




