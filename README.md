## 计算机毕业设计PySpark+SparkML+Kafka+Hive深圳智慧交通预警系统 交通拥堵预测决策  智慧交通大数据可视化 大数据毕业设计(源码+LW+PPT+讲解)


## 要求
### 源码有偿！一套(论文 PPT 源码+sql脚本+教程)

### 
### 加好友前帮忙start一下，并备注github有偿实时深圳交通27
### 我的QQ号是2827724252或者微信:code520888 或者 bysj2023nb

# 

### 加qq好友说明（被部分 网友整得心力交瘁）：
    1.加好友务必按照格式备注
    2.避免浪费各自的时间！
    3.当“客服”不容易，repo 主是体面人，不爆粗，性格好，文明人。



## 技术架构：
前端：vue.js、element-ui、echarts
后端：springboot、jdbcTemplate
数据库：MySQL、Hive数据仓库
消息队列：kafka
实时计算：SparkStreaming
离线计算：SparkSQL+Spark+Hive
算法端：SparkML

三方平台：阿里云短信接口

以上实时计算、离线计算、算法实现均用PySpark模块实现


## 算法讲解


1. 数据清洗与标准化算法
   系统对道路属性数据和道路速度数据进行清洗，过滤 CSV 表头脏数据、空路段 ID 数据，并将字符串字段转换为数值型字段。
   例如：路段长度、拥堵速度阈值、总通行时间、样本车辆数、总行驶距离、时间片等字段都会进行类型转换，为后续计算做准备。
2. 平均速度计算算法
   系统根据车辆总行驶距离和总通行时间计算路段平均速度。
   公式为：
   avg_speed_kmh = total_travel_distance_meters / total_travel_time_seconds * 3.6
   其中 3.6 用于将 m/s 转换为 km/h。
3. 道路拥堵判定算法
   系统将计算得到的平均速度与道路属性表中的拥堵速度阈值进行比较。
   判断规则为：
   avg_speed_kmh < congestion_speed_threshold_kmh 时，判定为拥堵，is_congested = 1；否则为不拥堵，is_congested = 0。

4. 拥堵率计算算法
   系统通过拥堵记录数和总记录数计算拥堵率。
   公式为：
   congestion_rate = congested_record_count / total_record_count * 100

5. 离线批处理聚合算法
   系统使用 PySpark 对 Hive 中的交通数据进行离线批处理聚合，生成每日交通总览、行政区拥堵排行、道路类型汇总、拥堵路段 TopN、全天时间片趋势、早晚高峰汇总、路段速度散点、方向运行分析等指标。

6. 分组聚合算法
   系统大量使用 groupBy + agg 对交通数据进行分组统计。
   主要聚合指标包括：平均速度、样本车辆数、拥堵记录数、总记录数、拥堵率、车辆占比、拥堵次数等。

7. TopN 排名算法
   系统使用 Spark Window 窗口函数和 row_number() 实现拥堵路段排名。
   排序规则主要为：
   先按 congestion_count 降序，再按 avg_speed_kmh 升序，筛选出拥堵最严重的 TopN 路段。

8. 时间片转换算法
   系统将原始 time_slice 转换为具体时间标签。
   每个时间片表示 5 分钟，通过以下方式换算：
   minute_of_day = (time_slice - 1) * 5
   再转换为 HH:mm 格式，用于全天时间片趋势分析。

9. 早晚高峰识别算法
   系统根据时间片识别早高峰、晚高峰和平峰。
   规则为：
   time_slice 在 85 到 108 之间，判定为早高峰；
   time_slice 在 205 到 228 之间，判定为晚高峰；
   其他时间段判定为平峰。

10. 道路类型占比算法
    系统按道路类型统计样本车辆数，并计算不同道路类型的车辆占比。
    公式为：
    vehicle_rate = sample_vehicle_count / total_vehicle_count * 100

11. 实时流式计算算法
    系统通过 Kafka 接收实时交通数据，使用 Spark Structured Streaming 进行微批处理。
    每个 micro-batch 会计算实时核心指标、实时速度趋势、实时行政区拥堵排行、实时拥堵路段 TopN、Kafka 接入量趋势等。

12. 实时窗口统计算法
    系统对实时数据按批次窗口进行统计，计算窗口内平均速度、记录数、活跃路段数、拥堵路段数、样本车辆数等指标，用于前端大屏动态展示。

13. PySpark ML 特征构建算法
    系统使用 VectorAssembler 将多个交通特征组合成机器学习特征向量。
    使用的特征包括：
    avg_speed_kmh、congestion_rate、sample_vehicle_count、congestion_count、total_record_count、segment_length_meters。

14. 特征标准化算法
    系统使用 Spark MLlib 的 StandardScaler 对特征向量进行标准化处理，降低不同量纲对模型训练的影响。

15. KMeans 聚类算法
    系统使用 Spark MLlib 的 KMeans 对路段进行聚类分析。
    模型将道路路段按照速度、拥堵率、车辆数、拥堵次数、路段长度等特征划分为不同风险簇，用于生成 ML 聚类画像。

16. 聚类模型评估算法
    系统使用 ClusteringEvaluator 计算 silhouette 轮廓系数，用于评估 KMeans 聚类效果。
    轮廓系数越高，说明聚类区分度越好。

17. 路段风险评分算法
    系统设计了自定义风险评分公式，用于衡量路段风险程度。
    风险评分主要由拥堵率、速度压力、车流压力和阈值压力加权组成。
    公式为：
    risk_score = congestion_rate * 0.50 + speed_pressure * 0.20 + vehicle_pressure * 0.20 + threshold_pressure * 0.10

18. 风险等级划分算法
    系统根据风险评分划分风险等级。
    规则为：
    risk_score >= 70 为高风险；
    risk_score >= 45 且 < 70 为中风险；
    risk_score < 45 为低风险。

19. 智能推荐排序算法
    系统根据风险评分、拥堵次数和平均速度对路段进行推荐排序。
    排序规则为：
    先按 risk_score 降序，再按 congestion_count 降序，最后按 avg_speed_kmh 升序，推荐优先治理路段。

20. 短时风险预测展示算法
    前端大屏根据 ML 推荐风险、聚类风险、实时拥堵指数等数据构造未来 30 分钟风险趋势，用于展示短时风险预测可视化效果。

21. 地图热力与风险映射算法
    前端根据行政区拥堵率、路段平均速度、拥堵次数等指标，将交通状态映射到地图颜色、散点大小、连线强度和风险状态，实现全市路网运行态势可视化。

22. 自动刷新与动态数据更新算法
    前端大屏通过定时轮询后端接口，周期性刷新实时指标、Kafka 接入量、地图状态、ML 推荐结果等数据，使大屏具备动态变化效果。

23. 管理后台分页查询算法
    后台管理系统对 MySQL 表进行分页查询，支持多条件组合过滤。
    查询条件包括包含、等于、不等于、大于、大于等于、小于、小于等于、区间、为空、不为空等。

24. 登录认证与验证码校验算法
    后台管理系统支持密码登录和短信验证码登录。
    密码使用 MD5 加密存储；短信验证码使用手机号、场景和过期时间进行校验，支持登录验证码和忘记密码重置验证码。

25. 数据写入优化算法
    离线计算脚本支持通过开关跳过超大 ODS 明细表写入 MySQL，避免每次重复写入千万级数据；同时保留离线指标表计算与写入，提高整体运行效率。


## 运行视频
https://www.bilibili.com/video/BV1BvTy6kEFb

## 运行截图
![](1.png)
![](2.png)
![](3.png)
![](4.png)
![](5.png)
![](6.png)
![](7.png)
![](8.png)
![](9.png)
![](10.png)
![](11.png)
![](12.png)
![](13.png)
![](14.png)
![](15.png)
![](16.png)
![](17.png)
![](18.png)
![](19.png)
![](20.png)
![](21.png)
![](22.png)





















