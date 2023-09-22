# Kafka LogDir级别的副本均衡算法


## 目标
由于Kafka一台broker有多个LogDir存储目录，在做副本均衡时不仅要均衡每台broker的磁盘使用总量，也要均衡每个LogDir的磁盘用量。

## 其他必要条件
* 尽量用最少的副本迁移来达成均衡的目的。
* 对于一些超大topic，本身已经做过均衡（分区均匀的分布在所有broker上），不需要参与均衡。
* 不能出现同broker不同LogDir之间的迁移，否则会触发bug [KAFKA-9087](https://issues.apache.org/jira/browse/KAFKA-9087) 导致迁移失败。
* 同一个TopicPartition的副本应该均匀分散到不同broker。
* prefered leader副本尽量均匀的分散到所有broker节点

## 参数
* **balancePercentage**: 允许误差范围,默认+-5%
* **maxReplicaSizeInGB**: 参与均衡的最大副本大小，默认50GB

## 算法思想
* 获取所有LogDir的大小和其包含的replicas，并对replicas从大到小排序。
* 计算disk usage的平均值。
* 对于usage高于平均值+误差值的，循环将其replicas移出到其他LogDir，直到usage小于误差范围的上限为止。
* 对于usage低于平均值-误差值的，循环将比其大的LogDir上的replicas移动到该LogDir上，直到usage大于误差范围的下限为止。
* 每次移出或者移入replica时，都要判断迁移操作是否满足以上的必要条件，不满足则跳过。

## 未来优化
* 未考虑LogDir大小不一致的场景，后续可以将disk大小改为使用率百分比，但需要提前录入每块LogDir的总容量。
