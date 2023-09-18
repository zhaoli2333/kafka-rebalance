package com.kafka.rebalance;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.LogDirDescription;

import java.util.Map;

@Slf4j
public class Main {
    public static void main(String[] args) {
        // 默认允许误差范围+-5%
        Double balancePercentage = 0.05;
        // 默认50G以下的副本参与计算
        Long maxReplicaSizeInGB = 50L;
        KafkaAdminClient adminClient = new KafkaAdminClient("", "", "localhost:9092");
        Map<Integer, Map<String, LogDirDescription>> map = adminClient.getLogDirDescriptions();
        KafkaRebalanceService kafkaRebalanceService = new KafkaRebalanceService(map, balancePercentage, maxReplicaSizeInGB * 1024 * 1024 * 1024L);
        log.info("rebalance result preview: ", kafkaRebalanceService.rebalancePreview());
    }
}
