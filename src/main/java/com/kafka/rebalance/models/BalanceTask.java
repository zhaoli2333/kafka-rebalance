package com.kafka.rebalance.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class BalanceTask {
    private String topic;
    private int partition;
    //副本分配列表
    private List<Integer> replicas;
    @JsonProperty(value = "log_dirs")
    private List<String> logDirs;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public List<Integer> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<Integer> replicas) {
        this.replicas = replicas;
    }

    @Override
    public String toString() {
        return "BalanceTask{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", replicas=" + replicas +
                '}';
    }
}
