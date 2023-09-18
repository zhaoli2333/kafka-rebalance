package com.kafka.rebalance.models;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LogDir {

    private String name;
    private Integer brokerId;
    private Map<TopicPartition, Replica> replicaMap;
    private List<Replica> replicas;
    private Long totalSize;

    public LogDir(String name, Integer brokerId, Map<TopicPartition, ReplicaInfo> replicaInfos) {
        this.name = name;
        this.brokerId = brokerId;
        this.replicas = replicaInfos.entrySet().stream().map(entry -> {
            return new Replica(entry.getKey(), brokerId, this, entry.getValue().size());
        }).collect(Collectors.toList());
        this.replicaMap = this.replicas.stream().collect(Collectors.toMap(Replica::getTopicPartition, replica -> replica));
    }

    public Long getTotalSize() {
        if(this.totalSize != null) {
            return this.totalSize;
        }
        this.totalSize = this.replicas.stream().mapToLong(replica -> replica.getSize()).sum();
        return this.totalSize;
    }

    public boolean contains(TopicPartition topicPartition) {
        return replicaMap.containsKey(topicPartition);
    }


    public void removeReplica(Replica replica) {
        this.replicaMap.remove(replica.getTopicPartition());
        this.replicas.remove(replica);
        this.totalSize = this.totalSize - replica.getSize();
    }

    public void addReplica(Replica replica) {
        if(!this.replicaMap.containsKey(replica.getTopicPartition())) {
            this.replicaMap.put(replica.getTopicPartition(), replica);
            this.replicas.add(replica);
            this.totalSize = this.totalSize + replica.getSize();
        }
        replica.setLogDir(this);
        replica.setBrokerId(this.getBrokerId());
    }

}
