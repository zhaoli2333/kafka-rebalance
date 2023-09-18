package com.kafka.rebalance;

import com.kafka.rebalance.models.BalanceTask;
import com.kafka.rebalance.models.LogDir;
import com.kafka.rebalance.models.Replica;
import com.kafka.rebalance.models.ReplicaPlacementInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class KafkaRebalanceService {

    private List<LogDir> logDirs;
    private Map<Integer, Map<String, LogDir>> brokerLogDirMaps;
    private double balanceUpperThreshold = 0D;
    private double balanceLowerThreshold = 0D;
    private long maxReplicaSize;

    private Set<TopicPartition> movedTopicPartitons;

    public KafkaRebalanceService(Map<Integer, Map<String, LogDirDescription>> logDirDescriptionMap, double balancePercentage, long maxReplicaSize) {
        logDirDescriptionMap.entrySet().stream().forEach(entry -> {
            Integer brokerId= entry.getKey();
            entry.getValue().entrySet().stream().forEach(logDir -> {
                String logDirName = logDir.getKey();
                Long totalSize = logDir.getValue().replicaInfos().values().stream().mapToLong(ReplicaInfo::size).sum();
                log.info("Before optimized result preview, broker {}, logDir {}, totalSizeInGB {}GB", brokerId, logDirName, totalSize/(1024*1024*1024));
            });
        });

        List<LogDir> logDirList = new ArrayList<>();
        Map<Integer, Map<String, LogDir>> brokerLogDirMaps = new HashMap<>();
        logDirDescriptionMap.entrySet().stream().forEach(entry -> {
            Integer brokerId = entry.getKey();
            brokerLogDirMaps.put(brokerId, new HashMap<>());
            entry.getValue().entrySet().forEach(innerEntry -> {
                LogDir logDir = new LogDir(innerEntry.getKey(), brokerId, innerEntry.getValue().replicaInfos());
                logDirList.add(logDir);
                brokerLogDirMaps.get(brokerId).put(logDir.getName(), logDir);
            });
        });
        this.logDirs = logDirList;
        this.brokerLogDirMaps = brokerLogDirMaps;
        double avgUtilization = this.logDirs.stream().mapToLong(LogDir::getTotalSize).average().getAsDouble();
        this.balanceUpperThreshold = avgUtilization * (1 + balancePercentage);
        this.balanceLowerThreshold = avgUtilization * (1 - balancePercentage);
        this.maxReplicaSize = maxReplicaSize;
        this.movedTopicPartitons = new HashSet<>();
    }


    public String rebalancePreview() {

        Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaPlacementInfos = getReplicaDistribution(this.logDirs);
        for(LogDir logDir : this.logDirs) {
            rebalanceForBroker(logDir);
        }

        // output optimized result
        Map<Integer, List<LogDir>> logDirMap = this.logDirs.stream().collect(Collectors.groupingBy(LogDir::getBrokerId));
        logDirMap.entrySet().stream().forEach(entry -> {
            Integer brokerId= entry.getKey();
            entry.getValue().stream().forEach(logDir -> {
                log.info("optimized result preview, broker {}, logDir {}, totalSizeInGB {}GB", brokerId, logDir.getName(), logDir.getTotalSize()/(1024*1024*1024));
            });
        });

        Map<TopicPartition, List<ReplicaPlacementInfo>> optimizedReplicaPlacementInfos = getReplicaDistribution(this.logDirs);
        List<BalanceTask> balanceTasks = diff(initReplicaPlacementInfos, optimizedReplicaPlacementInfos);
        return resultJsonTask(balanceTasks);
    }

    private String resultJsonTask(List<BalanceTask> balanceTasks) {
        try {
            Map<String, Object> reassign = new HashMap<>();
            reassign.put("partitions", balanceTasks);
            reassign.put("version", 1);
            return new ObjectMapper().writeValueAsString(reassign);
        } catch (Exception e) {
            log.error("result task json process error", e);
        }
        return "{}";
    }


    private void rebalanceForBroker(LogDir logDir) {
        double utilization = logDir.getTotalSize();
        boolean requireLessLoad = utilization > this.balanceUpperThreshold;
        boolean requireMoreLoad = utilization < this.balanceLowerThreshold;
        if (!requireMoreLoad && !requireLessLoad) {
            return;
        }
        boolean balanced = true;
        if (requireLessLoad) {
            balanced = rebalanceByMovingLoadOut(logDir, this.logDirs);
        } else if (requireMoreLoad) {
            balanced = rebalanceByMovingLoadIn(logDir, this.logDirs);
        }
        if (balanced) {
            log.info("Successfully balanced disk for broker {}, logDir {} by moving leaders and replicas.", logDir.getBrokerId(), logDir.getName());
        } else {
            log.info("Failed to balanced disk for broker {}, logDir {} by moving leaders and replicas.", logDir.getBrokerId(), logDir.getName());
        }
    }

    private List<BalanceTask> diff(Map<TopicPartition, List<ReplicaPlacementInfo>> initReplicaPlacementInfos, Map<TopicPartition, List<ReplicaPlacementInfo>> optimizedReplicaPlacementInfos) {
        List<BalanceTask> balanceTasks = new ArrayList<>();
        for(Map.Entry<TopicPartition, List<ReplicaPlacementInfo>> entry : initReplicaPlacementInfos.entrySet()) {
            TopicPartition tp = entry.getKey();
            List<ReplicaPlacementInfo> initReplicaPlacementInfo = entry.getValue();
            List<ReplicaPlacementInfo> optimizedReplicaPlacementInfo = optimizedReplicaPlacementInfos.get(tp);
            if(initReplicaPlacementInfo.equals(optimizedReplicaPlacementInfo)) {
                continue;
            }
            if(!balanceCheck(initReplicaPlacementInfo, optimizedReplicaPlacementInfo)) {
                log.warn("illegal balance, topicPartition {}, before {}, after {}", tp, initReplicaPlacementInfo, optimizedReplicaPlacementInfo);
                continue;
            }
            BalanceTask balanceTask = new BalanceTask(
                    tp.topic(),
                    tp.partition(),
                    optimizedReplicaPlacementInfo.stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toList()),
                    optimizedReplicaPlacementInfo.stream().map(ReplicaPlacementInfo::logdir).collect(Collectors.toList())
                    );
            balanceTasks.add(balanceTask);
        }
        return balanceTasks;
    }

    private boolean balanceCheck(List<ReplicaPlacementInfo> beforeReplicaPlacementInfos, List<ReplicaPlacementInfo> afterReplicaPlacementInfos) {
        for(ReplicaPlacementInfo beforePlacement : beforeReplicaPlacementInfos) {
            for(ReplicaPlacementInfo afterPlacement : afterReplicaPlacementInfos) {
                if(beforePlacement.brokerId() == afterPlacement.brokerId()
                        && !beforePlacement.logdir().equals(afterPlacement.logdir())) {
                    return false;
                }
            }
        }
        return true;
    }

    public Map<TopicPartition, List<ReplicaPlacementInfo>> getReplicaDistribution(List<LogDir> logDirs) {
        Map<TopicPartition, List<ReplicaPlacementInfo>> replicaDistribution = new HashMap<>();
        for (LogDir logDir : logDirs) {
            logDir.getReplicas().stream().forEach(j -> replicaDistribution.computeIfAbsent(j.getTopicPartition(), k -> new ArrayList<>())
                        .add(new ReplicaPlacementInfo(j.getBrokerId(), j.getLogDir().getName())));

        }
        return replicaDistribution;
    }


    private boolean rebalanceByMovingLoadOut(LogDir logDir, List<LogDir> candidateLogDirs) {

        SortedSet<LogDir> sortedCandidateLogDirs = sortedLogDirsFor(candidateLogDirs, false);
        SortedSet<Replica> replicasToMove = sortedReplicasFor(logDir.getReplicas(), true, this.maxReplicaSize);

        for (Replica replica : replicasToMove) {
            LogDir acceptedLogDir = maybeApplyBalancingAction(replica, sortedCandidateLogDirs, this.balanceUpperThreshold, this.balanceLowerThreshold);

            if (acceptedLogDir != null) {
                if (logDir.getTotalSize() < this.balanceUpperThreshold) {
                    return true;
                }
                // Remove and reinsert the logDir so the order is correct.
                sortedCandidateLogDirs.removeIf(b -> b.getBrokerId() == acceptedLogDir.getBrokerId() && b.getName().equals(acceptedLogDir.getName()));
                if (acceptedLogDir.getTotalSize() < balanceUpperThreshold) {
                    sortedCandidateLogDirs.add(acceptedLogDir);
                }
            }
        }

        return false;

    }

    private boolean rebalanceByMovingLoadIn(LogDir logDir, List<LogDir> candidateLogDirs) {
        SortedSet<LogDir> sortedCandidateLogDirs = sortedLogDirsFor(candidateLogDirs, true);
        Iterator<LogDir> candidateLogDirsIt = sortedCandidateLogDirs.iterator();
        LogDir nextLogDir = null;
        while(true) {
            LogDir candidateLogDir;
            if (nextLogDir != null) {
                candidateLogDir = nextLogDir;
                nextLogDir = null;
            } else if (candidateLogDirsIt.hasNext()) {
                candidateLogDir = candidateLogDirsIt.next();
            } else {
                break;
            }
            // try to move replicas in candidateLogDir to destinationLogDir
            SortedSet<Replica> replicasToMove = sortedReplicasFor(candidateLogDir.getReplicas(), true, this.maxReplicaSize);
            for (Replica replica : replicasToMove) {
                LogDir acceptedLogDir = maybeApplyBalancingAction(replica, Collections.singletonList(logDir), this.balanceUpperThreshold, this.balanceLowerThreshold);
                if (acceptedLogDir != null) {
                    if (logDir.getTotalSize() > this.balanceLowerThreshold) {
                        return true;
                    }
                    if (candidateLogDirsIt.hasNext() || nextLogDir != null) {
                        if (nextLogDir == null) {
                            nextLogDir = candidateLogDirsIt.next();
                        }
                        if (candidateLogDir.getTotalSize() < nextLogDir.getTotalSize()) {
                            break;
                        }
                    }
                }
            }
        }
        return false;
    }



    private LogDir maybeApplyBalancingAction(Replica replica, Collection<LogDir> sortedCandidateLogDirs, double balanceUpperThreshold, double balanceLowerThreshold) {
        // 如果该topicPartiton的其他副本已经参与过迁移，则跳过，避免出现tp-replica1:[broker1/data1 -> broker2/data3], tp-replica2:[broker4/data2 -> broker1/data2]的情况（同一个tp的不同副本先从broker1迁走，随后另一个副本又迁回broker1），触发kafka bug:https://issues.apache.org/jira/browse/KAFKA-9087
        if(movedTopicPartitons.contains(replica.getTopicPartition())) {
            return null;
        }
        for(LogDir logDir : sortedCandidateLogDirs) {
            // 待迁移副本如果和目标logDir属于同一个broker，则跳过，因为同broker不同目录之间复制会触发kafka bug:https://issues.apache.org/jira/browse/KAFKA-9087 而导致复制失败
            if (replica.getBrokerId() == logDir.getBrokerId()) {
                continue;
            }
            // 均衡的副本如果存在于当前lodDir上则跳过
            if (logDir.contains(replica.getTopicPartition())) {
                continue;
            }
            // 不允许该logDir所在的broker有该topicPartition的其他副本
            if(checkIfBrokerContainsReplica(replica, logDir)) {
                continue;
            }
            // 迁移后均衡条件不满足，则跳过
            double sourceUtilization = replica.getLogDir().getTotalSize() - replica.getSize();
            double destinationUtilization = logDir.getTotalSize() + replica.getSize();
            if (sourceUtilization < balanceLowerThreshold || destinationUtilization > balanceUpperThreshold) {
                continue;
            }
            // 迁移
            relocateReplica(replica, logDir);
            return logDir;
        }
        return null;
    }

    private boolean checkIfBrokerContainsReplica(Replica replica, LogDir logDir) {
        Map<String, LogDir> brokerLogDirs = this.brokerLogDirMaps.get(logDir.getBrokerId());
        for(Map.Entry<String, LogDir> entry : brokerLogDirs.entrySet()) {
            if(entry.getValue().getReplicaMap().containsKey(replica.getTopicPartition())) {
                return true;
            }
        }
        return false;
    }

    private void relocateReplica(Replica replica, LogDir destinationLogDir) {
        LogDir sourceLogDir = replica.getLogDir();
        sourceLogDir.removeReplica(replica);
        destinationLogDir.addReplica(replica);
        this.movedTopicPartitons.add(replica.getTopicPartition());
    }


    public SortedSet<LogDir> sortedLogDirsFor(List<LogDir> logDirs, boolean reverse) {
        Comparator<LogDir> comparator =
                Comparator.<LogDir>comparingDouble(LogDir::getTotalSize);
        if (reverse)
            comparator = comparator.reversed();
        SortedSet<LogDir> sortedLogDirs = new TreeSet<>(comparator);
        sortedLogDirs.addAll(logDirs);
        return sortedLogDirs;
    }

    public SortedSet<Replica> sortedReplicasFor(List<Replica> replicas, boolean reverse, Long maxSize) {
        Comparator<Replica> comparator =
                Comparator.<Replica>comparingDouble(Replica::getSize);
        if (reverse)
            comparator = comparator.reversed();
        SortedSet<Replica> sortedReplicas = new TreeSet<>(comparator);
        sortedReplicas.addAll(replicas.stream().filter(e -> e.getSize() > 0).filter(e -> e.getSize() < maxSize).collect(Collectors.toList()));
        return sortedReplicas;
    }


}
