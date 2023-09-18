package com.kafka.rebalance.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReplicaPlacementInfo {
    private final int brokerId;
    private final String logdir;

    public ReplicaPlacementInfo(int brokerId, String logdir) {
        this.brokerId = brokerId;
        this.logdir = logdir;
    }

    public ReplicaPlacementInfo(Integer brokerId) {
        this(brokerId, null);
    }

    public Integer brokerId() {
        return brokerId;
    }

    public String logdir() {
        return logdir;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ReplicaPlacementInfo)) {
            return false;
        }
        ReplicaPlacementInfo info = (ReplicaPlacementInfo) o;
        return brokerId == info.brokerId && Objects.equals(logdir, info.logdir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, logdir);
    }

    @Override
    public String toString() {
        if (logdir == null) {
            return String.format("{Broker: %d}", brokerId);
        } else {
            return String.format("{Broker: %d, Logdir: %s}", brokerId, logdir);
        }
    }
}
