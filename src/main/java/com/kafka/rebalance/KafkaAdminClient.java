package com.kafka.rebalance;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
public class KafkaAdminClient {


    private AdminClient adminClient;

    public KafkaAdminClient(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public KafkaAdminClient(String user, String password, String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        if(user != null) {
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "SCRAM-SHA-256");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + user + "\" password=\"" + password + "\";");
        }
        this.adminClient = AdminClient.create(props);
    }


    public void close() {
        if(this.adminClient != null) {
            this.adminClient.close();
        }
    }


    public Map<Integer, Map<String, LogDirDescription>> getLogDirDescriptions() {
        try {
            DescribeClusterResult describeClusterResult = adminClient.describeCluster();
            Collection<Node> nodes = describeClusterResult.nodes().get(5, TimeUnit.SECONDS);
            List<Integer> nodeIdList = nodes.stream().map(Node::id).collect(Collectors.toList());
            DescribeLogDirsResult describeLogDirsResult = adminClient.describeLogDirs(nodeIdList);
            Map<Integer, Map<String, LogDirDescription>> map = describeLogDirsResult.allDescriptions().get(15, TimeUnit.SECONDS);
            return map;
        } catch (TimeoutException e) {
            log.error("error", e);
            throw new KafkaException("Timed out waiting to send the call to kafka");
        } catch (Exception e) {
            log.error("error", e);
            throw new KafkaException(e.getMessage());
        }

    }



}
