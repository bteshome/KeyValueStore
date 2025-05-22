package com.bteshome.keyvaluestore.common.entities;

import com.bteshome.keyvaluestore.common.requests.StorageNodeJoinRequest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class StorageNode implements Serializable {
    private String id;
    private String host;
    private int port;
    private int grpcPort;
    private int managementPort;
    private String rack;
    private String storageDir;
    private StorageNodeStatus status;
    private Set<ReplicaAssignment> replicaAssignmentSet;
    @Serial
    private static final long serialVersionUID = 1L;

    public static StorageNode toStorageNode(StorageNodeJoinRequest request) {
        return new StorageNode( request.getId(),
                                request.getHost(),
                                request.getPort(),
                                request.getGrpcPort(),
                                request.getManagementPort(),
                                request.getRack(),
                                request.getStorageDir(),
                                StorageNodeStatus.INACTIVE,
                                new HashSet<>()
        );
    }

    public boolean isActive() {
        return status == StorageNodeStatus.ACTIVE;
    }

    public int getNumOwnedReplicas() {
        return replicaAssignmentSet.stream().filter(ReplicaAssignment::isLeader).collect(Collectors.toSet()).size();
    }

    public boolean hasReplicasFor(String tableName) {
        return replicaAssignmentSet.stream().anyMatch(replicaAssignment -> replicaAssignment.getTableName().equals(tableName));
    }

    public boolean hasAReplicaFor(String tableName, int partition) {
        return replicaAssignmentSet.stream().anyMatch(replicaAssignment ->
                replicaAssignment.getTableName().equals(tableName) &&
                        replicaAssignment.getPartitionIid() == partition);
    }

    public ReplicaAssignment getReplicaAssignmentFor(String tableName, int partition) {
        return replicaAssignmentSet.stream().filter(replicaAssignment ->
                replicaAssignment.getTableName().equals(tableName) &&
                        replicaAssignment.getPartitionIid() == partition).findFirst().get();
    }

    public StorageNode copy() {
        return new StorageNode( id,
                                host,
                                port,
                                grpcPort,
                                managementPort,
                                rack,
                                storageDir,
                                status,
                                replicaAssignmentSet.stream().map(ReplicaAssignment::copy).collect(Collectors.toSet()));
    }
}
