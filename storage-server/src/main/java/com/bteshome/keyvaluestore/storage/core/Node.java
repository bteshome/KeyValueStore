package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.MetadataClientBuilder;
import com.bteshome.keyvaluestore.common.MetadataClientSettings;
import com.bteshome.keyvaluestore.common.ResponseStatus;
import com.bteshome.keyvaluestore.common.requests.StorageNodeJoinRequest;
import com.bteshome.keyvaluestore.common.responses.GenericResponse;
import com.bteshome.keyvaluestore.storage.common.StorageSettings;
import com.bteshome.keyvaluestore.storage.states.State;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Component
@Slf4j
public class Node implements CommandLineRunner {
    @Autowired
    MetadataClientBuilder metadataClientBuilder;
    @Autowired
    MetadataClientSettings metadataClientSettings;
    @Autowired
    StorageSettings storageSettings;
    @Autowired
    HeartbeatSender heartbeatSender;
    @Autowired
    StorageNodeMetadataRefresher storageNodeMetadataRefresher;
    @Autowired
    State state;

    @Override
    public void run(String... args) throws IOException {
        log.info("Starting server ...");
        storageSettings.print();
        metadataClientSettings.print();

        try (RaftClient client = this.metadataClientBuilder.createRaftClient()) {
            StorageNodeJoinRequest request = new StorageNodeJoinRequest(
                    storageSettings.getNode().getId(),
                    storageSettings.getNode().getHost(),
                    storageSettings.getNode().getPort(),
                    storageSettings.getNode().getGrpcPort(),
                    storageSettings.getNode().getManagementPort(),
                    storageSettings.getNode().getRack(),
                    storageSettings.getNode().getStorageDir());
            log.info("Trying to join cluster '{}' with node id: '{}'", client.getGroupId().getUuid(), request.getId());
            final RaftClientReply reply = client.io().send(request);
            if (reply.isSuccess()) {
                String messageString = reply.getMessage().getContent().toString(StandardCharsets.UTF_8);
                GenericResponse response = ResponseStatus.toGenericResponse(messageString);
                if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                    log.info(response.getMessage());
                    storageNodeMetadataRefresher.fetch();
                    storageNodeMetadataRefresher.schedule();
                    heartbeatSender.schedule();
                    state.initialize();
                } else {
                    log.error(response.getMessage());
                }
            } else {
                log.error("Error joining cluster: ", reply.getException());
            }
        }
    }
}
