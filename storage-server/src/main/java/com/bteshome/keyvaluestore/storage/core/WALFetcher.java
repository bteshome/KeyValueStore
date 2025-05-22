package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.storage.requests.WALFetchRequest;
import com.bteshome.keyvaluestore.storage.responses.WALFetchPayloadType;
import com.bteshome.keyvaluestore.storage.responses.WALFetchResponse;
import com.bteshome.keyvaluestore.storage.entities.DataSnapshot;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Slf4j
public class WALFetcher {
    public static void fetch(PartitionState partitionState,
                             String nodeId,
                             String table,
                             int partition,
                             String leaderEndpoint,
                             int maxNumRecords,
                             WebClient webClient) {
        try {
            log.trace("WAL fetcher triggered for table '{}' partition '{}'.", table, partition);

            if (leaderEndpoint == null) {
                log.trace("No leader for table '{}' partition '{}'. Skipping fetch.", table, partition);
                return;
            }

            LogPosition lastFetchOffset = partitionState.getOffsetState().getEndOffset();

            WALFetchRequest request = new WALFetchRequest(
                    nodeId,
                    table,
                    partition,
                    lastFetchOffset,
                    maxNumRecords
            );

            Mono<ResponseEntity<WALFetchResponse>> mono = webClient
                    .post()
                    .uri("http://%s/api/wal/fetch/".formatted(leaderEndpoint))
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .bodyValue(request)
                    .retrieve()
                    .toEntity(WALFetchResponse.class);

            mono.subscribe(responseEntity -> {
                WALFetchResponse response = responseEntity.getBody();

                if (response == null) {
                    // TODO - change to error
                    log.trace("Error fetching WAL for table '{}' partition '{}'. Response is null.", table, partition);
                    return;
                }

                if (response.getHttpStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR.value() || response.getHttpStatusCode() == HttpStatus.BAD_REQUEST.value()) {
                    // TODO - change to error
                    log.trace("Error fetching WAL for table '{}' partition '{}'. Http status: {}, error: {}.",
                            table,
                            partition,
                            response.getHttpStatusCode(),
                            response.getErrorMessage());
                    return;
                }

                if (response.getHttpStatusCode() == HttpStatus.CONFLICT.value()) {
                    log.info("Received a truncate request from the new leader for table '{}' partition '{}'. Truncating to offset '{}'.",
                            table,
                            partition,
                            response.getTruncateToOffset());
                    partitionState.getWal().truncateToBeforeInclusive(response.getTruncateToOffset());
                    return;
                }

                if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
                    if (response.getPayloadType().equals(WALFetchPayloadType.LOG)) {
                        partitionState.appendLogEntries(
                                response.getEntries(),
                                response.getCommitedOffset());

                        log.trace("Fetched WAL for table '{}' partition '{}' lastFetchedOffset '{}'. entries size={}, commited offset={}.",
                                table,
                                partition,
                                lastFetchOffset,
                                response.getEntries().size(),
                                response.getCommitedOffset());
                    } else {
                        byte[] dataSnapshotBytes = response.getDataSnapshotBytes();
                        DataSnapshot dataSnapshot = JavaSerDe.deserialize(dataSnapshotBytes);
                        partitionState.applyDataSnapshot(dataSnapshot);

                        log.trace("Fetched data snapshot for table '{}' partition '{}' lastFetchedOffset '{}', last snapshot committed offset={}.",
                                table,
                                partition,
                                lastFetchOffset,
                                dataSnapshot.getLastCommittedOffset());
                    }
                }
            });
        } catch (Exception e) {
            // TODO - change to error
            log.trace("Error fetching WAL for table '{}' partition '{}'.", table, partition, e);
        }
    }
}
