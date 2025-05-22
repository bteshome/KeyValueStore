package com.bteshome.keyvaluestore.client.writers;

import com.bteshome.keyvaluestore.client.ClientException;
import com.bteshome.keyvaluestore.client.KeyToPartitionMapper;
import com.bteshome.keyvaluestore.client.ClientRetriableException;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.common.*;
import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

@Component
@Slf4j
public class ItemWriter {
    @Autowired
    KeyToPartitionMapper keyToPartitionMapper;
    @Autowired
    WebClient webClient;
    // TODO - this will be updated.
    private static final int VALUE_BYTES_MAX = 2015;

    public Mono<ItemPutResponse> putString(ItemWrite<String> request) {
        request.setValue(Validator.notEmpty(request.getValue(), "Value"));
        byte[] valueBytes = request.getValue().getBytes();
        return put(request.getTable(),
                request.getPartitionKey(),
                request.getKey(),
                request.getAck(),
                valueBytes,
                request.getIndexKeys(),
                request.getMaxRetries(),
                request.getPreviousVersion());
    }

    public <T extends Serializable> Mono<ItemPutResponse> putObject(ItemWrite<T> request) {
        byte[] valueBytes = JsonSerDe.serializeToBytes(request.getValue());
        return put(request.getTable(),
                request.getPartitionKey(),
                request.getKey(),
                request.getAck(),
                valueBytes,
                request.getIndexKeys(),
                request.getMaxRetries(),
                request.getPreviousVersion());
    }

    public <T extends Serializable> Mono<ItemPutResponse> putBytes(ItemWrite<byte[]> request) {
        return put(request.getTable(),
                request.getPartitionKey(),
                request.getKey(),
                request.getAck(),
                request.getValue(),
                request.getIndexKeys(),
                request.getMaxRetries(),
                request.getPreviousVersion());
    }

    private Mono<ItemPutResponse> put(String table,
                                      String partitionKey,
                                      String key,
                                      AckType ack,
                                      byte[] valueBytes,
                                      Map<String, String> indexes,
                                      int maxRetries,
                                      LogPosition previousVersion) {
        if (Strings.isBlank(partitionKey))
            partitionKey = key;

        ItemPutRequest itemPutRequest = new ItemPutRequest();
        itemPutRequest.setTable(Validator.notEmpty(table, "Table name"));
        key = Validator.notEmpty(key, "Key");
        itemPutRequest.getItems().add(new Item(key, partitionKey, valueBytes, indexes, previousVersion));
        itemPutRequest.setAck(ack);

        if (previousVersion != null && !previousVersion.equals(LogPosition.ZERO))
            itemPutRequest.setWithVersionCheck(true);

        int partition = keyToPartitionMapper.map(table, partitionKey);
        itemPutRequest.setPartition(partition);

        return put(itemPutRequest, partition, maxRetries);
    }

    Mono<ItemPutResponse> put(ItemPutRequest itemPutRequest,
                              int partition,
                              int maxRetries) {
        for (Item item : itemPutRequest.getItems()) {
            if (item.getValue().length > VALUE_BYTES_MAX)
                throw new ClientException("Value length exceeds %d bytes.".formatted(VALUE_BYTES_MAX));
        }

        String endpoint = MetadataCache.getInstance().getLeaderEndpoint(itemPutRequest.getTable(), partition);
        if (endpoint == null)
            return Mono.error(new ClientException("Table '%s' partition '%s' is offline.".formatted(itemPutRequest.getTable(), partition)));
        return put(endpoint, itemPutRequest, maxRetries);
    }

    private Mono<ItemPutResponse> put(String endpoint, ItemPutRequest itemPutRequest, int maxRetries) {
        return webClient
                .post()
                .uri("http://%s/api/items/put/".formatted(endpoint))
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(itemPutRequest)
                .retrieve()
                .toEntity(ItemPutResponse.class)
                .map(HttpEntity::getBody)
                .flatMap(response -> {
                    if (response.getHttpStatusCode() == HttpStatus.MOVED_PERMANENTLY.value()) {
                        return put(response.getLeaderEndpoint(), itemPutRequest, maxRetries);
                    } else if (response.getHttpStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR.value() ||
                               response.getHttpStatusCode() == HttpStatus.REQUEST_TIMEOUT.value()) {
                        return Mono.error(new ClientRetriableException("Unexpected status code: %s, %s".formatted(response.getHttpStatusCode(), response.getErrorMessage())));
                    } else {
                        return Mono.just(response);
                    }
                })
                .retryWhen(Retry.fixedDelay(maxRetries, Duration.ofSeconds(2)).filter(ex -> ex instanceof ClientRetriableException));
    }
}
