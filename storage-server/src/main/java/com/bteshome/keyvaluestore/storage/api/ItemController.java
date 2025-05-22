package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.client.adminrequests.ItemCountAndOffsetsRequest;
import com.bteshome.keyvaluestore.client.adminrequests.ItemVersionsGetRequest;
import com.bteshome.keyvaluestore.client.adminresponses.ItemCountAndOffsetsResponse;
import com.bteshome.keyvaluestore.client.adminresponses.ItemVersionsGetResponse;
import com.bteshome.keyvaluestore.client.requests.*;
import com.bteshome.keyvaluestore.client.responses.*;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/items")
@RequiredArgsConstructor
@Slf4j
public class ItemController {
    @Autowired
    State state;

    @PostMapping("/get/")
    public Mono<ResponseEntity<ItemGetResponse>> getItem(@RequestBody ItemGetRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return Mono.just(ResponseEntity.ok(ItemGetResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return partitionState.getItem(request);
    }

    @PostMapping("/get-versions/")
    public Mono<ResponseEntity<ItemVersionsGetResponse>> getItem(@RequestBody ItemVersionsGetRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return Mono.just(ResponseEntity.ok(ItemVersionsGetResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return partitionState.getItemVersions(request);
    }

    @PostMapping("/list/")
    public Mono<ResponseEntity<ItemListResponse>> listItems(@RequestBody ItemListRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return partitionState.listItems(request);
    }

    @PostMapping("/query/")
    public Mono<ResponseEntity<ItemListResponse>> queryForItems(@RequestBody ItemQueryRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return Mono.just(ResponseEntity.ok(ItemListResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return partitionState.queryForItems(request);
    }

    @PostMapping("/put/")
    public Mono<ResponseEntity<ItemPutResponse>> putItem(@RequestBody ItemPutRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return Mono.just(ResponseEntity.ok(ItemPutResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return partitionState.putItems(request);
    }

    @PostMapping("/delete/")
    public Mono<ResponseEntity<ItemDeleteResponse>> deleteItem(@RequestBody ItemDeleteRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return Mono.just(ResponseEntity.ok(ItemDeleteResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return partitionState.deleteItems(request);
    }

    @PostMapping("/count-and-offsets/")
    public Mono<ResponseEntity<ItemCountAndOffsetsResponse>> countItems(@RequestBody ItemCountAndOffsetsRequest request) {
        PartitionState partitionState = state.getPartitionState(request.getTable(), request.getPartition());

        if (partitionState == null) {
            return Mono.just(ResponseEntity.ok(ItemCountAndOffsetsResponse.builder()
                    .httpStatusCode(HttpStatus.NOT_FOUND.value())
                    .build()));
        }

        return partitionState.countItems();
    }
}