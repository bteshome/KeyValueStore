package com.bteshome.keyvaluestore.client.clientrequests;

import com.bteshome.keyvaluestore.client.requests.AckType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class BatchDelete {
    private String table;
    private AckType ack;
    private int maxRetries;
    private final List<String> keys = new ArrayList<>();
    private final List<String> partitionKeys = new ArrayList<>();
}