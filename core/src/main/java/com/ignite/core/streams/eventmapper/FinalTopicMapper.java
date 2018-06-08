package com.ignite.core.streams.eventmapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created by ashok on 6/6/2018.
 */
public class FinalTopicMapper {

    public JsonNode newFinalTopicMapper(JsonNode eventData) {

        ObjectNode newFinalTopicMapper = JsonNodeFactory.instance.objectNode();

        newFinalTopicMapper.put("ItemNumber", eventData.get("ItemNumber").asInt());
        newFinalTopicMapper.put("Store", eventData.get("Store").asInt());
        newFinalTopicMapper.put("Quantity", (eventData.get("Quantity").asDouble()));
        newFinalTopicMapper.put("InventoryState", eventData.get("InventoryState").asText());
        newFinalTopicMapper.put("IdempotentKey", eventData.get("IdempotentKey").asText());
        newFinalTopicMapper.put("EventTime", eventData.get("EventTime").asText());
        newFinalTopicMapper.put("EventType", eventData.get("EventType").asText());
        newFinalTopicMapper.put("TransactionID", eventData.get("TransactionID").asInt());
        return newFinalTopicMapper;

    }
}
