package com.ignite.core.streams.eventmapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created by ashok on 4/24/2018.
 */
public class StoreReceiveEventMapper {
    public JsonNode newStoreReceiveEventMapper(JsonNode eventData) {
        // create a new balance json object
        ObjectNode newStoreReceiveEventMapper = JsonNodeFactory.instance.objectNode();

        String eventType =  eventData.get("EventType").asText();

        newStoreReceiveEventMapper.put("ItemNumber", eventData.get("Item").asInt());
        newStoreReceiveEventMapper.put("Store", eventData.get("Store").asInt());
        newStoreReceiveEventMapper.put("Quantity", (0+(eventData.get("ReceiveQty").asDouble())));
        newStoreReceiveEventMapper.put("InventoryState", "Available");
        newStoreReceiveEventMapper.put("IdempotentKey", eventData.get("Item").toString()+eventData.get("Store").toString()+eventData.get("TimeStamp")+eventData.get("TransactionID").toString());
        newStoreReceiveEventMapper.put("EventTime", eventData.get("TimeStamp"));
        newStoreReceiveEventMapper.put("EventType", eventData.get("EventType").asText());
        newStoreReceiveEventMapper.put("TransactionID", eventData.get("TransactionID").asInt());
        return newStoreReceiveEventMapper;

    }
}
