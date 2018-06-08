package com.ignite.core.streams.eventmapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created by ashok on 4/24/2018.
 */
public class SaleEventMapper {

    public JsonNode newSaleEventMapper(JsonNode eventData) {
        // create a new balance json object
        ObjectNode newSaleEventMapper = JsonNodeFactory.instance.objectNode();

        String eventType =  eventData.get("EventType").asText();

        newSaleEventMapper.put("ItemNumber", eventData.get("Item").asInt());
        newSaleEventMapper.put("Store", eventData.get("Store").asInt());
        newSaleEventMapper.put("Quantity", (0-(eventData.get("SaleQty").asDouble())));
        newSaleEventMapper.put("InventoryState", "Available");
        newSaleEventMapper.put("IdempotentKey", eventData.get("Item").toString()+eventData.get("Store").toString()+eventData.get("TimeStamp")+eventData.get("TransactionID").toString());
        newSaleEventMapper.put("EventTime", eventData.get("TimeStamp"));
        newSaleEventMapper.put("EventType", eventData.get("EventType").asText());
        newSaleEventMapper.put("TransactionID", eventData.get("TransactionID").asInt());
        return newSaleEventMapper;


    }
}
