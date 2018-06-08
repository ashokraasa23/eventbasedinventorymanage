package com.ignite.core.streams.eventmapper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ignite.core.streams.DAO.CassandraDAO;
import com.ignite.core.streams.DAO.FinalTopicDAO;

import java.util.List;


/**
 * Created by ashok on 6/5/2018.
 */
public class CassandraMapper {

    CassandraDAO cassandraDAO = new CassandraDAO();
    FinalTopicDAO finalTopicDAO = new FinalTopicDAO();

    public JsonNode newCassandraMapper(JsonNode eventData) {

        String serverIp = "127.0.0.1";
        String keyspace = "eventbasedinventory";
        Cluster cluster = Cluster.builder().addContactPoint(serverIp).withPort(9042).build();
        Session session = cluster.connect(keyspace);
        ResultSet resultSet = null;
        // create a new balance json object

        finalTopicDAO.setEventType(eventData.get("EventType").asText());
        finalTopicDAO.setItem(eventData.get("ItemNumber").asInt());
        finalTopicDAO.setStore(eventData.get("Store").asInt());
        finalTopicDAO.setQuantity(eventData.get("Quantity").asDouble());
        finalTopicDAO.setInventoryState(eventData.get("InventoryState").asText());
        finalTopicDAO.setIdempotentKey(eventData.get("IdempotentKey").toString());
        finalTopicDAO.setEventTime(eventData.get("EventTime").asText());
        finalTopicDAO.setEventType(eventData.get("EventType").asText());
        finalTopicDAO.setTransactionID(eventData.get("TransactionID").asInt());


        String selectData = "select item,store,quantity from inventory where item = " + finalTopicDAO.getItem() + " and store = " + finalTopicDAO.getStore();
        //System.out.println(selectData);
        //String selectData = "select item,store,quantity from inventory";
        //resultSet = session.execute(selectData);
        //Iterator<Row> iter = resultSet.iterator();
        resultSet = session.execute(selectData);



        List<Row> rows = resultSet.all();


        if(!rows.isEmpty()) {
            for (Row row : rows) {
                cassandraDAO.setCItem(row.getInt("item"));
               // System.out.println("ItemNumber " + cassandraDAO.getCItem());
                cassandraDAO.setCStore(row.getInt("store"));
               // System.out.println("Store " + cassandraDAO.getCStore());
                cassandraDAO.setCQuantity(row.getDouble("quantity"));
                //System.out.println("Quantity " + cassandraDAO.getCQuantity());
            }

        }

        if(finalTopicDAO.getItem()== cassandraDAO.getCItem() && finalTopicDAO.getStore()==cassandraDAO.getCStore()){
            finalTopicDAO.setFinalQuantity(finalTopicDAO.getQuantity() + cassandraDAO.getCQuantity());
        }else{
            finalTopicDAO.setFinalQuantity(finalTopicDAO.getQuantity());
        }




        //System.out.println("ItemNumber " + CItem);

        //String data="insert into inventory(item, store, currenttime, eventtime, eventtype, idempotentkey, inventorystate, isprocessedind, quantity, transactionid) values ('"+Item+"','"+Store+"',toTimestamp(now()),'"+EventTime+"','"+EventType+"','"+IdempotentKey+"','"+InventoryState+"','Y','"+Quantity+"','"+TransactionID+"')";
        String data="insert into inventory(item, store, currenttime, eventtime, eventtype, idempotentkey, inventorystate, isprocessedind, quantity, transactionid) values ("+finalTopicDAO.getItem()+","+finalTopicDAO.getStore()+",toTimestamp(now()),'"+finalTopicDAO.getEventTime()+"','"+finalTopicDAO.getEventType()+"','"+finalTopicDAO.getIdempotentKey()+"','"+finalTopicDAO.getInventoryState()+"','Y',"+finalTopicDAO.getFinalQuantity()+","+finalTopicDAO.getTransactionID()+")";
        session.execute(data);

        //System.out.println("Upsert Successful......");

        ObjectNode newSaleEventMapper = JsonNodeFactory.instance.objectNode();
        newSaleEventMapper.put("ItemNumber", eventData.get("ItemNumber").asInt());
        newSaleEventMapper.put("ItemNumber", eventData.get("ItemNumber").asInt());
        newSaleEventMapper.put("Store", eventData.get("Store").asInt());
        newSaleEventMapper.put("Quantity", (eventData.get("Quantity").asDouble()));
        newSaleEventMapper.put("InventoryState", eventData.get("InventoryState").asText());
        newSaleEventMapper.put("IdempotentKey", eventData.get("IdempotentKey").asText());
        newSaleEventMapper.put("EventTime", eventData.get("EventTime").asText());
        newSaleEventMapper.put("EventType", eventData.get("EventType").asText());
        newSaleEventMapper.put("TransactionID", eventData.get("TransactionID").asInt());
        return newSaleEventMapper;

    }
}
