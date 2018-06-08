package com.ignite.core.streams.DAO;

/**
 * Created by ashok on 6/6/2018.
 */
public class FinalTopicDAO {

    String eventType;
    int Item ;
    int Store ;
    Double Quantity ;
    String InventoryState ;
    String IdempotentKey;
    String EventTime ;
    String EventType ;
    int TransactionID;
    Double finalQuantity;

    public Double getFinalQuantity() {
        return finalQuantity;
    }

    public void setFinalQuantity(Double finalQuantity) {
        this.finalQuantity = finalQuantity;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public int getTransactionID() {
        return TransactionID;
    }

    public void setTransactionID(int transactionID) {
        TransactionID = transactionID;
    }

    public int getItem() {
        return Item;
    }

    public void setItem(int item) {
        Item = item;
    }

    public int getStore() {
        return Store;
    }

    public void setStore(int store) {
        Store = store;
    }

    public Double getQuantity() {
        return Quantity;
    }

    public void setQuantity(Double quantity) {
        Quantity = quantity;
    }

    public String getInventoryState() {
        return InventoryState;
    }

    public void setInventoryState(String inventoryState) {
        InventoryState = inventoryState;
    }

    public String getIdempotentKey() {
        return IdempotentKey;
    }

    public void setIdempotentKey(String idempotentKey) {
        IdempotentKey = idempotentKey;
    }

    public String getEventTime() {
        return EventTime;
    }

    public void setEventTime(String eventTime) {
        EventTime = eventTime;
    }


    @Override
    public String toString() {
        return "FinalTopicDAO{" +
                "eventType='" + eventType + '\'' +
                ", Item=" + Item +
                ", Store=" + Store +
                ", Quantity=" + Quantity +
                ", InventoryState='" + InventoryState + '\'' +
                ", IdempotentKey='" + IdempotentKey + '\'' +
                ", EventTime='" + EventTime + '\'' +
                ", EventType='" + EventType + '\'' +
                ", TransactionID=" + TransactionID +
                ", finalQuantity=" + finalQuantity +
                '}';
    }
}
