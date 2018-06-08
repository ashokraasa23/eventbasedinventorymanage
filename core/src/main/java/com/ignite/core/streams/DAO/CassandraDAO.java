package com.ignite.core.streams.DAO;

/**
 * Created by ashok on 6/6/2018.
 */
public class CassandraDAO {

    private int CItem;
    private int CStore;
    private Double CQuantity;

    public int getCItem() {
        return CItem;
    }

    public void setCItem(int CItem) {
        this.CItem = CItem;
    }

    public int getCStore() {
        return CStore;
    }

    public void setCStore(int CStore) {
        this.CStore = CStore;
    }

    public Double getCQuantity() {
        return CQuantity;
    }

    public void setCQuantity(Double CQuantity) {
        this.CQuantity = CQuantity;
    }

    @Override
    public String toString() {
        return "CassandraDAO{" +
                "CItem=" + CItem +
                ", CStore=" + CStore +
                ", CQuantity=" + CQuantity +
                '}';
    }
}
