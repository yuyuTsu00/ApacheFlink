package com.learn.flink.model;

/**
 * A simple event flowing through every lab.
 *
 * This is a Flink POJO (public class, public no-arg constructor, public fields),
 * which lets Flink use its efficient built-in serializer for state and shuffles.
 */
public class Transaction {

    public String txnId;
    public String userId;
    public String category;
    public double amount;

    /** Event time in epoch milliseconds = when the transaction actually happened. */
    public long eventTime;

    public Transaction() {
    }

    @Override
    public String toString() {
        return "Transaction{txnId='" + txnId + '\'' +
                ", userId='" + userId + '\'' +
                ", category='" + category + '\'' +
                ", amount=" + amount +
                ", eventTime=" + eventTime + '}';
    }
}
