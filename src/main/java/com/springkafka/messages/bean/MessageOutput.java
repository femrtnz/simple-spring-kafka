package com.springkafka.messages.bean;

public final class MessageOutput {

    private final int numberOfValues;
    private final long sumOfValues;

    public MessageOutput(int numberOfValues, long sumOfValues) {
        this.numberOfValues = numberOfValues;
        this.sumOfValues = sumOfValues;
    }

    public int getNumberOfValues() {
        return numberOfValues;
    }

    public long getSumOfValues() {
        return sumOfValues;
    }
}
