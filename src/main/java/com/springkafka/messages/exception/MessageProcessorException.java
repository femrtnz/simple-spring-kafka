package com.springkafka.messages.exception;

public class MessageProcessorException extends RuntimeException {

    public MessageProcessorException(String message, Exception e) {
        super(message, e);
    }
}
