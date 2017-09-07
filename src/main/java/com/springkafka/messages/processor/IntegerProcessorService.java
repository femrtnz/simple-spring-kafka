package com.springkafka.messages.processor;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafka.messages.bean.MessageOutput;
import com.springkafka.messages.exception.MessageProcessorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class IntegerProcessorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntegerProcessorService.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public String execute(String message) {
        validate(message);
        final List<String> integerList = Arrays.asList(message.split(","));
        final int numberOfValues = integerList.size();
        try {
            final int sumOfValues = integerList.stream().mapToInt(Integer::parseInt).sum();
            final MessageOutput messageOutput = new MessageOutput(numberOfValues, sumOfValues);
            return OBJECT_MAPPER.writeValueAsString(messageOutput);
        } catch (JsonProcessingException | NumberFormatException e) {
            LOGGER.error("Error trying to parse the message: " + message, e);
            throw new MessageProcessorException("Error trying to parse the message: " + message, e);
        }
    }

    private void validate(String message) {
        //quantity between 1 and 30
    }
}
