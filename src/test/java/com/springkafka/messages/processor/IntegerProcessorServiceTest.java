package com.springkafka.messages.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springkafka.messages.bean.MessageOutput;
import com.springkafka.messages.config.MessageConfig;
import com.springkafka.messages.exception.MessageProcessorException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = MessageConfig.class)
public class IntegerProcessorServiceTest {

    private final static ObjectMapper MAPPER = new ObjectMapper();

    @Autowired
    private IntegerProcessorService processorService;


    @Test
    public void given_stringWithCommaSeparatedInteger_when_execute_should_returnValidJson() throws Exception {
        final String message = "1,2,3";
        final int numberOfValues = 3;
        final int sumOfValues = 6;
        final String jsonMessage = processorService.execute(message);
        try {
            final MessageOutput output = MAPPER.readValue(jsonMessage, MessageOutput.class);
            assertEquals(numberOfValues, output.getNumberOfValues());
            assertEquals(sumOfValues, output.getSumOfValues());
        } catch (Exception e){
            fail("The Message Is not valid Json: " + jsonMessage);
        }
    }

    @Test(expected = MessageProcessorException.class)
    public void given_stringWithCommaSeparatedInteger_when_execute_should_throwException() throws Exception {
        final String invalidString = "1,,3";
        processorService.execute(invalidString);
    }
}