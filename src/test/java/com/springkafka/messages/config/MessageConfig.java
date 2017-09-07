package com.springkafka.messages.config;

import com.springkafka.messages.processor.IntegerProcessorService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MessageConfig {

    @Bean
    public IntegerProcessorService integerProcessorService(){
        return new IntegerProcessorService();
    }
}
