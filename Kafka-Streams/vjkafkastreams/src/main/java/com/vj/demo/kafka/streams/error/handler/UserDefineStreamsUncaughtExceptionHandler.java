package com.vj.demo.kafka.streams.error.handler;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j; 

@Component
@Slf4j
public class UserDefineStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
	
	@Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.error("Stream encountered fatal exception: {}", exception.getMessage(), exception);
        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}