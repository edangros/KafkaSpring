package com.inspien.kafka.connect;

public class TaskBufferFullException extends RuntimeException{
    public TaskBufferFullException(String message){
        super(message);
    }
    public TaskBufferFullException(String message, Throwable cause){
        super(message,cause);
    }
}
