package com.inspien.kafka.connect.error;

public class ConnectionKeyException extends RuntimeException{
    public ConnectionKeyException(String message){
        super(message);
    }
    public ConnectionKeyException(String message, Throwable cause){
        super(message,cause);
    }
}
