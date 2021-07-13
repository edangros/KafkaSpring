package com.inspien.kafka.connect;

public class NoTaskException extends RuntimeException{
    public NoTaskException(String message){
        super(message);
    }
    public NoTaskException(String message, Throwable cause){
        super(message,cause);
    }
}
