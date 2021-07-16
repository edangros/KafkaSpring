
package com.inspien.kafka.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.inspien.kafka.connect.error.TaskBufferFullException;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.tomcat.util.collections.SynchronizedQueue;

import lombok.extern.slf4j.Slf4j;

/**
 * Generate Web Server and then do all the producing stuff using its own message buffer
 * The buffer uses Tomcat's {@link SynchronizedQueue}, which has auto expanding feature.
 */

@Slf4j
public class RESTInputSourceTask extends SourceTask implements ILoadBalancable {

    private enum CountingOption{
        BY_MSG_COUNT, BY_MSG_SIZE
    }

    private SynchronizedQueue<SourceRecord> buffer;

    private int bufferCnt;
    private int bufferSize;
    private String connectionId;
    private String name;
    private TaskLoadBalancer lb;
    private CountingOption countOption;


    /**
     * Initializze Task, with initial buffer size of 4.
     */
    public RESTInputSourceTask() {
        //buffer size must be small - Kafka connect acts as stream so consumption speed must be faster than source generation.
        //so if buffer is full, that's not the matter of buffer size, but that of the number of operating tasks.
        this(4);
    }

    /* visible for testing */
    RESTInputSourceTask(int initialBufferSize) { 
        //initialize load spec
        this.bufferCnt = 0;
        this.bufferSize = 0;
        //use tomcat's SynchronizedQueue, because each task will use its own buffer so every buffer must be as light as possible.
        buffer = new SynchronizedQueue<>(initialBufferSize);
    }

    @Override
    public String version() {
        return new RESTSyncConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        connectionId = props.get(RESTSyncConnector.CONNECTION_ID);
        this.name = connectionId + "_Task" + props.get(RESTSyncConnector.TASK_INDEX);
        //set count option
        switch(props.get(RESTSyncConnector.LOADBALANCER_SCORING)){
            case "BY_CNT":
                this.countOption = CountingOption.BY_MSG_COUNT;
                break;
            case "BY_SIZE":
                this.countOption = CountingOption.BY_MSG_SIZE;
                break;
            default:
                break;
        }
        //register to lb
        lb = RESTContextManager.getInstance().taskLoadBalancer(connectionId);
        lb.register(this);
        log.info("task {} is successfully created and registered in loadbalancer",this.name);
    }

    /**
     * polling buffer and send result.
     * this will be called WorkerSourceTask Thread, locking its process.
     * To prevent multithread issue, Polling will consume only 1 message in buffer queue. WorkerTask will do another loop as soon as the message is sent.
     */
    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();
        //For LB access, send single message per single buffer polling.
        SourceRecord record = this.buffer.poll();
        if (record != null){
            log.trace("task {} is polling que", this.name);
            records.add(record);
            this.bufferCnt -= 1;
            this.bufferSize -= record.toString().getBytes().length;
        }
        return records;
    }

    @Override
    public void stop() {
        log.trace("{} is Stopping",this.name);
        synchronized (this) {
            // deregister this from lb
            this.lb.deregister(this);
        }
    }

    public void put(SourceRecord record){
        if(this.bufferSize>=1024*1024*1024){
            log.error("{} stacks messages over 1GB. Cannot handle any more message.", this.name);
            throw new TaskBufferFullException(String.format(
                "%s stacks messages over 1GB(current usage : %d). Cannot handle any more message.",
                this.name, this.bufferSize));
        }
        this.bufferCnt += 1;
        this.bufferSize += record.toString().getBytes().length;
    }

    @Override
    public synchronized int loadScore() {
        switch(this.countOption){
            case BY_MSG_COUNT:
                return this.bufferCnt;
            case BY_MSG_SIZE:
                return this.bufferSize;
            default:
                log.error("Wrong Configuration for connection {}. Task will gain no job", this.name);
                return Integer.MAX_VALUE;
        }
    }
}
