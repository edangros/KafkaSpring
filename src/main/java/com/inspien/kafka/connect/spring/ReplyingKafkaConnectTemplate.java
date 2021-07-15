package com.inspien.kafka.connect.spring;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.inspien.kafka.connect.RESTContextManager;
import com.inspien.kafka.connect.RESTInputSourceTask;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.util.ConnectUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.requestreply.CorrelationKey;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.Base64Utils;
import org.springframework.util.concurrent.SettableListenableFuture;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.clients.producer.Producer;

import lombok.extern.slf4j.Slf4j;

/**
 * The counterpart of Spring-Kafka's {@link KafkaTemplate} and {@link ReplyingKafkaTemplate} which uses {@link SourceTask}s instead of {@link Producer}s.
 * But runs out of Spring's boundary, to 
 */
@Slf4j
public class ReplyingKafkaConnectTemplate implements BatchMessageListener<byte[], byte[]> {
    
    private static final long DEFAULT_REPLY_TIMEOUT = 5000L;

    private final GenericMessageListenerContainer<byte[], byte[]> replyContainer;

    private final ConcurrentMap<CorrelationKey, SettableListenableFuture<SinkRecord>> futures = new ConcurrentHashMap<>();

    private final String connectionID;
    private TaskScheduler scheduler = new ThreadPoolTaskScheduler();


    private long replyTimeout;

    private volatile boolean schedulerSet;

    private volatile boolean running;

    private JsonConverter converter;

    private ApplicationContext applicationContext;

    public ReplyingKafkaConnectTemplate(GenericMessageListenerContainer<byte[], byte[]> container, String connectionId, long replyTimeout) {
        Assert.notNull(container, "'replyContainer' cannot be null");
        this.replyContainer = container;
        this.replyContainer.setupMessageListener(this);
        this.connectionID = connectionId;
        this.replyTimeout = replyTimeout;
        this.converter = new JsonConverter();
    }

    public ReplyingKafkaConnectTemplate(GenericMessageListenerContainer<byte[], byte[]> container, String connectionId) {
        this(container, connectionId, DEFAULT_REPLY_TIMEOUT);
    }

    public void setTaskScheduler(TaskScheduler scheduler) {
        Assert.notNull(scheduler, "'scheduler' cannot be null");
        this.scheduler = scheduler;
        this.schedulerSet = true;
    }

    public void setReplyTimeout(long replyTimeout) {
        Assert.isTrue(replyTimeout >= 0, "'replyTimeout' must be >= 0");
        this.replyTimeout = replyTimeout;
    }

    /**
     * Return the topics/partitions assigned to the replying listener container.
     * 
     * @return the topics/partitions.
     */
    public Collection<TopicPartition> getAssignedReplyTopicPartitions() {
        return this.replyContainer.getAssignedPartitions();
    }

    public void afterPropertiesSet() throws Exception {
        if (!this.schedulerSet) {
            ((ThreadPoolTaskScheduler) this.scheduler).initialize();
        }
    }

    public synchronized void start() {
        if (!this.running) {
            try {
                afterPropertiesSet();
            } catch (Exception e) {
                throw new KafkaException("Failed to initialize", e);
            }
            this.replyContainer.start();
            this.running = true;
        }
    }

    public synchronized void stop() {
        if (this.running) {
            this.running = false;
            this.replyContainer.stop();
            this.futures.clear();
        }
    }

    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    public SettableListenableFuture<SinkRecord> sendAndReceive(SourceRecord record) {
        return sendAndReceive(record, Duration.ofMillis(this.replyTimeout));
    }
    public SettableListenableFuture<SinkRecord> sendAndReceive(SourceRecord record, Duration replyTimeout) {
        Assert.state(this.running, "Template has not been started"); // NOSONAR (sync)
        CorrelationKey correlationId = createCorrelationId(record);
        Assert.notNull(correlationId, "the created 'correlationId' cannot be null");
        record.headers().add(KafkaHeaders.CORRELATION_ID,
                            correlationId.getCorrelationId(),
                            Schema.STRING_SCHEMA);
        if (log.isDebugEnabled()) {
            log.debug("Sending: " + record + " with correlationId: " + correlationId);
        }
        SettableListenableFuture<SinkRecord> future = new SettableListenableFuture<>();
        this.futures.put(correlationId, future);
        
        //access to lb, then get best task
        RESTInputSourceTask task = RESTContextManager.getInstance().taskLoadBalancer(connectionID).getAppropriate();

        try {
            task.put(record);
        } catch (Exception e) {
            this.futures.remove(correlationId);
            throw new KafkaException("Send failed", e);
        }
        this.scheduler.schedule(() -> {
            SettableListenableFuture<SinkRecord> removed = this.futures.remove(correlationId);
            if (removed != null) {
                if (log.isWarnEnabled()) {
                    log.warn("Reply timed out for: " + record + " with correlationId: " + correlationId);
                }
                removed.setException(new KafkaException("Reply timed out"));
            }
        }, Instant.now().plusMillis(replyTimeout.toMillis()));
        return future;
    }

    /**
     * Subclasses can override this to generate custom correlation ids. The default
     * implementation is a 16 byte representation of a UUID.
     * 
     * @param record the record.
     * @return the key.
     */
    protected CorrelationKey createCorrelationId(SourceRecord record) {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return new CorrelationKey(bytes);
    }

    @Override
    public void onMessage(List<ConsumerRecord<byte[], byte[]>> data) {
        for (ConsumerRecord<byte[],byte[]> record : data){
            Iterator<Header> iterator = record.headers().iterator();
            CorrelationKey correlationId = null;
            while (correlationId == null && iterator.hasNext()) {
                Header next = iterator.next();
                if (next.key().equals(KafkaHeaders.CORRELATION_ID)) {
                    correlationId = new CorrelationKey(next.value());
                }
            }
            
            if (correlationId == null) {
                log.error("No correlationId found in reply: " + record
                        + " - to use request/reply semantics, the responding server must return the correlation id "
                        + " in the '" + KafkaHeaders.CORRELATION_ID + "' header");
                return;
            }

            SettableListenableFuture<SinkRecord> future = this.futures.remove(correlationId);

            if (future == null) {
                log.error("No pending reply: " + record + " with correlationId: " + correlationId
                        + ", perhaps timed out");
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Received: " + record + " with correlationId: " + correlationId);
            }
            future.set(convertRecord(record));
            
        }
    }

    public SinkRecord convertRecord(ConsumerRecord<byte[],byte[]> record){
        //convert key, value, headers
        SchemaAndValue keyAndSchema;
        SchemaAndValue valueAndSchema;
        Headers headers;
        try{
            keyAndSchema = converter.toConnectData(record.topic(), record.headers(), record.key());
            valueAndSchema = converter.toConnectData(record.topic(), record.headers(), record.value());
            //read kafka header and convert to connect header
            headers = new ConnectHeaders();
            org.apache.kafka.common.header.Headers recordHeaders = record.headers();
            if (recordHeaders != null) {
                String topic = record.topic();
                for (org.apache.kafka.common.header.Header recordHeader : recordHeaders) {
                    SchemaAndValue schemaAndValue = converter.toConnectHeader(topic, recordHeader.key(), recordHeader.value());
                    headers.add(recordHeader.key(), schemaAndValue);
                }
            }
        }
        catch(Exception e){
            return null;
        }

        Long timestamp = ConnectUtils.checkAndConvertTimestamp(record.timestamp());
        SinkRecord connectRecord = new SinkRecord(record.topic(), record.partition(),
                keyAndSchema.schema(), keyAndSchema.value(),
                valueAndSchema.schema(), valueAndSchema.value(),
                record.offset(),
                timestamp,
                record.timestampType(),
                headers);
        log.trace("{} Applying transformations to record in topic '{}' partition {} at offset {} and timestamp {} with key {} and value {}",
                this, record.topic(), record.partition(), record.offset(), timestamp, keyAndSchema.value(), valueAndSchema.value());

        // Error reporting will need to correlate each sink record with the original consumer record
        return connectRecord;
    }

}
