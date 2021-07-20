
package com.inspien.kafka.connect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.inspien.kafka.connect.error.TaskBufferFullException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.tomcat.util.collections.SynchronizedQueue;

import javassist.bytecode.analysis.Util;
import lombok.extern.slf4j.Slf4j;

/**
 * Generate Web Server and then do all the producing stuff using its own message buffer
 * The buffer uses Tomcat's {@link SynchronizedQueue}, which has auto expanding feature.
 */

@Slf4j
public class MirrorSyncSinkTask extends SinkTask {

    private String connectionId;
    private String responseTopic;
    private KafkaProducer<byte[],byte[]> producer; //Thread-safe Producer reference of the connector

    @Override
    public String version() {
        return new RESTSyncConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.connectionId = props.get(ConnectorConfig.NAME_CONFIG);
        this.responseTopic = props.get(MirrorSyncSinkConnector.RESPONSE_TOPIC);
        this.producer = MirrorSyncSinkConnector.producerRegistry.get(this.connectionId);
        log.info("A mirror process for {} is started",this.connectionId);
    }

    @Override
    public void stop() {
        log.info("A mirror process for {} is stopped");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        //do nothing. get record, transform it to sourcerecord, then send using producer
        for(SinkRecord record : records){
            log.info("record {} is mirrored to topic {}",record.toString(),this.responseTopic);
            byte[] key = Utils.CONVERTER.fromConnectData(this.responseTopic, record.keySchema(), record.key());
            byte[] value = Utils.CONVERTER.fromConnectData(this.responseTopic, record.valueSchema(), record.value());
            ProducerRecord<byte[],byte[]> producerRecord = new ProducerRecord<>(this.responseTopic, null, key, value);
            record.headers().forEach(header -> {
                RecordHeader rHeader = new RecordHeader(header.key(), Utils.CONVERTER.fromConnectHeader(
                    this.responseTopic, 
                    header.key(),
                    header.schema(),
                    header.value()
                )
                );
                producerRecord.headers().add(rHeader);
            });
            producer.send(producerRecord);
        }
    }
}
