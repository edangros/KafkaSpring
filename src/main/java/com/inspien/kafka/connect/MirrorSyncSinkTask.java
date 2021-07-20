
package com.inspien.kafka.connect;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MirrorSyncSinkTask extends SinkTask {

    private String connectionId;
    private String responseTopic;
    private KafkaProducer<byte[],byte[]> producer; //Thread-safe Producer reference of the connector

    @Override
    public String version() {
        return new MirrorSyncSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.connectionId = props.get(ConnectorConfig.NAME_CONFIG);
        this.responseTopic = props.get(MirrorSyncSinkConnector.RESPONSE_TOPIC);
        this.producer = MirrorSyncSinkConnector.producerRegistry.get(this.connectionId);
        log.info("A mirror process for {} is started : watching topic {}",this.connectionId,this.responseTopic);
    }

    @Override
    public void stop() {
        log.info("A mirror process for {} is stopped");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        //do nothing. get record, transform it to sourcerecord, then send using producer
        for(SinkRecord record : records){
            log.trace("record {} from connection {} is mirrored to topic {}",record.toString(),this.connectionId,this.responseTopic);
            byte[] key = Utils.CONVERTER.fromConnectData(this.responseTopic, record.keySchema(), record.key());
            byte[] value = Utils.CONVERTER.fromConnectData(this.responseTopic, record.valueSchema(), record.value());
            ProducerRecord<byte[],byte[]> producerRecord = new ProducerRecord<>(this.responseTopic, null, key, value);
            record.headers().forEach(header -> {
                byte[] hValue = Utils.HEADER_CONVERTER.fromConnectHeader(this.responseTopic, header.key(), header.schema(), header.value());
                RecordHeader rHeader = new RecordHeader(header.key(), hValue);
                producerRecord.headers().add(rHeader);
                log.trace("value transfered : {} -> {}", header.value().toString(),new String(rHeader.value()));
            });
            log.info("mirrored message : {}", producerRecord.toString());
            producer.send(producerRecord);
        }
        
    }
}
