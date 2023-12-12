package com.example.kafkastreamspring.util;

import com.example.kafkastreamspring.model.MessageUserBalance;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MessageUserBalanceTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        MessageUserBalance messageUserBalance = (MessageUserBalance) record.value();
        return messageUserBalance.getTimestamp().toEpochMilli();
    }
}
