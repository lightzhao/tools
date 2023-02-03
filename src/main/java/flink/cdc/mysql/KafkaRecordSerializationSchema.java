package flink.cdc.mysql;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.HashMap;

public class KafkaRecordSerializationSchema implements KafkaSerializationSchema<HashMap<String, byte[]>> {

    private String topic;

    public KafkaRecordSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(HashMap<String, byte[]> map, @Nullable Long timestamp) {
        byte[] key = map.get("key") == null ? null : map.get("key");
        byte[] value = map.get("value") == null ? null : map.get("value");
        return new ProducerRecord(topic, null, null, key, value);

    }
}
