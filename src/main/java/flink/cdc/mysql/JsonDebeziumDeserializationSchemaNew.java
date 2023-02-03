package flink.cdc.mysql;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Map;

/**
 * A JSON format implementation of {@link DebeziumDeserializationSchema} which deserializes the
 * received {@link SourceRecord} to JSON String.
 */
public class JsonDebeziumDeserializationSchemaNew implements DebeziumDeserializationSchema<HashMap> {


    private static final long serialVersionUID = 1L;

    private transient JsonConverter jsonConverter;

    /**
     * Configuration whether to enable {@link JsonConverterConfig#SCHEMAS_ENABLE_CONFIG} to include
     * schema in messages.
     */
    private final Boolean includeSchema;

    /**
     * The custom configurations for {@link JsonConverter}.
     */
    private Map<String, Object> customConverterConfigs;

    public JsonDebeziumDeserializationSchemaNew() {
        this(false);
    }

    public JsonDebeziumDeserializationSchemaNew(Boolean includeSchema) {
        this.includeSchema = includeSchema;
    }

    public JsonDebeziumDeserializationSchemaNew(
            Boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.includeSchema = includeSchema;
        this.customConverterConfigs = customConverterConfigs;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<HashMap> out) throws Exception {
        if (jsonConverter == null) {
            initializeJsonConverter();
        }
        byte[] key = jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
        byte[] value =
                jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        HashMap<String, byte[]> map = new HashMap<>();
        map.put("key", key);
        map.put("value", value);
        out.collect(map);
    }

    /**
     * Initialize {@link JsonConverter} with given configs.
     */
    private void initializeJsonConverter() {
        jsonConverter = new JsonConverter();
        final HashMap<String, Object> configs = new HashMap<>(2);
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
        if (customConverterConfigs != null) {
            configs.putAll(customConverterConfigs);
        }
        jsonConverter.configure(configs);
    }

    @Override
    public TypeInformation<HashMap> getProducedType() {
        return BasicTypeInfo.of(HashMap.class);
    }
}
