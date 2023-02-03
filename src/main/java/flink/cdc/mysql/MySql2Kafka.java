package flink.cdc.mysql;

import com.google.gson.Gson;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import flink.cdc.mysql.config.Config;
import flink.cdc.mysql.config.KafkaSinkEntity;
import flink.cdc.mysql.config.MysqlSourceEntity;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;

public class MySql2Kafka {

    private final static Logger logger = LoggerFactory.getLogger(MySql2Kafka.class);

    public static void main(String[] args) throws Exception {
        Gson gson = new Gson();
        Config config = gson.fromJson(args[0], Config.class);
        logger.info("mysql2kafka config:{}", config.toString());
        MysqlSourceEntity mysqlSourceEntity = config.getMysqlSource();
        KafkaSinkEntity kafkaSinkEntity = config.getKafkaSink();

        if (mysqlSourceEntity.getHostname() == null || mysqlSourceEntity.getDatabaseList() == null || mysqlSourceEntity.getPassword() == null
                || mysqlSourceEntity.getUsername() == null || mysqlSourceEntity.getTableList() == null || kafkaSinkEntity.getServers() == null ||
                kafkaSinkEntity.getTopic() == null) {
            throw new IllegalArgumentException("Missing required parameter.");
        }


        MySqlSource<HashMap> mySqlSource = MySqlSource.<HashMap>builder()
                .hostname(mysqlSourceEntity.getHostname())
                .port(mysqlSourceEntity.getPort())
                .databaseList(mysqlSourceEntity.getDatabaseList()) // set captured database
                .tableList(mysqlSourceEntity.getTableList()) // set captured table
                .username(mysqlSourceEntity.getUsername())
                .password(mysqlSourceEntity.getPassword())
                .deserializer(new JsonDebeziumDeserializationSchemaNew()) // converts SourceRecord to JSON String
                .debeziumProperties(mysqlSourceEntity.getDebeziumProperties())
//                .startupOptions(startupOptions)
                .build();


        Properties sinkProps = new Properties();
        sinkProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkEntity.getServers());
        sinkProps.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaSinkEntity.getClientId());

        FlinkKafkaProducer myProducer = new FlinkKafkaProducer(kafkaSinkEntity.getTopic(),
                new KafkaRecordSerializationSchema(kafkaSinkEntity.getTopic()), sinkProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        DataStream dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource")
                // set 4 parallel source tasks
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering

        dataStreamSource.addSink(myProducer).name("KafkaSink");

        env.execute();
    }
}
