package flink.cdc.mysql;

import com.google.gson.Gson;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import flink.cdc.mysql.config.Config;
import flink.cdc.mysql.config.KafkaSinkEntity;
import flink.cdc.mysql.config.MysqlSourceEntity;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class MySql2Kafka {

    private final static Logger logger = LoggerFactory.getLogger(MySql2Kafka.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("please add config file path.");
        }
        String filepPath = args[0];
        Gson gson = new Gson();
        Config config = gson.fromJson(new BufferedReader(new FileReader(filepPath)), Config.class);

        MysqlSourceEntity mysqlSourceEntity = config.getMysqlSource();
        KafkaSinkEntity kafkaSinkEntity = config.getKafkaSink();

        if (mysqlSourceEntity.getHostname() == null || mysqlSourceEntity.getDatabaseList() == null || mysqlSourceEntity.getPassword() == null
                || mysqlSourceEntity.getUsername() == null || mysqlSourceEntity.getTableList() == null || kafkaSinkEntity.getServers() == null ||
                kafkaSinkEntity.getTopic() == null) {
            throw new IllegalArgumentException("Missing required parameter.");
        }

        Properties debziumProperties = mysqlSourceEntity.getDebeziumProperties();
        debziumProperties.setProperty("converters", "dateConverters");
        debziumProperties.setProperty("dateConverters.type", "com.data.dpd.MySqlDateTimeConverter");
        debziumProperties.setProperty("decimal.handling.mode", "string");
        List<String> tableList = DBTableFindUtil.listTables(mysqlSourceEntity);
        List<String> dbList = DBTableFindUtil.listDBs(mysqlSourceEntity);

        logger.info("mysql2kafka config:{}", config.toString());

        MySqlSource<HashMap> mySqlSource = MySqlSource.<HashMap>builder()
                .hostname(mysqlSourceEntity.getHostname())
                .port(mysqlSourceEntity.getPort())
                .databaseList(dbList.toArray(new String[dbList.size()]))
                .tableList(tableList.toArray(new String[tableList.size()]))
                .username(mysqlSourceEntity.getUsername())
                .password(mysqlSourceEntity.getPassword())
                .deserializer(new JsonDebeziumDeserializationSchemaNew()) // converts SourceRecord to JSON String
                .debeziumProperties(debziumProperties)
                .startupOptions(getStartupOptions(mysqlSourceEntity))
                .build();


        Properties sinkProps = new Properties();
        sinkProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSinkEntity.getServers());
        sinkProps.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaSinkEntity.getClientId());
        if (kafkaSinkEntity.getParams() != null) {
            sinkProps.putAll(kafkaSinkEntity.getParams());
        }


        FlinkKafkaProducer myProducer = new FlinkKafkaProducer(kafkaSinkEntity.getTopic(),
                new KafkaRecordSerializationSchema(kafkaSinkEntity.getTopic()), sinkProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        if (config.getCommon().containsKey("execution.checkpoint.interval")) {
            env.enableCheckpointing(Long.parseLong(config.getCommon().get("execution.checkpoint.interval")));
        } else {
            env.enableCheckpointing(180000);
        }

        env.disableOperatorChaining();

        DataStream dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource")
                // set 4 parallel source tasks
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering

        dataStreamSource.addSink(myProducer).name("KafkaSink");

        env.execute();
    }

    public static StartupOptions getStartupOptions(MysqlSourceEntity mysqlSourceEntity) {
        StartupOptions startupOptions = StartupOptions.latest();

        switch (mysqlSourceEntity.getStartMode()) {
            case INITIAL:
                startupOptions = StartupOptions.initial();
                break;
            case EARLIEST_OFFSET:
                startupOptions = StartupOptions.earliest();
                break;
            case LATEST_OFFSET:
                startupOptions = StartupOptions.latest();
                break;
            case SPECIFIC_OFFSETS:
                if (StringUtils.isNotBlank(mysqlSourceEntity.getStartModeWithFile()) && mysqlSourceEntity.getStartModeWithOffset() != null) {
                    BinlogOffset binlogOffset = BinlogOffset.ofBinlogFilePosition(mysqlSourceEntity.getStartModeWithFile(), mysqlSourceEntity.getStartModeWithOffset());
                    startupOptions = StartupOptions.specificOffset(binlogOffset);
                }
                if (StringUtils.isNotBlank(mysqlSourceEntity.getStartModeWithGtid())) {
                    BinlogOffset binlogOffset = BinlogOffset.ofGtidSet(mysqlSourceEntity.getStartModeWithGtid());
                    startupOptions = StartupOptions.specificOffset(binlogOffset);
                }
                break;
            case TIMESTAMP:
                startupOptions = StartupOptions.timestamp(mysqlSourceEntity.getStartModeWithTimestamp());
                break;
            default:
                throw new IllegalArgumentException("mysql cdc startup mode error.");
        }
        return startupOptions;
    }
}
