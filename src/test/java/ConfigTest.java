import com.google.gson.Gson;
import flink.cdc.mysql.config.Config;
import flink.cdc.mysql.config.KafkaSinkEntity;
import flink.cdc.mysql.config.MysqlSourceEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigTest {

    public static void main(String[] args) {
        Config config = new Config();
        Map<String, String> common = new HashMap<>();

        MysqlSourceEntity mysqlSourceEntity = new MysqlSourceEntity();
        mysqlSourceEntity.setHostname("hostname_add");
        mysqlSourceEntity.setDatabaseList("dbname");
        mysqlSourceEntity.setPassword("123");
        mysqlSourceEntity.setPort(3306);
        mysqlSourceEntity.setTableList("dbname.t1");
        mysqlSourceEntity.setUsername("mysql");
        mysqlSourceEntity.setDebeziumProperties(getDebeziumProperties());

        KafkaSinkEntity kafkaSinkEntity = new KafkaSinkEntity();
        kafkaSinkEntity.setServers("server-address");
        kafkaSinkEntity.setTopic("topicName");
        kafkaSinkEntity.setClientId("client.id");

        config.setCommon(common);
        config.setMysqlSource(mysqlSourceEntity);
        config.setKafkaSink(kafkaSinkEntity);
        Gson gson = new Gson();

        System.out.println(gson.toJson(config));
    }

    private static Properties getDebeziumProperties() {
        Properties properties = new Properties();
        properties.setProperty("converters", "dateConverters");
        properties.setProperty("dateConverters.type", "debezium.converter.MySqlDateTimeConverter");
        properties.setProperty("decimal.handling.mode", "string");
        properties.put("message.key.columns", "");
        return properties;
    }
}
