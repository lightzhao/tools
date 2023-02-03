package flink.cdc.mysql.config;

import java.util.Map;

public class Config {

    Map<String, String> common;

    MysqlSourceEntity mysqlSource;

    KafkaSinkEntity kafkaSink;

    public Map<String, String> getCommon() {
        return common;
    }

    public void setCommon(Map<String, String> common) {
        this.common = common;
    }

    public MysqlSourceEntity getMysqlSource() {
        return mysqlSource;
    }

    public void setMysqlSource(MysqlSourceEntity mysqlSourceEntity) {
        this.mysqlSource = mysqlSourceEntity;
    }

    public KafkaSinkEntity getKafkaSink() {
        return kafkaSink;
    }

    public void setKafkaSink(KafkaSinkEntity kafkaSinkEntity) {
        this.kafkaSink = kafkaSinkEntity;
    }

    @Override
    public String toString() {
        return "Config{" +
                "common=" + common +
                ", mysqlSource=" + mysqlSource +
                ", kafkaSink=" + kafkaSink +
                '}';
    }
}
