package flink.cdc.mysql.config;

import java.util.Properties;

public class KafkaSinkEntity {

    private String servers;
    private String clientId;
    private String topic;
    private Properties params;

    @Override
    public String toString() {
        return "KafkaSink{" +
                "servers='" + servers + '\'' +
                ", clientId='" + clientId + '\'' +
                ", topic='" + topic + '\'' +
                ", params=" + params +
                '}';
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Properties getParams() {
        return params;
    }

    public void setParams(Properties params) {
        this.params = params;
    }
}
