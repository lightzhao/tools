package flink.cdc.mysql.config;


import java.util.Properties;

public class MysqlSourceEntity {

    private String hostname;
    private Integer port;
    private String databaseList;
    private String tableList;
    private String username;
    private String password;
    private Properties debeziumProperties;

    @Override
    public String toString() {
        return "MysqlSource{" +
                "hostname='" + hostname + '\'' +
                ", port=" + port +
                ", databaseList=" + databaseList +
                ", tableList=" + tableList +
                ", username='" + username + '\'' +
                ", debeziumProperties=" + debeziumProperties +
                '}';
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDatabaseList() {
        return databaseList;
    }

    public void setDatabaseList(String databaseList) {
        this.databaseList = databaseList;
    }

    public String getTableList() {
        return tableList;
    }

    public void setTableList(String tableList) {
        this.tableList = tableList;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Properties getDebeziumProperties() {
        return debeziumProperties;
    }

    public void setDebeziumProperties(Properties debeziumProperties) {
        this.debeziumProperties = debeziumProperties;
    }
}
