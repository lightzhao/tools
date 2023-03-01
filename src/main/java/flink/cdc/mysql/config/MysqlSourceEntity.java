package flink.cdc.mysql.config;


import com.ververica.cdc.connectors.mysql.table.StartupMode;

import java.util.Properties;

public class MysqlSourceEntity {

    private String hostname;
    private Integer port;
    private String databaseList;
    private String tableList;
    private String username;
    private String password;
    private Properties debeziumProperties = new Properties();
    private StartupMode startMode = StartupMode.LATEST_OFFSET;
    private String startModeWithFile;
    private Long startModeWithOffset;
    private String startModeWithGtid;
    private Long startModeWithTimestamp;

    @Override
    public String toString() {
        return "MysqlSourceEntity{" +
                "hostname='" + hostname + '\'' +
                ", port=" + port +
                ", databaseList='" + databaseList + '\'' +
                ", tableList='" + tableList + '\'' +
                ", username='" + username + '\'' +
                ", debeziumProperties=" + debeziumProperties +
                ", startMode=" + startMode +
                ", startModeWithFile='" + startModeWithFile + '\'' +
                ", startModeWithOffset=" + startModeWithOffset +
                ", startModeWithGtid='" + startModeWithGtid + '\'' +
                ", startModeWithTimestamp=" + startModeWithTimestamp +
                '}';
    }

    public String getStartModeWithFile() {
        return startModeWithFile;
    }

    public void setStartModeWithFile(String startModeWithFile) {
        this.startModeWithFile = startModeWithFile;
    }

    public StartupMode getStartMode() {
        return startMode;
    }

    public void setStartMode(StartupMode startMode) {
        this.startMode = startMode;
    }

    public Long getStartModeWithOffset() {
        return startModeWithOffset;
    }

    public void setStartModeWithOffset(Long startModeWithOffset) {
        this.startModeWithOffset = startModeWithOffset;
    }

    public String getStartModeWithGtid() {
        return startModeWithGtid;
    }

    public void setStartModeWithGtid(String startModeWithGtid) {
        this.startModeWithGtid = startModeWithGtid;
    }

    public Long getStartModeWithTimestamp() {
        return startModeWithTimestamp;
    }

    public void setStartModeWithTimestamp(Long startModeWithTimestamp) {
        this.startModeWithTimestamp = startModeWithTimestamp;
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
