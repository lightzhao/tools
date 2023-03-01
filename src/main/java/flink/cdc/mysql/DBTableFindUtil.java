package flink.cdc.mysql;

import flink.cdc.mysql.config.MysqlSourceEntity;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.ververica.cdc.connectors.mysql.source.utils.StatementUtils.quote;

public class DBTableFindUtil {

    private final static Logger logger = LoggerFactory.getLogger(DBTableFindUtil.class);

    static final String MYSQL_CONNECTION_URL = "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useSSL=${useSSL}&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";

    public static JdbcConnection getJdbcConnection(MysqlSourceEntity mysqlSourceEntity) throws Exception {

        Properties properties = new Properties();
        properties.put("hostname", mysqlSourceEntity.getHostname());
        properties.put("port", mysqlSourceEntity.getPort() + "");
        properties.put("user", mysqlSourceEntity.getUsername());
        properties.put("password", mysqlSourceEntity.getPassword());

        Configuration.Builder jdbcConfigBuilder = Configuration.from(properties)
                .edit()
                .with("connectTimeout", 30 * 1000)
                .with("useSSL", true);
        JdbcConnection jdbc = new JdbcConnection(jdbcConfigBuilder.build(),
                JdbcConnection.patternBasedFactory(MYSQL_CONNECTION_URL));
        return jdbc;
    }

    public static List<String> listDBs(MysqlSourceEntity mysqlSourceEntity)
            throws Exception {
        final List<String> databaseNames = new ArrayList<>();
        JdbcConnection mysql = getJdbcConnection(mysqlSourceEntity);
        try {
            AtomicReference<String> sql = new AtomicReference<>();
            sql.set("SHOW DATABASES");
            mysql.query(sql.get(), rs -> {
                while (rs.next()) {
                    if (rs.getString(1).matches(mysqlSourceEntity.getDatabaseList())) {
                        databaseNames.add(rs.getString(1));
                        logger.info("include database:{}", rs.getString(1));
                    }

                }
            });
        } finally {
            mysql.close();
        }
        return databaseNames;
    }

    public static List<String> listTables(MysqlSourceEntity mysqlSourceEntity)
            throws Exception {

        JdbcConnection mysql = getJdbcConnection(mysqlSourceEntity);
        List<String> tables = new ArrayList<>();
        try {
            AtomicReference<String> sql = new AtomicReference<>();

            final List<String> databaseNames = new ArrayList<>();
            sql.set("SHOW DATABASES");
            mysql.query(sql.get(), rs -> {
                while (rs.next()) {
                    databaseNames.add(rs.getString(1));
                }
            });
            for (String dbName : databaseNames) {
                try {
                    sql.set("SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'");
                    mysql.query(sql.get(), rs -> {
                        while (rs.next()) {
                            String tableName = dbName + "." + rs.getString(1);
                            if (tableName.matches(mysqlSourceEntity.getTableList())) {
                                logger.info("include table:{}", tableName);
                                tables.add(tableName);
                            }

                        }
                    });
                } catch (SQLException e) {
                    // We were unable to execute the query or process the results, so skip this ...
                    logger.warn("\t skipping database '{}' due to error reading tables: {}", dbName, e.getMessage());
                }
            }
        } finally {
            mysql.close();
        }
        return tables;
    }

}
