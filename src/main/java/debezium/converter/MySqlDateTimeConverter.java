package debezium.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * MySqlDateTimeConverter
 */
public class MySqlDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private final static Logger logger = LoggerFactory.getLogger(MySqlDateTimeConverter.class);

    private ZoneOffset zoneOffset = ZoneOffset.of("+8");

    DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
    DateTimeFormatter formatter_ = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'+08:00'");

    @Override
    public void configure(Properties props) {
        readProps(props, "zone", z -> zoneOffset = ZoneOffset.of(z));
    }

    private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.length() == 0) {
            return;
        }
        try {
            callback.accept(settingValue.trim());
        } catch (IllegalArgumentException | DateTimeException e) {
            logger.error("The {} setting is illegal: {}", settingKey, settingValue);
            throw e;
        }
    }


    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;
        if ("DATETIME".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.int64().optional();
            converter = this::convertDateTime;
        }
        if ("TIMESTAMP".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional();
            converter = this::convertTimeStamp;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
        }
    }

    private Long convertDateTime(Object input) {
        if (input == null)
            return null;
        /**
         * binlog 增量数据
         */
        if (input instanceof LocalDateTime) {
            LocalDateTime zoneTime = (LocalDateTime) input;
            int year = zoneTime.getYear();
            int month = zoneTime.getMonth().getValue();
            int day = zoneTime.getDayOfMonth();
            int hours = zoneTime.getHour();
            int minutes = zoneTime.getMinute();
            int seconds = zoneTime.getSecond();
            int nanoOfSecond = zoneTime.getNano();
            LocalDateTime localDateTime = LocalDateTime.of(year, month, day, hours, minutes, seconds, nanoOfSecond);
            return localDateTime.toEpochSecond(zoneOffset) * 1000;
            /**
             * 全量数据
             */
        } else if (input instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) input;
            return timestamp.getTime();
        } else {
            return null;
        }
    }

    public String convertTimeStamp(Object input) {
        if (input == null)
            return null;
        /**
         * binlog 增量数据
         */
        if (input instanceof ZonedDateTime) {
            ZonedDateTime zoneddatetime = (ZonedDateTime) input;
            String value = zoneddatetime.format(formatter);
            Date dateObj = Date.from(Instant.parse(value));
            zoneddatetime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateObj.getTime()), zoneOffset);
            return zoneddatetime.format(formatter_);
            /**
             * 全量数据
             */
        } else if (input instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) input;
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getTime()), zoneOffset).toString();
        } else {
            return null;
        }
    }
}