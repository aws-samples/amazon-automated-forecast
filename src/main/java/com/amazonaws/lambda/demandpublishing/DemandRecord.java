package com.amazonaws.lambda.demandpublishing;

import com.opencsv.bean.AbstractBeanField;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvCustomBindByName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;

import static com.amazonaws.lambda.demandpublishing.DemandRecord.LocalDateTimeConverter.FORECAST_DATE_TIME_FORMATTER;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DemandRecord {

    public static class Attribute {
        public static final String ITEM_ID      = "item_id";
        public static final String TIMESTAMP    = "timestamp";
        public static final String TARGET_VALUE = "target_value";
    }

    public static class LocalDateTimeConverter extends AbstractBeanField {

        /*
         * Refer to: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns,
         * uuuu: year
         *      More details about using 'u' instead of 'y' are in:
         *      https://stackoverflow.com/questions/41177442/uuuu-versus-yyyy-in-datetimeformatter-formatting-pattern-codes-in-java
         * MM: month-of-year
         * dd: day-of-year; ('DD' is the day-of-year)
         * HH: hour-of-day (0-23) ('hh' is the clock-hour-of-am-pm (1-12))
         * mm: minute-of-hour
         * ss: second-of-minute
         */
        public static final DateTimeFormatter FORECAST_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

        @Override
        protected LocalDateTime convert(String s) {
            /*
             * Refer to: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns,
             * uuuu: year
             *      More details about using 'u' instead of 'y' are in:
             *      https://stackoverflow.com/questions/41177442/uuuu-versus-yyyy-in-datetimeformatter-formatting-pattern-codes-in-java
             * MM: month-of-year
             * dd: day-of-year; ('DD' is the day-of-year)
             * HH: hour-of-day (0-23) ('hh' is the clock-hour-of-am-pm (1-12))
             * mm: minute-of-hour
             * ss: second-of-minute
             */
            return LocalDateTime.parse(s, FORECAST_DATE_TIME_FORMATTER);
        }
    }

    @CsvBindByName(column = Attribute.ITEM_ID, required = true)
    private String itemId;

    @CsvCustomBindByName(column = Attribute.TIMESTAMP, converter = LocalDateTimeConverter.class)
    private LocalDateTime timestamp;

    @CsvBindByName(column = Attribute.TARGET_VALUE, required = true)
    private String targetValue;

    public String toCsvRowString() {
        StringJoiner sj = new StringJoiner(",");
        sj.add(itemId).add(FORECAST_DATE_TIME_FORMATTER.format(timestamp)).add(targetValue);
        return sj.toString();
    }
}



