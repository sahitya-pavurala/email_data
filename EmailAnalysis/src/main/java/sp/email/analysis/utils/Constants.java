package sp.email.analysis.utils;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by sahityapavurala on 2/23/17.
 */
public class Constants {

    public static final String CREATE_SENDER = "";
    public static final String CREATE_RECIPIENT = "";
    public static final String DELIMITER = ",";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss Z (z)").withZoneUTC();
}
