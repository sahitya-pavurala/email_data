package sp.email.analysis.utils;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Constants class for the application
 * Created by sahityapavurala on 2/23/17.
 */
public class Constants {

    public static final String CREATE_RECIPIENTS = "select sender,count(*) as cnt from email where label='broadcast' " +
            "group by sender order by cnt desc";

    public static final String CREATE_SENDERS = "select recipient,count(e.message_id) as cnt from recipient r inner join email e on e.message_id = r.message_id  " +
            "where e.label = 'direct' " +
            "group by recipient order by cnt desc";

    public static final String CREATE_RESPONSE_TIMES = "select t2.message_id,t2.sender,t2.subject,t1.sender,abs(t2.email_date-t1.email_date) as response_time " +
            "from email t1 left outer join email t2 on t1.hash = t2.hash and t1.message_id != t2.message_id and t1.sender != t2.sender " +
            "where t2.hash is not null order by response_time";

    public static final String DELIMITER = ",";
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss Z (z)").withZoneUTC();
}
