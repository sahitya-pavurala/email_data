package sp.email.analysis.utils;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Email Utility class
 * Created by sahityapavurala on 2/24/17.
 */
public class EmailUtils {

    /**
     * Method the parse the date in the emails
     * @param date
     * @return
     */
    public static long parseDate(String date) {

        long dt = Constants.DATE_TIME_FORMATTER.parseMillis(date);
        return dt;
    }

    /**
     * Method to get the hash value of the email subject
     * @param value
     * @return
     */
    public static String getHash(String value) {
        if(value == null)
            return null;
        String newVal = value.toLowerCase().replace("re:", "").replaceAll("\\s", "").trim();
        String retVal = null;
        if (newVal.length() > 0) {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                retVal = md.digest(newVal.getBytes()).toString();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            catch (Exception e){
                e.printStackTrace();
            }

        }

        return retVal;
    }

}
