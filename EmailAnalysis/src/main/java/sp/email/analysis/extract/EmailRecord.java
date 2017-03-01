package sp.email.analysis.extract;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sp.email.analysis.utils.Constants;

/**
 * Created by sahityapavurala on 2/23/17.
 * Bean class for EmailRecord
 */
public class EmailRecord {

    private String message_id;
    private String sender;
    private String subject;
    private long email_date;
    private String label;
    private String hash;

    //getters and setters

    public String getMessage_id() {
        return message_id;
    }

    public void setMessage_id(String messageID) {
        this.message_id = messageID;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public long getEmail_date() {
        return email_date;
    }

    public void setEmail_date(long email_date) {
        this.email_date = email_date;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String sub_md5) {
        this.hash = sub_md5;
    }

    /**
     * Override toString method to return the email record as
     * String seperated by a delimiter
     * @return
     */
    public String toString(){
        StringBuilder sb = new StringBuilder();

        sb.append(message_id).append(Constants.DELIMITER).append(sender).append(Constants.DELIMITER)
                .append(subject).append(Constants.DELIMITER).append(email_date).append(Constants.DELIMITER)
                .append(label).append(Constants.DELIMITER).append(hash);

        return sb.append("\n").toString();
    }

    /**
     * Return the schema of the email record to construct
     * dataframe of email records from csv
     * @return
     */
    public static StructType getSchema() {
        StructType customSchema = new StructType(new StructField[]{
                new StructField("message_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("sender", DataTypes.StringType, true, Metadata.empty()),
                new StructField("subject", DataTypes.StringType, true, Metadata.empty()),
                new StructField("email_date", DataTypes.LongType, true, Metadata.empty()),
                new StructField("label", DataTypes.StringType, true, Metadata.empty()),
                new StructField("hash", DataTypes.StringType, true, Metadata.empty())
        });

        return customSchema;
    }



}
