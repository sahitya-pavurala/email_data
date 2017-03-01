package sp.email.analysis.extract;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import sp.email.analysis.utils.Constants;

/**
 * Created by sahityapavurala on 2/23/17.
 * Bean class for recipient record
 */
public class RecipientRecord {

    private String message_id;
    private String recipient;
    private int is_to;  //  to be used
    private int is_cc;  //  while counting the receivers
    private int is_bcc; //  and senders

    public String getMessage_id() {
        return message_id;
    }

    public void setMessage_id(String message_id) {
        this.message_id = message_id;
    }

    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    public int getIs_to() {
        return is_to;
    }

    public void setIs_to(int is_to) {
        this.is_to = is_to;
    }

    public int getIs_cc() {
        return is_cc;
    }

    public void setIs_cc(int is_cc) {
        this.is_cc = is_cc;
    }

    public int getIs_bcc() {
        return is_bcc;
    }

    public void setIs_bcc(int is_bcc) {
        this.is_bcc = is_bcc;
    }

    /**
     * Override toString method to return the recipient record as
     * String seperated by a delimiter
     * @return
     */
    public String toString(){
        StringBuilder sb = new StringBuilder();

        sb.append(message_id).append(Constants.DELIMITER)
                .append(recipient).append(Constants.DELIMITER).append(is_to).append(Constants.DELIMITER)
                .append(is_cc).append(Constants.DELIMITER).append(is_bcc);

        return sb.append("\n").toString();
    }

    /**
     * Return the schema of the recipient record to construct
     * dataframe of recipient records from csv
     * @return
     */
    public static StructType getSchema() {
        StructType customSchema = new StructType(new StructField[]{
                new StructField("message_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("recipient", DataTypes.StringType, true, Metadata.empty()),
                new StructField("is_to", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("is_cc", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("is_bcc", DataTypes.IntegerType, true, Metadata.empty())
        });

        return customSchema;
    }


}
