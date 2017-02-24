package sp.email.analysis.extract;

import sp.email.analysis.utils.Constants;

/**
 * Created by sahityapavurala on 2/23/17.
 */
public class RecipientRecord {

    private String message_id;
    private String sender;
    private String recipient;
    private int is_to;
    private int is_cc;
    private int is_bcc;

    public String getMessage_id() {
        return message_id;
    }

    public void setMessage_id(String message_id) {
        this.message_id = message_id;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
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

    public String toString(){
        StringBuilder sb = new StringBuilder();

        sb.append(message_id).append(Constants.DELIMITER).append(sender).append(Constants.DELIMITER)
                .append(recipient).append(Constants.DELIMITER).append(is_to).append(Constants.DELIMITER)
                .append(is_cc).append(Constants.DELIMITER).append(is_bcc);

        return sb.toString();
    }


}
