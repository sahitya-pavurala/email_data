package sp.email.analysis.extract;

import sp.email.analysis.utils.Constants;

/**
 * Created by sahityapavurala on 2/23/17.
 */
public class EmailRecord {

    private String message_id;
    private String sender;
    private String subject;
    private String email_date;
    private String label;
    private String hash;


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

    public String getEmail_date() {
        return email_date;
    }

    public void setEmail_date(String email_date) {
        this.email_date = email_date;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String sub_md5) {
        this.hash = sub_md5;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();

        sb.append(message_id).append(Constants.DELIMITER).append(sender).append(Constants.DELIMITER)
                .append(subject).append(Constants.DELIMITER).append(email_date).append(Constants.DELIMITER)
                .append(label).append(Constants.DELIMITER).append(hash);

        return sb.toString();
    }


}
