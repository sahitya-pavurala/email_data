package sp.email.analysis.utils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import sp.email.analysis.extract.EmailRecord;
import sp.email.analysis.extract.RecipientRecord;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static sp.email.analysis.utils.EmailUtils.parseDate;

/**
 * Created by sahityapavurala on 2/22/17.
 */
public class FilePreprocessor {

    public static void process(String dirName) throws IOException {

        File dir = new File(dirName);
        String[] extensions = new String[]{"txt"};
        List<File> files = (List<File>) FileUtils.listFiles(dir, extensions, true);
        files.remove(files.size() - 1);
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            Path homeDir = fs.getHomeDirectory();
            FSDataOutputStream emailoutputStream = fs.create(new Path(homeDir.toString() + "email"));
            FSDataOutputStream recipientoutputStream = fs.create(new Path(homeDir.toString() + "recipient"));

            for (File file : files) {


                Scanner sc = new Scanner(file);

                while (sc.hasNextLine()) {

                    EmailRecord emailRecord = new EmailRecord();
                    List<RecipientRecord> recipientRecords = new ArrayList<RecipientRecord>();
                    String s = sc.nextLine();
                    String val = s.split(": ")[1];
                    if (s.startsWith("Message-ID"))
                        emailRecord.setMessage_id(val);
                    else if (s.startsWith("From"))
                        emailRecord.setSender(val);
                    else if (s.startsWith("Date"))
                        emailRecord.setEmail_date(parseDate(val));
                    else if (s.startsWith("Subject")) {
                        emailRecord.setSubject(val);
                        if (val.startsWith("Re:"))
                            emailRecord.setLabel("broadcast");
                        else emailRecord.setLabel("direct");
                        emailRecord.setHash(EmailUtils.getHash(val));
                    }
                    else if(s.startsWith("To")){
                        String[] ids = val.split(",");
                        for (String id : ids){
                            RecipientRecord recipientRecord = new RecipientRecord();
                            recipientRecord.setMessage_id(emailRecord.getMessage_id());
                            recipientRecord.setSender(emailRecord.getSender());
                            recipientRecord.setRecipient(id.trim());
                            recipientRecord.setIs_to(1);
                            recipientRecords.add(recipientRecord);
                        }
                    }
                    else if(s.startsWith("Cc")){
                        String[] ids = val.split(",");
                        for (String id : ids){
                            RecipientRecord recipientRecord = new RecipientRecord();
                            recipientRecord.setMessage_id(emailRecord.getMessage_id());
                            recipientRecord.setSender(emailRecord.getSender());
                            recipientRecord.setRecipient(id.trim());
                            recipientRecord.setIs_cc(1);
                            recipientRecords.add(recipientRecord);
                        }
                    }
                    else if(s.startsWith("Bcc")){
                        String[] ids = val.split(",");
                        for (String id : ids){
                            RecipientRecord recipientRecord = new RecipientRecord();
                            recipientRecord.setMessage_id(emailRecord.getMessage_id());
                            recipientRecord.setSender(emailRecord.getSender());
                            recipientRecord.setRecipient(id.trim());
                            recipientRecord.setIs_bcc(1);
                            recipientRecords.add(recipientRecord);
                        }
                    }
                    else if (s.startsWith("X-")) {
                        emailoutputStream.write(emailRecord.toString().getBytes());
                        for(RecipientRecord record: recipientRecords)
                            recipientoutputStream.write(record.toString().getBytes());
                        break;
                    }
                }

            }

        } catch (Exception e) {

        }


    }
}



