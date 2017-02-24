package sp.email.analysis.utils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import sp.email.analysis.extract.EmailRecord;
import sp.email.analysis.extract.RecipientRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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

                EmailRecord emailRecord = new EmailRecord();
                RecipientRecord recipientRecord = new RecipientRecord();

                Scanner sc = new Scanner(file);

                while (sc.hasNextLine()) {
                    String s = sc.nextLine();
                    String val = s.split(": ")[1];
                    if (s.startsWith("Message-ID")) {
                        emailRecord.setMessage_id(val);
                        recipientRecord.setMessage_id(val);
                    } else if (s.startsWith("From")) {
                        emailRecord.setSender(val);
                        recipientRecord.setSender(val);
                    } else if (s.startsWith("Date"))
                        emailRecord.setEmail_date(val);
                    else if (s.startsWith("Subject")) {
                        emailRecord.setSubject(val);
                        if (val.startsWith("Re:"))
                            emailRecord.setLabel("broadcast");
                        else emailRecord.setLabel("direct");
                        emailRecord.setHash(String.valueOf(val.replace("Re", "").replaceAll("\\s+", "").hashCode()));
                    } else if (s.startsWith("X-"))
                        break;


                }

            }

        } catch (Exception e) {

        }


    }
}



