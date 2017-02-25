package sp.email.analysis.utils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;
import sp.email.analysis.extract.EmailRecord;
import sp.email.analysis.extract.RecipientRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static sp.email.analysis.utils.EmailUtils.parseDate;

/**
 * Created by sahityapavurala on 2/22/17.
 */
public class FilePreprocessor {
    public static Logger LOGGER = Logger.getLogger(FilePreprocessor.class);


    public static void process(String dirName) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new Configuration());

        List<Path> filePaths = readFilePaths(new Path(dirName));
        try{
            Path homeDir = fs.getHomeDirectory();
            Path emalPath = new Path(homeDir,"/test/email/email.txt");
            if (fs.exists(emalPath))
                fs.delete(emalPath);

            Path recipientPath = new Path(homeDir,"/test/recipient/recipient.txt");
            if(fs.exists(recipientPath))
                fs.delete(recipientPath);

            FSDataOutputStream emailoutputStream = fs.create(emalPath,true);
            FSDataOutputStream recipientoutputStream = fs.create(recipientPath,true);

            for (Path file : filePaths) {
                EmailRecord emailRecord = new EmailRecord();
                List<RecipientRecord> recipientRecords = new ArrayList<RecipientRecord>();

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));
                String s = br.readLine();

                while (s != null) {
                    String[] vals = s.split(": ");
                    String val =null;
                    if(vals.length > 0)
                        val = s.split(": ")[1];

                    if (s.startsWith("Message-ID"))
                        emailRecord.setMessage_id(val);
                    else if (s.startsWith("From"))
                        emailRecord.setSender(val);
                    else if (s.startsWith("Date"))
                        emailRecord.setEmail_date(parseDate(val));
                    else if (s.startsWith("Subject")) {
                        emailRecord.setSubject(val);
                        emailRecord.setHash(EmailUtils.getHash(val));
                    }
                    else if(s.startsWith("To")){
                        List<String> ids = getIds(val);
                        emailRecord.setLabel(getLabel(ids.size()));
                        recipientRecords.addAll(loadRecipients(ids,emailRecord,"to"));
                    }
                    else if(s.startsWith("Cc")){
                        List<String> ids = getIds(val);
                        emailRecord.setLabel(getLabel(ids.size()));
                        recipientRecords.addAll(loadRecipients(ids,emailRecord,"cc"));
                    }
                    else if(s.startsWith("Bcc")){
                        List<String> ids = getIds(val);
                        emailRecord.setLabel(getLabel(ids.size()));
                        recipientRecords.addAll(loadRecipients(ids,emailRecord,"bcc"));
                    }
                    else if (s.startsWith("X-")) {
                        emailoutputStream.writeBytes(emailRecord.toString());
                        for(RecipientRecord record: recipientRecords)
                            recipientoutputStream.writeBytes(record.toString());
                        break;
                    }

                    s = br.readLine();
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    public static List<Path> readFilePaths(Path dirname) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        List<Path> filePaths = new ArrayList<Path>();

         class CustomFileFilter implements PathFilter {


            public boolean accept(Path path) {
                return !path.toString().endsWith(".txt");
            }
        }

        FileStatus[] dirStatus = fs.listStatus(dirname);
        for (FileStatus st: dirStatus){
            if(st.isDirectory()){
                FileStatus[] fstatuses = fs.listStatus(st.getPath(),new CustomFileFilter());
                for(FileStatus fstatus: fstatuses){
                    filePaths.add(fstatus.getPath());}
            }

        }

        return filePaths;
    }

    public static String getLabel(int num){
        String label = "direct";

        if (num > 1)
            label = "broadcast";
        return label;

    }

    public static List<String> getIds(String val ){
        List<String> ids = new ArrayList<String>();
        if(val != null)
            for(String id: val.split(",")){
                if (id.contains("@"))
                    ids.add(id.trim());
            }

            return ids;
    }

    public static List<RecipientRecord> loadRecipients(List<String> ids,EmailRecord emailRecord, String check){
        List<RecipientRecord> records =  new ArrayList<RecipientRecord>();
        for (String id : ids){
            RecipientRecord recipientRecord = new RecipientRecord();
            recipientRecord.setMessage_id(emailRecord.getMessage_id());
            recipientRecord.setSender(emailRecord.getSender());
            recipientRecord.setRecipient(id.trim());
            if(check.equals("to"))
                recipientRecord.setIs_to(1);
            else if(check.equals("cc"))
                recipientRecord.setIs_cc(1);
            else if(check.equals("bcc"))
                recipientRecord.setIs_bcc(1);

            records.add(recipientRecord);
        }

        return records;
    }
}



