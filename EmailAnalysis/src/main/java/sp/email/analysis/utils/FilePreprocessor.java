package sp.email.analysis.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;
import sp.email.analysis.extract.EmailRecord;
import sp.email.analysis.extract.RecipientRecord;
import sp.email.analysis.transform.OutputTransformer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static sp.email.analysis.utils.EmailUtils.parseDate;

/** class to process email text files and extract records
 * Created by sahityapavurala on 2/22/17.
 */
public class FilePreprocessor {
    public static Logger LOGGER = Logger.getLogger(FilePreprocessor.class);

    /**
     * Method to prcoess the files
     * @param dirName
     * @param outputTrans
     * @throws IOException
     */
    public static void process(String dirName, OutputTransformer outputTrans) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new Configuration());

        List<Path> filePaths = readFilePaths(new Path(dirName));
        try{
            Path homeDir = fs.getHomeDirectory();
            LOGGER.info("The user home directory is ::"+homeDir.toString());
            Path emalPath = new Path(homeDir,"test/email/email.csv");
            LOGGER.info("Email file path :: "+ emalPath);
            if (fs.exists(emalPath))
                fs.delete(emalPath);

            Path recipientPath = new Path(homeDir,"test/recipient/recipient.csv");
            LOGGER.info("Recipient file path ::"+ recipientPath);
            if(fs.exists(recipientPath))
                fs.delete(recipientPath);

            FSDataOutputStream emailoutputStream = fs.create(emalPath,true);
            FSDataOutputStream recipientoutputStream = fs.create(recipientPath,true);

            for (Path file : filePaths) {
                EmailRecord emailRecord = new EmailRecord();
                List<RecipientRecord> recipientRecords = new ArrayList<RecipientRecord>();
                int recipient_count = 0;
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));
                String s = br.readLine();
                String previousLine = null;
                while (s != null) {
                    LOGGER.info("The line values is ::" + s);
                    String[] vals = s.split(": ");
                    String val =null;
                    if(vals.length > 1)
                        val = s.split(": ")[1];

                    if (s.startsWith("Message-ID")){
                        previousLine = "Message-ID";
                        emailRecord.setMessage_id(val);
                    }
                    else if (s.startsWith("From")) {
                        previousLine = "From";
                        emailRecord.setSender(val);
                    }
                    else if (s.startsWith("Date")) {
                        previousLine = "Date";
                        emailRecord.setEmail_date(parseDate(val));
                    }
                    else if (s.startsWith("Subject")) {
                        previousLine = "Subject";
                        val = s.replace("Subject:","").trim().replaceAll(","," ").replaceAll("\\s", " ");
                        emailRecord.setSubject(val);
                        emailRecord.setHash(EmailUtils.getHash(val));
                    }
                    else if(s.startsWith("To")){
                        previousLine = "To";
                        List<String> to_ids = getIds(val);
                        recipient_count += to_ids.size();
                        recipientRecords.addAll(loadRecipients(to_ids,emailRecord,"to"));
                    }
                    else if(s.startsWith("Cc")){
                        previousLine = "Cc";
                        List<String> cc_ids = getIds(val);
                        recipient_count += cc_ids.size();
                        recipientRecords.addAll(loadRecipients(cc_ids,emailRecord,"cc"));
                    }
                    else if(s.startsWith("Bcc")){
                        previousLine = "Bcc";
                        List<String> bcc_ids = getIds(val);
                        recipient_count +=  bcc_ids.size();
                        recipientRecords.addAll(loadRecipients(bcc_ids,emailRecord,"bcc"));
                    }
                    else if (s.startsWith("X-")) {
                        emailRecord.setLabel(getLabel(recipient_count));
                        LOGGER.info("The email record is ::" + emailRecord.toString());
                        emailoutputStream.writeBytes(emailRecord.toString());
                        for(RecipientRecord record: recipientRecords) {
                            recipientoutputStream.writeBytes(record.toString());
                            LOGGER.info("The recipient record is ::"+ record.toString());
                        }
                        break;
                    }
                    else if (s.startsWith("Mime-Version") || s.startsWith("Content-"))
                        previousLine = "";
                    else{
                        if(previousLine == "To:") {
                            List<String> to_ids = getIds(s.trim());
                            recipient_count += to_ids.size();
                            recipientRecords.addAll(loadRecipients(to_ids, emailRecord, "to"));
                        }
                        if(previousLine == "Cc") {
                            List<String> cc_ids = getIds(val);
                            recipient_count += cc_ids.size();
                            recipientRecords.addAll(loadRecipients(cc_ids, emailRecord, "cc"));
                        }if(previousLine == "Bcc") {
                            List<String> bcc_ids = getIds(val);
                            recipient_count +=  bcc_ids.size();
                            recipientRecords.addAll(loadRecipients(bcc_ids,emailRecord,"bcc"));
                        }
                        if(previousLine=="Subject"){
                            val = s.replaceAll(" +", " ").replaceAll(","," ");
                            emailRecord.setSubject(emailRecord.getSubject()+val);
                            emailRecord.setHash(EmailUtils.getHash(val));
                        }

                    }

                    s = br.readLine();
                }

            }
            emailoutputStream.flush();
            recipientoutputStream.flush();
            emailoutputStream.close();
            recipientoutputStream.close();
            LOGGER.info("Output Streams closed");

            outputTrans.setSources(emalPath,recipientPath);
            outputTrans.setSchema(EmailRecord.getSchema(),RecipientRecord.getSchema());

        } catch (Exception e) {
            LOGGER.info("Exception in FileProcess "+ e.toString());
            e.printStackTrace();
        }


    }

    /**
     * Mehod to read file paths from the directory
     * @param dirname
     * @return
     * @throws IOException
     */
    public static List<Path> readFilePaths(Path dirname) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        List<Path> filePaths = new ArrayList<Path>();

         class CustomFileFilter implements PathFilter {


            public boolean accept(Path path) {
                return path.toString().endsWith(".txt");
            }
        }

        FileStatus[] dirStatus = fs.listStatus(dirname);
        for (FileStatus st: dirStatus){
            LOGGER.info("Reading file paths in ::" + st.getPath());
            if(st.isDirectory()){
                FileStatus[] fstatuses = fs.listStatus(st.getPath(),new CustomFileFilter());
                for(FileStatus fstatus: fstatuses){
                    LOGGER.info("adding file path :: "+ fstatus.getPath());
                    filePaths.add(fstatus.getPath());}
            }

        }

        return filePaths;
    }

    /**
     * Method to get the email label
     * @param num
     * @return
     */
    public static String getLabel(int num){
        String label = "direct";

        if (num > 1)
            label = "broadcast";
        return label;

    }

    /**
     * Method to get ids from a email
     * @param val
     * @return
     */
    public static List<String> getIds(String val ){
        List<String> ids = new ArrayList<String>();
        if(val != null)
            for(String id: val.split(",")){
                if (id.contains("@"))
                    ids.add(id.trim());
            }

            return ids;
    }

    /**
     * Method to load recipient records
     * @param ids
     * @param emailRecord
     * @param check
     * @return
     */
    public static List<RecipientRecord> loadRecipients(List<String> ids,EmailRecord emailRecord, String check){
        List<RecipientRecord> records =  new ArrayList<RecipientRecord>();
        for (String id : ids){
            RecipientRecord recipientRecord = new RecipientRecord();
            recipientRecord.setMessage_id(emailRecord.getMessage_id());
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



