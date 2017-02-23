package sp.email.analysis.utils;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

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
        String[] extensions = new String[] { "txt"};
        List<File> files = (List<File>) FileUtils.listFiles(dir, extensions, true);
        files.remove(files.size()-1);

        String inputDirPath = "/Users/sahityapavurala/Desktop/slack interview/email_data/EmailAnalysis/resources/inputDir";
        new File(inputDirPath).mkdir();




        List<String> fileNames = new ArrayList<String>();
        for (File file: files) {
             String tempPath = new Path(new Path(file.getParentFile().getName())+"-"+file.getName()).toString();
             String newFilePath = new Path(inputDirPath ,tempPath).toString();
             File newFile = new File(newFilePath);

             Scanner sc = new Scanner(file);
             FileWriter writer = new FileWriter(newFile,true);
             PrintWriter printer = new PrintWriter(writer);
             while(sc.hasNextLine()) {
                 String s = sc.nextLine();
                 printer.write(s+"\n");
            }

            printer.flush();
            printer.close();
            writer.close();

        }

    }

}


