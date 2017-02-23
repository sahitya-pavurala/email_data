package sp.email.analysis;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import sp.email.analysis.utils.FilePreprocessor;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sahityapavurala on 2/22/17.
 */
public class AnalysisDriver {

    public AnalysisDriver() {
    }

    public static void main(String[] args){

        List<String> inputFiles = null;
        try {
            FilePreprocessor.process("/Users/sahityapavurala/Desktop/slack interview/enron_with_categories");
        } catch (IOException e) {
            e.printStackTrace();
        }




        //JavaSparkContext sc;
        //SparkConf conf = new SparkConf();
        //sc = new JavaSparkContext(conf);

        //JavaRDD filesRDD = sc.textFile(",".join(inputFiles));



    }
}
