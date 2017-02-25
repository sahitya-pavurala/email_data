package sp.email.analysis;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import sp.email.analysis.utils.FilePreprocessor;


import java.io.IOException;
import java.util.List;

/**
 * Created by sahityapavurala on 2/22/17.
 */
public class AnalysisDriver {

    public AnalysisDriver() {
    }

    public static void main(String[] args){


        try {
            FilePreprocessor.process(args[0]);
        } catch (IOException e) {
            e.printStackTrace();
        }


        SparkConf conf = new SparkConf();
        SparkContext sc = new SparkContext(conf);
        HiveContext hqlc = new HiveContext(sc);




        }
}
