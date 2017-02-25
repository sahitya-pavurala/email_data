package sp.email.analysis;
import org.apache.log4j.Logger;
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

    public static final Logger LOGGER = Logger.getLogger(AnalysisDriver.class);

    public AnalysisDriver() {
    }

    public static void main(String[] args){



        SparkContext sc = null;


        try {
            SparkConf conf = new SparkConf().setAppName("Enron Email Analysis");
            sc = new SparkContext(conf);
            HiveContext hqlc = new HiveContext(sc);
            LOGGER.info("Loaded spark context and hive context");

            LOGGER.info("Starting file pre-process");
            FilePreprocessor.process(args[0]);
            LOGGER.info("Completed file pre-process, ready to transform");
        } catch (IOException e) {
            e.printStackTrace();
        }


        if(sc != null)
            sc.stop();

        }
}
