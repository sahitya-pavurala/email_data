package sp.email.analysis.transform;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

/** Interface for output transformers
 * Created by sahityapavurala on 2/28/17.
 */
public interface Transformer {

    void transform(HiveContext hqlc, SparkContext sc);
}
