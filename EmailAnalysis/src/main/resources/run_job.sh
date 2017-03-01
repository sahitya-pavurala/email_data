spark-submit \
--class sp.email.analysis.AnalysisDriver \
--master yarn \
--deploy-mode cluster \
--jars spark-csv_2.11-1.5.0.jar,commons-csv-1.1.jar,univocity-parsers-1.5.1.jar \
target/email-analysis-1.0-SNAPSHOT.jar \
{1}