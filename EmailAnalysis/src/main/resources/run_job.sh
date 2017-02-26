spark-submit \
--class sp.email.analysis.AnalysisDriver
--master yarn \
--deploy-mode cluster \
--jars spark-csv_2.11-1.5.0.jar
email-analysis-1.0-SNAPSHOT.jar
/user/cloudera/test_input
