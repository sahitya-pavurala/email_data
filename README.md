# email_data
Requirements:
Maven 3.3.9
Java 1.8
Spark 1.6
univocity-parsers-1.5.1.jar
commons-csv-1.1.jar
spark-csv_2.11-1.5.0.jar

1) Open the EmailAnalysis directory and compile the code using "mvn clean install"

2) copy the required jars from src/main/resources/ into the current directory

3) copy the run_job.sh into the current directory

4) copy the enron_with_categories into hdfs in user home direcory (Example /user/cloudera/enron_with_categories) - "/user/cloudera" is the home directory

5) run the script ./run_job.sh with the copied directory as parameter "./run_job.sh /user/cloudera/enron_with_categories"
