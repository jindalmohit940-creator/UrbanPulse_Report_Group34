# Flink Kafka connector JARs

`incident_detection_job.py` loads every `.jar` in this directory via
`env.add_jars(...)`. PyFlink's DataStream Kafka connector needs the actual
Java connector JAR on the classpath — `pip install apache-flink` does not
ship it.

Download (match the Flink version `apache-flink` installed, e.g. 2.3.0 ->
Flink 1.20.x line):

```powershell
# Windows PowerShell, run from Task_C/jars/
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar" -OutFile "flink-sql-connector-kafka-3.2.0-1.19.jar"
```

Check your installed `apache-flink` version first (`pip show apache-flink`)
and pick the matching connector line from
https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-kafka
— connector/runtime version mismatches are the #1 cause of
`ClassNotFoundException` / `NoSuchMethodError` at job submission.

