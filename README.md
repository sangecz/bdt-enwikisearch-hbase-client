# Simple HBASE based ENWIKI SEARCH in Metacentrum 

### How to run the ENWIKI SEARCH

STEP 1: Build the application with dependencies using Maven

```Shell
mvn clean compile assembly:single
```

STEP 2: Copy JAR file,jaas.conf and asynchbase.properties to HADOR

STEP 3: Edit jaas.conf (replace hawking with yout login name):

STEP 4: Get vocabulary file from hdfs: /bigdata/marekp11_task2.txt and copy to HADOR

STEP 5: Get document info file from hdfs: /bigdata/marekp11_vocab.txt and copy to HADOR

STEP 6: Run the client

```Shell
java -jar HBaseClient-jar-with-dependencies.jar -auth_conf PATH/TO/jaas.conf -async_conf PATH/TO/asynchbase.properties -table "marekp11:wiki_index" -vocab /PATH/TO/marekp11_task2.txt -docinfo /PATH/TO/marekp11_docinfo.txt
```

