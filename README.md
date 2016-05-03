# Simple HBASE client in Metacentrum 

### How to run the client

STEP 1: Build application with dependencies using Maven

```Shell
mvn clean compile assembly:single
```

STEP 2: Copy resulting .jar file together with jaas.conf and asynchbase.properties

STEP 3: Edit jaas.conf (replace hawking with yout login name):

STEP 4: Run client

```Shell
java -jar HBaseClient-jar-with-dependencies.jar -auth_conf PATH/TO/jaas.conf -async_conf PATH/TO/asynchbase.properties -table "LOGIN:TABLENAME" -row_key ROW_KEY
```


