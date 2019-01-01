::call ssh hadoop@self "~/program/hadoop-2.8.5/bin/hdfs dfs -rm /input/eco2.csv"
call ssh hadoop@self "~/program/hadoop-2.8.5/bin/hdfs dfs -put ~/input/eco2.csv /input"
::call ssh hadoop@self "~/program/hadoop-2.8.5/bin/hadoop jar ~/jar_hadoop/*.jar com.zx.mapreduce.ecommerce.Specialty"
::call ssh hadoop@self "~/program/hadoop-2.8.5/bin/hdfs dfs -cat /x/BestTime/part-r-00000"
