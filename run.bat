call mvn clean package
call scp ./target/*.jar hadoop@self:~/jar_hadoop/
call ssh hadoop@self "~/program/hadoop-2.8.5/bin/hadoop jar ~/jar_hadoop/*.jar com.zx.mapreduce.economic.ModifyData"
call ssh hadoop@self "~/program/hadoop-2.8.5/bin/hadoop jar ~/jar_hadoop/*.jar com.zx.mapreduce.lucknoob.LuckNoob"
