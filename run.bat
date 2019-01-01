call mvn clean package
call scp ./target/*.jar hadoop@self:~/jar_hadoop/
call ssh hadoop@self "~/program/hadoop-2.8.5/bin/hadoop jar ~/jar_hadoop/hadoop_demo1-1.0-SNAPSHOT.jar com.zx.ecommerce.GrossDomesticTransaction"
