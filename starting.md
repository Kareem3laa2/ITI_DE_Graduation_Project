## Run This commands first
 - docker exec -it nyc-namenode bash
 - hdfs dfs -mkdir -p /data/bronze
 - hdfs dfs -put data/bronze /data/



## Give spark the permissions to write on HDFS
 - hdfs dfs -chmod 777 /data/silver
