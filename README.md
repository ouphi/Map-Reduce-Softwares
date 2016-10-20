# Map-Reduce-Softwares

## Launch jobs
- Count first name by origin :

/usr/hdp/current/hadoop-client/bin/hadoop jar TP2.jar OriginCount /res/prenoms.csv output

- Count number of first name by number of origin (how many first name has x origins ? For x = 1,2,3...) :

/usr/hdp/current/hadoop-client/bin/hadoop jar TP2.jar NameByOriginCount /res/prenoms.csv output

-  Proportion (in%) of male or female :

/usr/hdp/current/hadoop-client/bin/hadoop jar TP2.jar ProportionMaleFemale /res/prenoms.csv output

## Hive
Hive Queries are in the file hive_query.hql

## Sources : 
https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

http://stackoverflow.com/questions/5450290/accessing-a-mappers-counter-from-a-reducer

http://fr.hortonworks.com/hadoop-tutorial/introducing-apache-hadoop-developers/
