# Map-Reduce-Softwares
- Count first name by origin : 
/usr/hdp/current/hadoop-client/bin/hadoop jar TP2.jar OriginCount /res/prenoms.csv output
- Count number of first name by number of origin (how many first name has x origins ? For x = 1,2,3...) : 
/usr/hdp/current/hadoop-client/bin/hadoop jar TP2.jar NameByOriginCount /res/prenoms.csv output
-  Proportion (in%) of male or female :
/usr/hdp/current/hadoop-client/bin/hadoop jar TP2.jar ProportionMaleFemale /res/prenoms.csv output
