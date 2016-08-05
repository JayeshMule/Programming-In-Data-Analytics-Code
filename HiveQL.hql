CREATE TABLE IF NOT EXISTS accidents(Longitude double,Latitude double,Police_Force int,Accident_Severity int,Number_of_Vehicles int,Number_of_Casualties int,Date string, Day int,Month int,Year int,Day_of_Week int,Time string,Local_Authority_District int,Road_Type int,Speed_limit int,Light_Conditions int,Weather_Conditions int,Road_Surface_Conditions int,Urban_or_Rural_Area int,Did_Police_Officer_Attend_Scene_of_Accident int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH 'hdfs://localhost:54310/user/hduser/Accidents/part-m-00000' INTO TABLE accidents;

insert overwrite local directory 'file:///home/hduser/Accidents'
row format delimited
fields terminated by ','
select Road_Surface_Conditions,Weather_Conditions,Accident_Severity,sum(Number_of_Casualties) from hiveaccidents group by Road_Surface_Conditions,Weather_Conditions,Accident_Severity;
 
insert overwrite local directory 'file:///home/hduser/TimeAccidents'
row format delimited
fields terminated by ','
select  Time,sum(Number_of_Casualties) as count from hiveaccidents group by Time SORT BY count desc limit 15;
