records = LOAD 'hdfs://localhost:54310/user/hduser/Accidents/part-m-00000' USING PigStorage(',') AS (Longitude:double,Latitude:double,Police_Force:INT,Accident_Severity:INT,Number_of_Vehicles:INT,Number_of_Casualties:INT,Date:chararray,Day:INT, Month:INT,Year:INT,Dayofweek:INT,Time:chararray,Local_Authority_District:INT,Road_Type:INT,Speed_limit:INT,Light_Conditions:INT,Weather_Conditions:INT,Road_Surface_Conditions:INT,Urban_or_Rural_Area:INT,PoliceAttend:INT);

Dump Records;
year_record = GROUP records BY Year;	
year_accidents_record = FOREACH year_record GENERATE group, SUM(records.Number_of_Casualties);
Dump year_accidents_record;
STORE year_accidents_record INTO 'hdfs://localhost:54310/user/hduser/PigYearAccidentsOutput';

month_record = GROUP records BY Month;
month_accidents_record = FOREACH month_record GENERATE group, SUM(records.Number_of_Casualties);
Dump month_accidents_record;
STORE month_accidents_record INTO 'hdfs://localhost:54310/user/hduser/PigMonthAccidentsOutput';

day_record = GROUP records BY Day;
day_accidents_record = FOREACH day_record GENERATE group, SUM(records.Number_of_Casualties);
Dump day_accidents_record;
STORE day_accidents_record INTO 'hdfs://localhost:54310/user/hduser/PigDayAccidentsOutput';

weekday_record = GROUP records BY Dayofweek;
weekday_accidents_record = FOREACH weekday_record GENERATE group, SUM(records.Number_of_Casualties);
Dump weekday_accidents_record;
STORE weekday_accidents_record INTO 'hdfs://localhost:54310/user/hduser/PigWeekDayAccidentsOutput';
