--REGISTER '/home/eleni/pig-0.17.0/lib/piggybank.jar';

DEFINE CsvExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

data = LOAD 'athlete_events.csv' USING CsvExcelStorage() AS     
(ID:chararray, Name:chararray, Sex:chararray, Age:int, Height:double, Weight:double, Team:chararray, NOC:chararray, 
Games:chararray, Year:int, Season:chararray, City:chararray, Sport:chararray, Event:chararray, Medal:chararray);


-- discard the header line
NoHeader = FILTER data BY ('ID' != $0);
/*  Takes all the records except of the first one. 
the 1st one has the 'ID' string in the first position (first field of tuple) indicated by $0*/


nonull = FOREACH NoHeader GENERATE ID, Name, Sex, (Age is null ? 0 : Age) AS Age, (Height is null ? 0.0 : Height) AS Height,
(Weight is null ? 0.0 : Weight) AS Weight, Team, NOC, Games, Year, Season, City, Sport, Event, (Medal is null ? 'Not Avail' : Medal) AS Medal;

--distinctdata = DISTINCT nonull; not necessary in this case


grp = GROUP nonull BY (ID, Name, Sex);

result = FOREACH grp 
                   {
                    golds = FILTER nonull BY Medal == 'Gold';
                    GENERATE FLATTEN(group) AS (ID, Name, Sex), COUNT(golds.Medal) AS Golds;
                   };

--DUMP result;

STORE result INTO 'question1' USING CsvExcelStorage();

/* The output file has 135.571 distinct records corresponding to the IDs/athletes - 
also confirmed in python*/
```
