data = LOAD 'athlete_events.csv' USING PigStorage(',') AS (ID:chararray, Name:chararray, Sex:chararray, Age:int, Height:float, Weight:float, Team:chararray, NOC:chararray, Games:chararray, Year:int, Season:chararray, City:chararray, Sport:chararray, Event:chararray, Medal:chararray);

grp = GROUP data BY (ID, Name, Sex);

result = FOREACH grp 
                   {
                    golds = FILTER data BY Medal == '"Gold"';
                    GENERATE FLATTEN(group) AS (ID, Name, Sex), COUNT(golds.Medal) as Golds;
                   };


limit5 = LIMIT result 100;

DUMP limit5;
