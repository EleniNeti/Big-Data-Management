data = LOAD 'athlete_events.csv' USING PigStorage(',') AS (ID:chararray, Name:chararray, Sex:chararray, Age:int, Height:float, Weight:float, Team:chararray, NOC:chararray, Games:chararray, Year:int, Season:chararray, City:chararray, Sport:chararray, Event:chararray, Medal:chararray);

grp = GROUP data BY (Name, Sex, Age, Team, Sport, Games);

result = FOREACH grp 
                   {
                    golds = FILTER data BY Medal == '"Gold"';
		    silvers = FILTER data BY Medal == '"Silver"';
	            bronzes = FILTER data BY Medal == '"Bronze"';
                    total = FILTER data BY Medal == '"Gold"' OR Medal == '"Silver"' OR Medal == '"Bronze"';
                    GENERATE FLATTEN(group) AS (Name, Sex, Age, Team, Sport, Games), COUNT(golds) as Golds, COUNT(silvers) as Silvers, COUNT(bronzes) as Bronzes, COUNT(total) as Total;
                   };


result = ORDER result BY Golds DESC, Total DESC, Name DESC;

DUMP result;
