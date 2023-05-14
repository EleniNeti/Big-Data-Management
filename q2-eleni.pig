DEFINE CsvExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

data = LOAD 'athlete_events.csv' USING CsvExcelStorage() AS     
(ID:chararray, Name:chararray, Sex:chararray, Age:int, Height:double, Weight:double, Team:chararray, NOC:chararray, 
Games:chararray, Year:int, Season:chararray, City:chararray, Sport:chararray, Event:chararray, Medal:chararray);


-- discard the header line
NoHeader = FILTER data BY ('ID' != $0);
/*  Takes all the records except of the first one. 
the 1st one has the 'ID' string in the first position (first field of tuple) indicated by $0*/

-- cleaning
nonull = FOREACH NoHeader GENERATE ID, Name, Sex, (Age is null ? 0 : Age) AS Age, (Height is null ? 0.0 : Height) AS Height,
(Weight is null ? 0.0 : Weight) AS Weight, Team, NOC, Games, Year, Season, City, Sport, Event, (Medal is null ? 'Not Avail' : Medal) AS Medal;


grp = GROUP data BY (Name, Sex, Age, Team, Sport, Games);

result = FOREACH grp 
                   {
                    golds = FILTER data BY Medal == 'Gold';
		    silvers = FILTER data BY Medal == 'Silver';
	            bronzes = FILTER data BY Medal == 'Bronze';
                    total = FILTER data BY Medal == 'Gold' OR Medal == 'Silver' OR Medal == 'Bronze';
                    GENERATE FLATTEN(group) AS (Name, Sex, Age, Team, Sport, Games), COUNT(golds) as Golds, COUNT(silvers) as Silvers, COUNT(bronzes) as Bronzes, COUNT(total) as Total;
                   };



result = ORDER result BY Golds DESC, Total DESC, Name DESC; /* ordering by Name was set back to DESC mode in order for the RANK...BY to run correctly*/

first10 = LIMIT result 10; /* first 10 records */

ranked = rank first10 by Golds DESC, Total DESC DENSE; /* ranking the first 10 records 

 DENSE added to indicate the exact same performances. 
 
 E.g.:(4,Kristin Otto,F,22,East Germany,Swimming,1988 Summer,6,0,0,6)   
 (4,Vitaly Venediktovich Shcherbo,M,20,Unified Team,Gymnastics,1992 Summer,6,0,0,6) should both be ranked as 4th best performance*/

STORE ranked INTO 'question2' USING CsvExcelStorage();