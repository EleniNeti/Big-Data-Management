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

--distinctdata = DISTINCT nonull; not necessary in this case

grp = GROUP data BY (Name, Sex, Age, Team, Sport, Games);

result = FOREACH grp 
                   {
                    golds = FILTER data BY Medal == 'Gold';
		    silvers = FILTER data BY Medal == 'Silver';
	            bronzes = FILTER data BY Medal == 'Bronze';
                    total = FILTER data BY Medal == 'Gold' OR Medal == 'Silver' OR Medal == 'Bronze';
                    GENERATE FLATTEN(group) AS (Name, Sex, Age, Team, Sport, Games), COUNT(golds) as Golds, COUNT(silvers) as Silvers, COUNT(bronzes) as Bronzes, COUNT(total) as Total;
                   };



result = ORDER result BY Golds DESC, Total DESC, Name ASC; /* ordering by name should be in ascending order,(A-Z) - Asceding is the default btw*/

first10 = LIMIT result 10; /* first 10 records */

ranked = RANK first10; /* ranking the first 10 records */

STORE ranked INTO 'question2' USING CsvExcelStorage();