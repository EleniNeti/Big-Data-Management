
/*   register the PiggyBank JAR 
not sure if this step is necessary for local mode */

REGISTER '/home/eleni/pig-0.17.0/lib/piggybank.jar'; /*  replace with '/path/to/piggybank.jar' 
paths for me are: /home/eleni/pig-0.17.0/lib/piggybank.jar or
                 /home/eleni/pig-0.17.0/contrib/piggybank/java/piggybank.jar  */

/*    Read CsvExcelStorage() in documentation @ https://pig.apache.org/docs/r0.17.0/api/org/apache/pig/piggybank/storage/CSVExcelStorage.html 
and CSVLoader() in documentation @ https://pig.apache.org/docs/latest/api/org/apache/pig/piggybank/storage/CSVLoader.html       */


DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
DEFINE CsvExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

--------------------------------------------------------load dataset-----------------------------------------------------------------------------------------

data = LOAD 'athlete_events.csv' USING CSVLoader() AS     
(ID:chararray, Name:chararray, Sex:chararray, Age:int, Height:double, Weight:double, Team:chararray, NOC:chararray, 
Games:chararray, Year:int, Season:chararray, City:chararray, Sport:chararray, Event:chararray, Medal:chararray);

/*     With CSVLoader you properly escape the comma inside double-quoted fields. E.g. when dumping the data desired output is:
(Christine Jacoba Aaftink,F,25,185,82,Netherlands,NED,1992 Winter,1992,Winter,Albertville,Speed Skating,"Speed Skating Women's 1,000 metres",NA)
rather than 
(Christine Jacoba Aaftink,F,25,185,82,Netherlands,NED,1992 Winter,1992,Winter,Albertville,Speed Skating,"Speed Skating Women's 1,000 metres")
when using PigStorage(','). 
000 metres" is the value assigned to the last field of the tuple, the 'Medal' one. */ 

-------------------------------------------------------cleaning--------------------------------------------------------------------------------------

--set null values to 0 for age, height and weight columns 
nonull = FOREACH data GENERATE ID, (Name matches '.* " .*' ? '' : Name) AS Name, Sex, (Age is null ? 0 : Age) AS Age, (Height is null ? 0.0 : Height) AS Height,
(Weight is null ? 0.0 : Weight) AS Weight, Team, NOC, Games, Year, Season, City, Sport, Event, (Medal is null ? 'Not Avail' : Medal) AS Medal;

/*  binary condition operator:starts with a boolean test followed by a question mark (?), then the value to return if the test is true, then a :, 
and finally the value to return if the test is false 
example:
 2 == 2 ? 1 : 4 --returns 1
 2 == 3 ? 1 : 4 --returns 4   */


-- delete duplicate rows
distinctdata = DISTINCT nonull;

-- run the script and display the result
dump distinctdata;

--store processed data into output folder 'processed_data' located in the same directory with pig, search for "part-r-00000" file.
STORE distinctdata INTO 'processed_data' USING CsvExcelStorage();




