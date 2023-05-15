DEFINE CsvExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

data = LOAD 'athlete_events.csv' USING CsvExcelStorage() AS     
(ID:chararray, Name:chararray, Sex:chararray, Age:int, Height:double, Weight:double, Team:chararray, NOC:chararray, 
Games:chararray, Year:int, Season:chararray, City:chararray, Sport:chararray, Event:chararray, Medal:chararray);

NoHeader = FILTER data BY ('ID' != $0);

nonull = FOREACH NoHeader GENERATE ID, (Name matches '.* " .*' ? '' : Name) AS Name, Sex, (Age is null ? 0 : Age) AS Age, (Height is null ? 0.0 : Height) AS Height,
(Weight is null ? 0.0 : Weight) AS Weight, (REGEX_EXTRACT(Team, '^(.+?)(?:-.*)?$', 1)) AS Team, NOC, Games, Year, Season, City, Sport, Event, (Medal is null ? 'Not Avail' : Medal) AS Medal;

/*  REGEX_EXCTRACT see more @https://pig.apache.org/docs/latest/func.html#regex-extract 

* This is a preprocessing step for the 'Team' field, it cuts off the hyphen "-" and everything after it.

* E.g. if Team = 'Greece-1' it will return 'Greece'. 

It is impossible to capture other Team-names with regular expression and substring methods. For instance,          
                          Team       NOC
3                    Denmark/Sweden  DEN
20615               Pistoja/Firenze  ITA
24683         Great Britain/Germany  GBR  
50771          United States/France  USA
52450          United States/France  FRA
54975                 Barion/Bari-2  ITA                                                                 */




females = FILTER nonull BY Sex == 'F'; 
-----------------------------------------------------------------------------------------------------------------------------------------------
/* Create first relation to count distinct female participations. */

grp = GROUP females BY (Games, Team);

female_count = FOREACH grp { uniq_id = DISTINCT females.ID; GENERATE FLATTEN(group) AS (Games, Team), COUNT(uniq_id) AS FemaleParticipations;};

ranked_teams = ORDER female_count BY FemaleParticipations DESC;

-----------------------------------------------------------------------------------------------------------------------------------------------
/* Create second relation to count the most preferable sport for each Team at a given Game (organization) */
grp2 = GROUP females BY (Games, Team, Sport);

sports_count = FOREACH grp2 GENERATE FLATTEN(group) AS (Games, Team, Sport), COUNT(females.Sport) AS SportCount;

grp3 = GROUP sports_count BY (Games, Team);

max_sport = FOREACH grp3 {
    sorted_sports = ORDER sports_count BY SportCount DESC;
    max_sport = LIMIT sorted_sports 1;
   GENERATE FLATTEN(group) AS (Games, Team), BagToString(max_sport.Sport, ',') AS Sport;
};
------------------------------------------------------------------------------------------------------------------------------------------------
/*Join the two relations on common keys. */
jnd = join ranked_teams by (Games, Team), max_sport by (Games, Team);

/* From the joined relation keep only fields of interest. */
result = FOREACH jnd GENERATE ranked_teams::Games AS Games, ranked_teams::Team AS Team, ranked_teams::FemaleParticipations AS FemaleParticipations, max_sport::Sport AS Sport;


/*  TODO: add NOC as field, 

*   do the correct ranking of the result
*   and give the top3    */








