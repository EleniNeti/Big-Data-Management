DEFINE CsvExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

/*Load the dataset */

data = LOAD 'athlete_events.csv' USING CsvExcelStorage() AS     
(ID:chararray, Name:chararray, Sex:chararray, Age:int, Height:double, Weight:double, Team:chararray, NOC:chararray, 
Games:chararray, Year:int, Season:chararray, City:chararray, Sport:chararray, Event:chararray, Medal:chararray);

/*Load noc and country name dataset*/

nocs = LOAD 'noc.csv' USING CsvExcelStorage() AS     
(NOC:chararray, region:chararray, notes:chararray);

NoHeaderData = FILTER data BY ('ID' != $0);

/*Preprocess initial data*/

nonull = FOREACH NoHeaderData GENERATE ID, (Name matches '.* " .*' ? '' : Name) AS Name, Sex, (Age is null ? 0 : Age) AS Age, (Height is null ? 0.0 : Height) AS Height,
(Weight is null ? 0.0 : Weight) AS Weight, (REGEX_EXTRACT(Team, '^(.+?)(?:-.*)?$', 1)) AS Team, NOC, Games, Year, Season, City, Sport, Event, (Medal is null ? 'Not Avail' : Medal) AS Medal;

NoHeaderNocs = FILTER nocs BY ('ID' != $0);

/*Join preprocessed data with noc-country data*/

join_data_nocs = join NoHeaderData by NOC, NoHeaderNocs by NOC;

/*Generate the clean version of our data (include NOC only once and replace Team with corresponding country name)*/

clean_data = FOREACH join_data_nocs{
    GENERATE $0 AS ID, $1 AS Name, $2 AS Sex, $3 AS Age, $4 AS Height, $5 AS Weight, $16 AS Team, $7 AS NOC, $8 AS Games, $9 AS Year, $10 AS Season, $11 AS City, $12 AS Sport, $13 AS Event, $14 AS Medal;
}

/* Create first relation to count distinct female participations. */

females = FILTER clean_data BY Sex == 'F'; 

grp = GROUP females BY (Games, Team, NOC);

female_count = FOREACH grp { uniq_id = DISTINCT females.ID; GENERATE FLATTEN(group) AS (Games, Team, NOC), COUNT(uniq_id) AS FemaleParticipations;};

ranked_teams = ORDER female_count BY FemaleParticipations DESC;

/* Create second relation to count the most preferable sport for each Team at a given Game (organization) */

grp2 = GROUP females BY (Games, Team, Sport);

sports_count = FOREACH grp2 GENERATE FLATTEN(group) AS (Games, Team, Sport), COUNT(females.Sport) AS SportCount;

grp3 = GROUP sports_count BY (Games, Team);

max_sport = FOREACH grp3 {
    sorted_sports = ORDER sports_count BY SportCount DESC;
    max_sport = LIMIT sorted_sports 1;
    GENERATE FLATTEN(group) AS (Games, Team), BagToString(max_sport.Sport, ',') AS Sport;
};

/*Join the two relations on common keys. */
jnd = join ranked_teams by (Games, Team), max_sport by (Games, Team);

/* From the joined relation keep only fields of interest. */
result = FOREACH jnd GENERATE ranked_teams::Games AS Games, ranked_teams::Team AS Team, ranked_teams::NOC AS NOC, ranked_teams::FemaleParticipations AS FemaleParticipations, max_sport::Sport AS Sport;

/*Find the max value of female participations per Olympic Games*/
grp4 = GROUP result BY Games;

max_participations1 = FOREACH grp4 {
    female_distinct = DISTINCT result.FemaleParticipations;
    female_distinct_max1 = MAX(female_distinct);
    GENERATE group AS Games, female_distinct_max1 AS Max1;
}

join_data = join result by Games, max_participations1 by Games;

join_data = FOREACH join_data {
    GENERATE $0, $1, $2, (int)$3, $4, (int)$6 AS Max1;
}

/*Find the second largest value of female participations per Olympic Games (if it does not consider keep max as second largest value.)*/
grp5 = GROUP join_data BY Games;

max_participations2 = FOREACH grp5 {
    filtered = FILTER join_data BY $3 < $5;
    female_distinct = DISTINCT filtered.$3;
    female_distinct_max2 = MAX(female_distinct);
    GENERATE group AS Games, female_distinct_max2 AS Max2;
}

join_data = join join_data by Games, max_participations2 by Games;

join_data = FOREACH join_data {
    GENERATE $0, $1, $2, (int)$3, $4,  ($7 IS NULL ? (int)$5 : (int)$7) AS Max2;
}

/*Find the third largest value of female participations per Olympic Games (if it does not consider the second largest value as third largest value.)*/
grp6 = GROUP join_data BY Games;

max_participations3 = FOREACH grp6 {
    filtered = FILTER join_data BY $3 < $5;
    female_distinct = DISTINCT filtered.$3;
    female_distinct_max3 = MAX(female_distinct);
    GENERATE group AS Games, female_distinct_max3 AS Max3;
}

join_data = join join_data by Games, max_participations3 by Games;

join_data = FOREACH join_data {
    GENERATE $0, $1, $2, (int)$3, $4, ($7 IS NULL ? (int)$5 : (int)$7) AS Max3;
}

/*Filter data per game so that only the three top teams regarding female participation are kept.
  Then order the filtered result by the number of female participation in descending order
   and in case of ties order the results alphabetically by the respective sport name.
*/

grp7 = GROUP join_data BY Games;

filtered = FOREACH grp7 {
    filtered = FILTER join_data BY $3 >= $5;
    ordered = ORDER filtered BY $3 DESC, $4 ASC;
    GENERATE FLATTEN(ordered);
}

final = FOREACH filtered {
    GENERATE $0 AS Games, $1 AS Team, $2 AS NOC , (int)$3 AS FemaleParticipations, $4 AS Sport;
}

STORE final INTO 'question3' USING PigStorage(',');

