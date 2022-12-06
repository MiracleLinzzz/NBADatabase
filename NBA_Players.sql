USE NBA;
DROP TABLE IF EXISTS player_game_stats;
DROP TABLE IF EXISTS game_basics;
DROP TABLE IF EXISTS player_game_stats;
DROP TABLE IF EXISTS NBA.SEASONS;

-- DATA CLEANING & SEASONAL STATS CALCULATION
-- CREATE TABLE SEASONS
DROP TABLE IF EXISTS NBA.SEASONS;
CREATE TABLE IF NOT EXISTS NBA.SEASONS(
	SEASON varchar(16) NOT NULL,
    SEASON_TYPE varchar(16),
    SEASON_START DATE DEFAULT NULL,
    SEASON_END DATE DEFAULT NULL,
    PRIMARY KEY (SEASON, SEASON_TYPE));
INSERT INTO NBA.SEASONS(
	SEASON,
    SEASON_TYPE,
    SEASON_START,
    SEASON_END) 
		VALUES
			("2020","allstar","2020-02-14","2020-02-16"),
            ("2020","playoffs","2020-08-17","2020-10-11"),
			("2020","preseason","2020-12-11", "2020-12-19"),
            ("2020", "regular","2020-12-22","2021-05-16"),
            ("2021","allstar","2021-03-07","2021-03-09"),
            ("2021","playoffs","2021-05-22","2021-07-20"),
			("2021", "preseason","2021-10-03", "2021-10-15"),
            ("2021","regular","2021-10-19","2022-04-10");

-- ADD PRIMARY KEY TO TABLE PLAYER_BASICS
ALTER TABLE PLAYER_BASICS
    ADD CONSTRAINT PLAYER_BASICS_pk
        PRIMARY KEY (PLAYER_ID);

-- DATA CLEANING FOR TABLE TEAMS
ALTER TABLE teams
    DROP column LEAGUE_ID;

UPDATE teams
SET ARENA_CAPACITY = NULL
WHERE ARENA_CAPACITY = '';

-- DATA CLEANING FOR TABLE games
UPDATE games
SET PTS_home = NULL
WHERE PTS_home = 'None';

UPDATE games
SET PTS_away = NULL
WHERE PTS_away = 'None';

-- CREATE TABLE GAME_BASICS AND INSERT DATA
create table GAME_BASICS
(
    GAME_ID         varchar(64) not null
        primary key,
    GAME_DATE_EST   date        null,
    HOME_TEAM_ID    varchar(64) null,
    VISITOR_TEAM_ID varchar(64) null,
    PTS_home        int null,
    PTS_away        int null,
    HOME_TEAM_WINS  varchar(64) null,
    SEASON          varchar(16) null,
    SEASON_TYPE     varchar(16) null,
    constraint GAME_BASICS_GAME_ID_uindex
        unique (GAME_ID),
    constraint GAME_BASICS_SEASONS_SEASON_SEASON_TYPE_fk
        foreign key (SEASON, SEASON_TYPE) references SEASONS (SEASON, SEASON_TYPE),
    constraint GAME_BASICS_teams_TEAM_ID_fk
        foreign key (HOME_TEAM_ID) references teams (TEAM_ID),
    constraint GAME_BASICS_teams_TEAM_ID_fk_2
        foreign key (VISITOR_TEAM_ID) references teams (TEAM_ID)
);

insert into GAME_BASICS (GAME_ID, GAME_DATE_EST, HOME_TEAM_ID, VISITOR_TEAM_ID, PTS_home, PTS_away, HOME_TEAM_WINS, SEASON, SEASON_TYPE)
SELECT DISTINCT G.GAME_ID,
                CAST(G.GAME_DATE_EST AS DATE) AS GAME_DATE_EST,
                G.HOME_TEAM_ID,
                G.VISITOR_TEAM_ID,
                PTS_home,
                PTS_away,
                G.HOME_TEAM_WINS,
                S.SEASON,
                S.SEASON_TYPE
FROM (GAMES G LEFT JOIN SEASONS S
ON CAST(G.GAME_DATE_EST AS DATE) BETWEEN S.SEASON_START AND S.SEASON_END);

-- CREATE TABLE player_game_stats, INSERT DATA
create table player_game_stats as (
select distinct GAME_ID,
                TEAM_ID,
                PLAYER_ID,
                START_POSITION,
                MIN,
                FGM,
                FGA,
                IF(FG_PCT != "None", FORMAT(FG_PCT, 3), NULL) AS FG_PCT,
                FG3M,
                FG3A,
                IF(FG3_PCT != "None", FORMAT(FG3_PCT, 3), NULL) AS FG3_PCT,
                FTM,
                FTA,
                IF(FT_PCT != "None", FORMAT(FT_PCT, 3), NULL) AS FT_PCT,
                OREB,
                DREB,
                REB,
                AST,
                STL,
                BLK,
                TOs,
                PF,
                PTS,
                PLUS_MINUS
from GAME_DETAILS);

-- DATA CLEANING FOR TABLE player_game_stats

update player_game_stats
set  START_POSITION = NULL 
where START_POSITION = 'None';

update player_game_stats
set MIN = NULL,
    FGM = NULL,
    FGA = NULL,
    FG_PCT = NULL,
    FG3M = NULL,
    FG3A = NULL,
    FG3_PCT = NULL,
    FTM = NULL,
    FTA = NULL,
    FT_PCT = NULL,
    OREB = NULL,
    DREB = NULL,
    REB = NULL,
    AST = NULL,
    STL = NULL,
    BLK = NULL,
    TOs = NULL,
    PF = NULL,
    PTS = NULL,
    PLUS_MINUS = NULL
where MIN = 'None';

update player_game_stats
set  PLUS_MINUS = NULL 
where PLUS_MINUS = 'None';

delete from player_game_stats
where PLAYER_ID = '1627759' and GAME_ID = '22000070' and FGA = '20.0';

delete from player_game_stats
where PLAYER_ID = '1629684' and GAME_ID = '22000070' and FGA = '6.0';

delete from player_game_stats
where PLAYER_ID = '1630165' and GAME_ID = '22000070' and FGA = '5.0';

delete from player_game_stats
where PLAYER_ID = '1630191' and GAME_ID = '22000070' and FGA = '4.0';

delete from player_game_stats
where PLAYER_ID = '1630202' and GAME_ID = '22000070' and BLK = '1.0';

delete from player_game_stats
where PLAYER_ID = '203924' and GAME_ID = '22000070' and DREB = '3.0';

with temp as (
    select distinct player_game_stats.player_id
    from player_game_stats
    where PLAYER_ID not in (select distinct PLAYER_ID from PLAYER_BASICS)
)
delete from player_game_stats
where PLAYER_ID in (select * from temp);

-- SET CONSTRAINTS FOR TABLE player_game_stats
alter table player_game_stats
    add constraint player_game_stats_pk
        primary key (GAME_ID, PLAYER_ID);

alter table player_game_stats
    add constraint player_game_stats_GAME_BASICS_GAME_ID_fk
        foreign key (GAME_ID) references GAME_BASICS (GAME_ID);

alter table player_game_stats
    add constraint player_game_stats_PLAYER_BASICS_PLAYER_ID_fk
        foreign key (PLAYER_ID) references PLAYER_BASICS (PLAYER_ID);

alter table player_game_stats
    add constraint player_game_stats_teams_TEAM_ID_fk
        foreign key (TEAM_ID) references teams (TEAM_ID);

-- ALL DATA HAVE BEEN CLEANED, DROP ORIGINAL TABLES
DROP TABLE IF EXISTS GAMES;
DROP TABLE IF EXISTS GAME_DETAILS;

-- CALCULATION FOR TABLE PLAYER_TEAM_SEASON_STATS
DROP TABLE IF EXISTS NBA.PLAYER_TEAM_SEASON_STATS;

CREATE TABLE NBA.PLAYER_TEAM_SEASON_STATS AS
WITH temp1 AS (
    SELECT DISTINCT GAME_ID,
           PLAYER_ID,
           p.TEAM_ID,
           ABBREVIATION,
           MIN,
           FGM,
           FGA,
           FG3M,
           FG3A,
           FTM,
           FTA,
           OREB,
           DREB,
           REB,
           AST,
           STL,
           BLK,
           TOs,
           PF,
           PTS,
           PLUS_MINUS
    FROM player_game_stats p
    LEFT JOIN TEAMS t
    ON p.TEAM_ID = t.TEAM_ID
),
  temp2 AS (
      SELECT DISTINCT t1.PLAYER_ID,
             gb.SEASON,
             gb.SEASON_TYPE,
             t1.TEAM_ID,
             t1.ABBREVIATION,
             ROUND(AVG(COALESCE(t1.MIN, 0 )),1) AS MIN,
             ROUND(AVG(COALESCE(t1.PTS, 0 )),1) AS PTS,
             ROUND(AVG(COALESCE(t1.FGM, 0 )),1) AS FGM,
             ROUND(AVG(COALESCE(t1.FGA, 0 )),1) AS FGA,
             FORMAT(ROUND(AVG(COALESCE(t1.FGM, 0 )),1)
                        /IF(ROUND(AVG(COALESCE(t1.FGA, 0 )),1) != 0, ROUND(AVG(COALESCE(t1.FGA, 0 )), 1), 1) * 100, 1) AS FG_PCT,
             ROUND(AVG(COALESCE(t1.FG3M, 0 )),1) AS FG3M,
             ROUND(AVG(COALESCE(t1.FG3A, 0 )),1) AS FG3A,
             FORMAT(ROUND(AVG(COALESCE(t1.FG3M, 0 )),1)
                        /IF(ROUND(AVG(COALESCE(t1.FG3A, 0 )),1) != 0, ROUND(AVG(COALESCE(t1.FG3A, 0 )),1), 1) * 100, 1) AS FG3_PCT,
             ROUND(AVG(COALESCE(t1.FTM, 0 )),1) AS FTM,
             ROUND(AVG(COALESCE(t1.FTA, 0 )),1) AS FTA,
             FORMAT(ROUND(AVG(COALESCE(t1.FTM, 0 )),1)
                        /IF(ROUND(AVG(COALESCE(t1.FTA, 0 )),1) != 0, ROUND(AVG(COALESCE(t1.FTA, 0 )),1), 1) * 100, 1) AS FT_PCT,
             ROUND(AVG(COALESCE(t1.OREB, 0)),1) AS OREB,
             ROUND(AVG(COALESCE(t1.DREB, 0)),1) AS DREB,
             ROUND(AVG(COALESCE(t1.AST, 0)),1) AS AST,
             ROUND(AVG(COALESCE(t1.STL, 0 )),1) AS STL,
             ROUND(AVG(COALESCE(t1.BLK, 0 )),1) AS BLK,
             ROUND(AVG(COALESCE(t1.TOs, 0 )),1) AS TOs,
             ROUND(AVG(COALESCE(t1.PF, 0 )),1) AS PF,
             ROUND(AVG(COALESCE(t1.PLUS_MINUS, 0 )),1) AS PLUS_MINUS
      FROM temp1 t1
      LEFT JOIN GAME_BASICS gb
        ON t1.GAME_ID = gb.GAME_ID
      GROUP BY t1.PLAYER_ID, t1.TEAM_ID, gb.SEASON, gb.SEASON_TYPE, t1.ABBREVIATION
  )
SELECT * FROM TEMP2;

