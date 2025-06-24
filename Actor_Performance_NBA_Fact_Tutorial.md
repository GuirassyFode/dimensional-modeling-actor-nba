**PostgreSQL Data Modeling Tutorial: Building an Actor Performance Tracker & NBA Game Fact Table**

This hands-on tutorial walks you through building dimensional models in PostgreSQL, using two real-world cases: tracking actor film performance and creating a fact table for NBA game stats. You'll learn structured types, quality classification, deduplication techniques, and Slowly Changing Dimensions (SCD).

---

## ✨ 1. Create Custom Types

We start by defining custom PostgreSQL types for structured film stats and player performance categories.

```sql
CREATE TYPE film_stats AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');
```

---

## 🎭 2. Create the `actors` Table

This table stores actor data by year, aggregating their yearly films and tagging their performance.

```sql
DROP TABLE IF EXISTS actors;

CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    film TEXT,
    current_year INT,
    votes INT,
    rating FLOAT,
    films film_stats[],
    quality_class quality_class,
    is_active BOOLEAN,
    PRIMARY KEY (actorid, current_year)
);
```

---

## 📊 3. Insert Cumulative Actor Data

Use a `FULL OUTER JOIN` to merge current and previous year data. We classify actors based on average rating:

```sql
INSERT INTO actors
WITH last_year AS (
    SELECT * FROM actors WHERE current_year = 1969
),
this_year AS (
    SELECT * FROM actor_films WHERE year = 1970
)
SELECT
    COALESCE(t.actor, l.actor) AS actor,
    COALESCE(t.actorid, l.actorid) AS actorid,
    COALESCE(t.film, l.film) AS film,
    1970 AS current_year,
    COALESCE(t.votes, l.votes) AS votes,
    COALESCE(t.rating, l.rating) AS rating,

    CASE
        WHEN l.films IS NULL THEN ARRAY[
            ROW(t.film, t.votes, t.rating, t.filmid)::film_stats
        ]
        WHEN t.film IS NOT NULL THEN l.films || ARRAY[
            ROW(t.film, t.votes, t.rating, t.filmid)::film_stats
        ]
        ELSE l.films
    END AS films,

    CASE
        WHEN t.actor IS NOT NULL THEN
            CASE
                WHEN t.rating > 8 THEN 'star'
                WHEN t.rating > 7 THEN 'good'
                WHEN t.rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE l.quality_class
    END AS quality_class,

    t.actor IS NOT NULL AS is_active
FROM this_year t
FULL OUTER JOIN last_year l ON t.actor = l.actor AND t.actorid = l.actorid;
```

---

## 🕓 4. Track Actor History with SCD Type 2

```sql
CREATE TABLE actors_history_scd (
    actorid TEXT,
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date DATE,
    end_date DATE
);

INSERT INTO actors_history_scd
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    DATE_TRUNC('year', MAKE_DATE(current_year, 1, 1)) AS start_date,
    DATE_TRUNC('year', MAKE_DATE(current_year + 1, 1, 1)) - INTERVAL '1 day' AS end_date
FROM actors;
```

---

## 🏀 5. Build the NBA Game Details Fact Table

We deduplicate raw data using `ROW_NUMBER()`, extract key attributes, and transform fields like minutes played.

```sql
DROP TABLE IF EXISTS ftc_game_details;

CREATE TABLE ftc_game_details (
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_start_position TEXT,
    dim_is_playing_at_home BOOLEAN,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_fgm INTEGER,
    m_fga INTEGER,
    m_fg3m INTEGER,
    m_fg3a INTEGER,
    m_ftm INTEGER,
    m_fta INTEGER,
    m_oreb INTEGER,
    m_dreb INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    m_stl INTEGER,
    m_blk INTEGER,
    m_turnovers INTEGER,
    m_pf INTEGER,
    m_pts INTEGER,
    m_plus_minus INTEGER,
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
);
```

```sql
INSERT INTO ftc_game_details
WITH deduped AS (
    SELECT 
        g.game_date_est,
        g.season,
        g.home_team_id,
        g.visitor_team_id,
        gs.*,
        ROW_NUMBER() OVER (PARTITION BY gs.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM public.game_details AS gs
    JOIN public.games g ON gs.game_id = g.game_id
)
SELECT 
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    POSITION('DNP' IN comment) > 0 AS dim_did_not_play,
    POSITION('DND' IN comment) > 0 AS dim_did_not_dress,
    POSITION('NWT' IN comment) > 0 AS dim_not_with_team,
    CASE 
        WHEN min IS NOT NULL AND POSITION(':' IN min) > 0 THEN
            CAST(SPLIT_PART(min, ':', 1) AS REAL) + 
            CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60
        ELSE 0
    END AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;
```

---

## 📈 6. Analyze Player Participation

```sql
SELECT dim_player_name,
       COUNT(*) AS num_games,
       SUM(m_pts) AS total_points,
       COUNT(CASE WHEN dim_did_not_play THEN 1 END) AS bailed_num,
       CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL) / COUNT(*) AS bailed_prc
FROM ftc_game_details
GROUP BY 1
ORDER BY bailed_prc DESC;
```

---

## 💡 Tips for Data Engineers

- Use `ROW_NUMBER()` to clean duplicates without losing information.
- Always coalesce `NULL` fields and apply `CASE` when transforming strings to numbers.
- Maintain naming conventions (`dim_`, `m_`) to clarify dimension vs. measure fields.
- Validate logic with small date ranges before generalizing.

---

### 🔗 Author & Portfolio
Built by [Guirassy Fode](https://github.com/GuirassyFode)  
Upskilling via [**DataExpert.io Labs**](https://dataexpert.io)
