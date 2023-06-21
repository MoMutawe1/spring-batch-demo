CREATE TABLE IF NOT EXISTS video_games_sales
(
    id              INTEGER,
    name            VARCHAR(50),
    platform        VARCHAR(50),
    year            INTEGER,
    genre           VARCHAR(50),
    publisher       VARCHAR(50),
    na_sales        FLOAT,
    eu_sales        FLOAT,
    jp_sales        FLOAT,
    other_sales     FLOAT,
    global_sales    FLOAT

);