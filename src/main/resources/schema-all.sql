DROP TABLE IF EXISTS bible;

CREATE TABLE bible (
    id          BIGINT          UNSIGNED    NOT NULL    AUTO_INCREMENT  PRIMARY KEY,
    book        INT             UNSIGNED    NOT NULL,
    citation    VARCHAR(1000)               NOT NULL,
    chapter     SMALLINT        UNSIGNED    NOT NULL,
    verse       SMALLINT        UNSIGNED    NOT NULL,
    version     CHAR(3)                     NOT NULL
);