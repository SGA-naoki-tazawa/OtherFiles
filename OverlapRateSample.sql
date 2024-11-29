CREATE OR REPLACE TABLE TEST_1BILLION.DA_TEST.OVERLAP_RATE_SAMPLE AS
WITH
    sa AS(
    SELECT
        SEASON,
        SERVICES,
        REGION,
        DUPLICATE_TARGET,
        SEGMENT,
        SUM(NUMBER) AS NUMBER_NONALL
    FROM
        TEST_1BILLION.DA_TEST.SONY_ACCOUNT_SAMPLE
    WHERE
        DUPLICATE_TARGET != 'All'
    GROUP BY
        SEASON,
        SERVICES,
        REGION,
        DUPLICATE_TARGET,
        SEGMENT
    ),

    sa_all AS(
    SELECT
        SEASON,
        SERVICES,
        REGION,
        MAX(NUMBER) AS NUMBER_ALL
    FROM
        TEST_1BILLION.DA_TEST.SONY_ACCOUNT_SAMPLE
    WHERE
        DUPLICATE_TARGET = 'All'
    GROUP BY
        SEASON,
        SERVICES,
        REGION
    )

    SELECT
        sa.SEASON,
        sa.SERVICES,
        sa.REGION,
        sa.DUPLICATE_TARGET,
        sa.SEGMENT,
        IFNULL(sa.NUMBER_NONALL,0) AS NUMBER,
        sa_all.NUMBER_ALL AS ALL_NUMBER,
        NUMBER / ALL_NUMBER AS Oberlap_Rate
    FROM
        sa 
    INNER JOIN
        sa_all
    ON
        sa.SERVICES = sa_all.SERVICES
        AND
        sa.REGION = sa_all.REGION
        AND
        sa.SEASON = sa_all.SEASON
     ;