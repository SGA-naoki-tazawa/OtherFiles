CREATE OR REPLACE TABLE TEST_1BILLION.DA_TEST.MAIN_SAMPLE AS
WITH
    -- Sony_acccountデータにおけるAg_allデータと重複するレコードを抽出
    sony_account_duplication AS(
    SELECT
        SEASON,
        CASE
            WHEN DATA_DEFFINITION LIKE 'SNA　管理顧客ID数%' THEN 'SNA　管理顧客ID数'
            WHEN DATA_DEFFINITION LIKE 'SOLA　管理顧客ID数%' THEN 'SOLA　管理顧客ID数'
            ELSE DATA_DEFFINITION
        END AS DATA_DEFFINITION_REPLACE,    -- ag_allテーブルと結合させる際に使用するため名寄せする。
        SUM(NUMBER) AS NUMBER
    FROM
        TEST_1BILLION.DA_TEST.SONY_ACCOUNT_SAMPLE
    WHERE
        DATA_DEFFINITION != SERVICES    -- この条件の項目がag_allと重複するレコードとなる。
        AND DUPLICATE_TARGET = 'All'    -- この条件のレコードのみが集計対象となる。※Overlap_rateのダッシュボードでは'All'以外の項目も使用する。
        AND DATA_DEFFINITION != 'HPCアプリログのマーケティング利用許諾（DL×許諾率）'    -- こちらの項目は対象から除外している。
    GROUP BY
        SEASON,
        DATA_DEFFINITION_REPLACE
    ),

    -- Ag_allデータからSony_accountデータとの重複分を除外したうえで出力する
    ag_all AS(
    SELECT
        AG.AGGREGATION_DATE,
        AG.DATA_DEFINITION,
        AG.KEY_DATA_ITEM,
        AG.OPCO,
        AG.OTHER_DATA_ITEM,
        AG.REGION,
        AG.SEASON,
        UPPER(AG.SEGMENT) AS SEGMENT,   -- 大文字と小文字の表記ゆれがあるから大文字に統一する。
        AG.SERVICES,
        AG.NUMBER - IFNULL(SA.NUMBER,0) AS NUMBER,  -- NUMBERからsony_accountとの重複分を差し引いて除外する。
        CASE
            WHEN OPCO IN ('SPE','SME','SMEJ')  THEN 'Sony'
            ELSE NULL
        END AS COMPANY,
        'Non_Sony_account' AS IS_SONY_ACCOUNT,
        'Ag_all' AS  DATA_TYPE,     -- データの種類を識別するためのカラム
        CASE
            WHEN OPCO = 'SPE' THEN 'PICTURE'
            WHEN OPCO IN ('SME','SMEJ') THEN 'MUSIC'
            ELSE NULL
        END AS CATEGORY     -- BloomBergのダッシュボードで使用するCATEGORYカラムを作成
    FROM
        TEST_1BILLION.DA_TEST.AG_ALL_SAMPLE AS AG
    LEFT JOIN 
        sony_account_duplication AS SA
    ON
        AG.DATA_DEFINITION = SA.DATA_DEFFINITION_REPLACE
        AND AG.SEASON = SA.SEASON
    WHERE
        AG.NUMBER > IFNULL(SA.NUMBER,0)  -- NUMBERからsony_accountとの重複分を差し引いて除外した結果０以下の値になるレコードをする。
    ),

    -- Sony_accountデータを出力
    sony_account AS(
    SELECT
        AGGREGATION_DATE,
        DATA_DEFFINITION,
        KEY_DATA_ITEM,
        OPCO,
        OTHER_DATA_ITEM,
        REGION,
        SEASON,
        UPPER(SEGMENT) AS SEGMENT,  -- 大文字と小文字の表記ゆれがあるから大文字に統一する。
        SERVICES,
        NUMBER,
        CASE
            WHEN OPCO IN ('SPE','SME','SMEJ')  THEN 'Sony'
            ELSE NULL
        END AS COMPANY,     -- BloomBergのダッシュボードで使用するCOMPANYカラムを作成
        'Sony_account' AS IS_SONY_ACCOUNT,
        'Ag_all' AS  DATA_TYPE,   -- データの種類を識別するためのカラム
        CASE
            WHEN OPCO = 'SPE' THEN 'PICTURE'
            WHEN OPCO IN ('SME','SMEJ') THEN 'MUSIC'
            ELSE NULL
        END AS CATEGORY     -- BloomBergのダッシュボードで使用するCATEGORYカラムを作成
    FROM
        TEST_1BILLION.DA_TEST.SONY_ACCOUNT_SAMPLE
    WHERE
        DUPLICATE_TARGET = 'All'    -- この条件のレコードのみが集計対象となる。※Overlap_rateのダッシュボード'All'以外の項目も使用する。

    ),

    -- BloomBergデータを出力
    bloom_berg AS(
    SELECT
        NULL AS AGGREGATION_DATE,
        DATA_DEFINITION,
        NULL AS KEY_DATA_ITEM,
        OPCO,
        NULL AS OTHER_DATA_ITEM,
        REGION,
        SEASON,
        NULL AS SEGMENT,
        NULL AS SERVICES,
        NUMBER,
        COMPANY,
        NULL AS IS_SONY_ACCOUNT,
        'BloomBerg' AS DATA_TYPE,    -- データの種類を識別するためのカラム
        CASE
            WHEN OPCO = 'SPE' THEN 'PICTURE'
            WHEN OPCO IN ('SME','SMEJ') THEN 'MUSIC'
            ELSE NULL
        END AS CATEGORY     -- BloomBergのダッシュボードで使用するCATEGORYカラムを作成する。
    FROM
        TEST_1BILLION.DA_TEST.BLOOM_BERG_SAMPLE
    WHERE
        NUMBER > 0    -- NUMBERがNULLではない、または0以下ののレコードは除外する。
    )

-- 作成したテーブルをすべてユニオンする。
SELECT * FROM ag_all
UNION ALL
SELECT * FROM sony_account
UNION ALL
SELECT * FROM bloom_berg
;