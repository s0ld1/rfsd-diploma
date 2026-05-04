#!/usr/bin/env python3
import sys
from pathlib import Path
import duckdb

DATA_DIR = "/Users/admin/.cache/huggingface/hub/datasets--irlspbru--RFSD/snapshots/0211c9905c8f2bbf61029ebe7fe5bea4f2c6184a/RFSD"
OUT_CSV  = "/Users/admin/Downloads/rfsd_region_year.csv"
MIN_YEAR = 2011
MAX_YEAR = 2024

# 32.5 и 32.50 — в реальных данных ОКВЭД хранится как "32.50", "32.50.1" и частично "32.5" и т.д.
HT_OKVED_PREFIXES = [
    "20", "21", "26", "27", "28", "29", "30", "32.5", "32.50", "33",
    "50", "51", "61", "62", "63", "64", "65", "66",
    "69", "70", "71", "72", "75", "78", "85", "86", "87", "88"
]

EXCLUDED_REGIONS = ("9900", "9000", "9300", "9400", "9500")

def build_ht_regex(prefixes):
    parts = [p.replace(".", r"\.") for p in prefixes]
    return r"^(?:" + "|".join(parts) + r")(?:\.|$)"

def main():
    parquet_glob = str(Path(DATA_DIR) / "**" / "*.parquet")
    out_csv = Path(OUT_CSV)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=8;")

    con.execute(f"""
        CREATE OR REPLACE VIEW base AS
        SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=1);
    """)

    ht_regex = build_ht_regex(HT_OKVED_PREFIXES)
    excluded = ", ".join(f"'{r}'" for r in EXCLUDED_REGIONS)

    # Шаг 1: Фильтрация
    # - убираем NULL region_taxcode
    # - убираем исключённые регионы (Байконур, Запорожье, Донецк, Луганск, Херсон)
    # - диапазон лет
    con.execute(f"""
        CREATE OR REPLACE VIEW filtered AS
        SELECT
            CAST(year          AS INTEGER) AS year,
            CAST(region_taxcode AS VARCHAR) AS region_taxcode,
            CAST(region         AS VARCHAR) AS region_name,
            CAST(okved          AS VARCHAR) AS okved,
            COALESCE(CAST(inn AS VARCHAR), CAST(ogrn AS VARCHAR)) AS firm_id,
            CAST(line_2110      AS DOUBLE)  AS revenue,
            regexp_matches(CAST(okved AS VARCHAR), '{ht_regex}') AS is_ht
        FROM base
        WHERE region_taxcode IS NOT NULL
          AND CAST(region_taxcode AS VARCHAR) NOT IN ({excluded})
          AND CAST(year AS INTEGER) BETWEEN {MIN_YEAR} AND {MAX_YEAR};
    """)

    # Шаг 2: одна строка на фирму × год × регион
    # MAX(revenue) — убирает дубли по одной фирме
    # MAX(is_ht)   — фирма считается HT если хотя бы один ОКВЭД совпал
    con.execute("""
        CREATE OR REPLACE VIEW firm_year_region AS
        SELECT
            year,
            region_taxcode,
            MIN(region_name)                                    AS region_name,
            firm_id,
            MAX(revenue)                                        AS revenue_firm,
            MAX(CASE WHEN is_ht THEN 1 ELSE 0 END)             AS is_ht_firm
        FROM filtered
        GROUP BY year, region_taxcode, firm_id;
    """)

    # Шаг 3: агрегация по регион × год
    con.execute("""
        CREATE OR REPLACE VIEW region_stats AS
        SELECT
            year,
            region_taxcode,
            MIN(region_name) AS region_name,

            -- Общие счётчики фирм
            COUNT(*)                                                        AS firms_n,
            SUM(CASE WHEN revenue_firm >  0 THEN 1 ELSE 0 END)             AS firms_revenue_pos_n,
            SUM(CASE WHEN revenue_firm =  0 THEN 1 ELSE 0 END)             AS firms_revenue_zero_n,
            SUM(CASE WHEN revenue_firm IS NULL THEN 1 ELSE 0 END)          AS firms_revenue_null_n,
            SUM(CASE WHEN revenue_firm <  0 THEN 1 ELSE 0 END)             AS firms_revenue_neg_n,

            -- Доля фирм с пропущенной выручкой (только NULL)
            CASE WHEN COUNT(*) = 0 THEN NULL
                 ELSE SUM(CASE WHEN revenue_firm IS NULL THEN 1.0 ELSE 0 END) / COUNT(*)
            END AS revenue_missing_share,

            -- HT-счётчики фирм
            SUM(CASE WHEN is_ht_firm = 1 THEN 1 ELSE 0 END)                             AS ht_firms_n,
            SUM(CASE WHEN is_ht_firm = 1 AND revenue_firm >  0 THEN 1 ELSE 0 END)       AS firms_revenue_ht_pos_n,
            SUM(CASE WHEN is_ht_firm = 1 AND revenue_firm =  0 THEN 1 ELSE 0 END)       AS firms_revenue_ht_zero_n,
            SUM(CASE WHEN is_ht_firm = 1 AND revenue_firm IS NULL THEN 1 ELSE 0 END)    AS firms_revenue_ht_null_n,
            SUM(CASE WHEN is_ht_firm = 1 AND revenue_firm <  0 THEN 1 ELSE 0 END)       AS firms_revenue_ht_neg_n,

            -- Доля HT-фирм с пропущенной выручкой (только NULL)
            CASE WHEN SUM(CASE WHEN is_ht_firm = 1 THEN 1 ELSE 0 END) = 0 THEN NULL
                 ELSE SUM(CASE WHEN is_ht_firm = 1 AND revenue_firm IS NULL THEN 1.0 ELSE 0 END)
                      / SUM(CASE WHEN is_ht_firm = 1 THEN 1 ELSE 0 END)
            END AS ht_revenue_missing_share,

            -- Суммы: только >= 0 (отрицательные не учитываются)
            SUM(CASE WHEN revenue_firm >= 0 THEN revenue_firm ELSE 0 END)               AS revenue,
            AVG(CASE WHEN revenue_firm >  0 THEN revenue_firm ELSE NULL END)            AS mean_revenue,
            median(CASE WHEN revenue_firm >  0 THEN revenue_firm ELSE NULL END)         AS median_revenue,

            SUM(CASE WHEN is_ht_firm = 1 AND revenue_firm >= 0 THEN revenue_firm ELSE 0 END)           AS ht_revenue,
            AVG(CASE WHEN is_ht_firm = 1 AND revenue_firm >  0 THEN revenue_firm ELSE NULL END)        AS mean_ht_revenue,
            median(CASE WHEN is_ht_firm = 1 AND revenue_firm >  0 THEN revenue_firm ELSE NULL END)     AS median_ht_revenue

        FROM firm_year_region
        GROUP BY year, region_taxcode;
    """)

    # Шаг 4: сохранить CSV
    # region_taxcode сортируем как число, чтобы порядок был 100, 200, ..., 9200
    con.execute(f"""
        COPY (
            SELECT
                year,
                region_taxcode,
                region_name,
                firms_n,
                firms_revenue_pos_n,
                firms_revenue_zero_n,
                firms_revenue_null_n,
                firms_revenue_neg_n,
                ht_firms_n,
                firms_revenue_ht_pos_n,
                firms_revenue_ht_zero_n,
                firms_revenue_ht_null_n,
                firms_revenue_ht_neg_n,
                revenue,
                mean_revenue,
                median_revenue,
                ht_revenue,
                mean_ht_revenue,
                median_ht_revenue,
                revenue_missing_share,
                ht_revenue_missing_share
            FROM region_stats
            ORDER BY year, TRY_CAST(region_taxcode AS INTEGER)
        )
        TO '{OUT_CSV}'
        (HEADER, DELIMITER ',');
    """)

    rows = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{OUT_CSV}')").fetchone()[0]
    print(f"Готово: {rows} строк сохранено в {OUT_CSV}")

if __name__ == "__main__":
    main()
