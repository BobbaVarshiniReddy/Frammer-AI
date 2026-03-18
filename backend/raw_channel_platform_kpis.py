import pyarrow as pa
from database import get_connection


def get_kpi14_platform_publish_distribution() -> tuple[pa.Table, str]:
    """
    KPI-14: For every platform, the percentage share of total published
    videos across all platforms.
    Formula: SUM(Platform Column) / SUM(All Platform Columns) × 100

    Returns
    -------
    table       : columns ["platform", "total_published", "distribution_pct"]
                  Rows sorted by distribution_pct DESC.
    description : str
    """
    con = get_connection()
    table = con.execute("""
        WITH platform_totals AS (
            SELECT
                'Facebook'   AS platform, SUM("Facebook")   AS total FROM raw_channels
            UNION ALL SELECT 'Instagram',  SUM("Instagram")  FROM raw_channels
            UNION ALL SELECT 'Linkedin',   SUM("Linkedin")   FROM raw_channels
            UNION ALL SELECT 'Reels',      SUM("Reels")      FROM raw_channels
            UNION ALL SELECT 'Shorts',     SUM("Shorts")     FROM raw_channels
            UNION ALL SELECT 'X',          SUM("X")          FROM raw_channels
            UNION ALL SELECT 'Youtube',    SUM("Youtube")    FROM raw_channels
            UNION ALL SELECT 'Threads',    SUM("Threads")    FROM raw_channels
        ),
        grand_total AS (
            SELECT SUM(total) AS grand FROM platform_totals
        )
        SELECT
            pt.platform,
            pt.total                                                        AS "total_published",
            ROUND(100.0 * pt.total / NULLIF(gt.grand, 0), 2)               AS "distribution_pct"
        FROM raw_channel_platform
        CROSS JOIN grand_total gt
        ORDER BY "distribution_pct" DESC
    """).arrow()
    con.close()
    return table, "KPI-14 – Platform Publish Distribution (%)"


def get_kpi08_zero_publish_channel_count() -> tuple[pa.Table, str]:
    """
    KPI-08 (partial): Count of channels that have zero publishes across
    ALL platforms — a channel-health / pipeline signal.
    Formula: COUNT(rows WHERE all platform cols = 0)

    Returns
    -------
    table       : columns ["category", "channel_count"]
                  Two rows: 'zero_publish' and 'active'.
    description : str
    """
    con = get_connection()
    table = con.execute("""
        WITH classified AS (
            SELECT
                CASE
                    WHEN (
                        COALESCE("Facebook",   0) +
                        COALESCE("Instagram",  0) +
                        COALESCE("Linkedin",   0) +
                        COALESCE("Reels",      0) +
                        COALESCE("Shorts",     0) +
                        COALESCE("X",          0) +
                        COALESCE("Youtube",    0) +
                        COALESCE("Threads",    0)
                    ) = 0 THEN 'zero_publish'
                    ELSE 'active'
                END AS category
            FROM raw_channel_platform
        )
        SELECT
            category,
            COUNT(*) AS "channel_count"
        FROM classified
        GROUP BY category
        ORDER BY category = 'zero_publish' DESC   -- zero_publish row first
    """).arrow()
    con.close()
    return table, "KPI-08 – Zero-Publish Channel Count (platform view)"


def get_kpi08_zero_publish_channel_rate() -> tuple[pa.Table, str]:
    """
    KPI-08 (extended): Percentage of channels with zero publishes,
    alongside the active channel share — useful for dashboards.
    Formula: zero_publish_count / total_channels × 100

    Returns
    -------
    table       : columns ["category", "channel_count", "share_pct"]
                  Two rows: 'zero_publish' and 'active'.
    description : str
    """
    con = get_connection()
    table = con.execute("""
        WITH classified AS (
            SELECT
                CASE
                    WHEN (
                        COALESCE("Facebook",   0) +
                        COALESCE("Instagram",  0) +
                        COALESCE("Linkedin",   0) +
                        COALESCE("Reels",      0) +
                        COALESCE("Shorts",     0) +
                        COALESCE("X",          0) +
                        COALESCE("Youtube",    0) +
                        COALESCE("Threads",    0)
                    ) = 0 THEN 'zero_publish'
                    ELSE 'active'
                END AS category
            FROM raw_channels
        ),
        counts AS (
            SELECT category, COUNT(*) AS channel_count
            FROM classified
            GROUP BY category
        ),
        total AS (
            SELECT SUM(channel_count) AS total_channels FROM counts
        )
        SELECT
            c.category,
            c.channel_count,
            ROUND(100.0 * c.channel_count / NULLIF(t.total_channels, 0), 2) AS "share_pct"
        FROM counts c
        CROSS JOIN total t
        ORDER BY c.category = 'zero_publish' DESC   -- zero_publish row first
    """).arrow()
    con.close()
    return table, "KPI-08 – Zero-Publish Channel Rate (%)"