/* ============================================================================
  

   QUESTION 2:
     "What is the total uptime and downtime of the whole production floor?"

   QUESTION 3:
     "Which production line had the most downtime, and how much was that?"

   DEFINITIONS
   ─────────────────────────────────────────────────────────────────────────
   UPTIME   = time a line spends INSIDE a session (START to STOP).
              = SUM of all duration_seconds for that line.

   DOWNTIME = time a line spends BETWEEN sessions (STOP to next START).
              = SUM of gaps between consecutive sessions on the same line.

   Open sessions (NULL stop_timestamp) do NOT contribute to downtime.
  
   PART 1 - BUSINESS QUESTION 2
   Total uptime and downtime per production line, plus a floor summary row
   ============================================================================*/


WITH

sessions_with_prev AS
(
    SELECT
        production_line_id,
        session_index,
        start_timestamp,
        stop_timestamp,
        duration_seconds,
        LAG(stop_timestamp) OVER (
            PARTITION BY production_line_id
            ORDER BY     start_timestamp
        ) AS prev_stop_timestamp
    FROM production.vw_production_sessions
),

line_summary AS
(
    SELECT
        production_line_id,
        SUM(ISNULL(duration_seconds, 0))           AS uptime_seconds,
        SUM(
            CASE
                WHEN prev_stop_timestamp IS NOT NULL
                 AND start_timestamp > prev_stop_timestamp
                    THEN CAST(
                             DATEDIFF(SECOND, prev_stop_timestamp, start_timestamp)
                         AS BIGINT)
                ELSE 0
            END
        )                                          AS downtime_seconds
    FROM sessions_with_prev
    GROUP BY production_line_id
),

line_formatted AS
(
    SELECT
        production_line_id,
        uptime_seconds,
        downtime_seconds,
        RIGHT('00' + CAST(uptime_seconds / 3600 AS NVARCHAR(10)), 2) + ':'
        + RIGHT('00' + CAST((uptime_seconds   % 3600) / 60 AS NVARCHAR(2)), 2) + ':'
        + RIGHT('00' + CAST( uptime_seconds   % 60         AS NVARCHAR(2)), 2)
                                                           AS uptime_hms,
        RIGHT('00' + CAST(downtime_seconds / 3600 AS NVARCHAR(10)), 2) + ':'
        + RIGHT('00' + CAST((downtime_seconds % 3600) / 60 AS NVARCHAR(2)), 2) + ':'
        + RIGHT('00' + CAST( downtime_seconds % 60         AS NVARCHAR(2)), 2)
                                                           AS downtime_hms
    FROM line_summary
)

SELECT
   
  
    sum(uptime_seconds) as total_sec_uptime,
    sum(downtime_seconds) as total_sec_downtme
FROM  line_formatted


GO


/* ============================================================================
   PART 2 - BUSINESS QUESTION 3
   Which production line had the most downtime?
   ============================================================================ */

WITH

sessions_with_prev AS
(
    SELECT
        production_line_id,
        start_timestamp,
        stop_timestamp,
        LAG(stop_timestamp) OVER (
            PARTITION BY production_line_id
            ORDER BY     start_timestamp
        ) AS prev_stop_timestamp
    FROM production.vw_production_sessions
),

line_downtime AS
(
    SELECT
        production_line_id,
        SUM(
            CASE
                WHEN prev_stop_timestamp IS NOT NULL
                 AND start_timestamp > prev_stop_timestamp
                    THEN CAST(
                             DATEDIFF(SECOND, prev_stop_timestamp, start_timestamp)
                         AS BIGINT)
                ELSE 0
            END
        ) AS downtime_seconds
    FROM sessions_with_prev
    GROUP BY production_line_id
)

SELECT TOP 1 
    production_line_id,
    downtime_seconds,
    RIGHT('00' + CAST(downtime_seconds / 3600 AS NVARCHAR(10)), 2) + ':'
    + RIGHT('00' + CAST((downtime_seconds % 3600) / 60 AS NVARCHAR(2)), 2) + ':'
    + RIGHT('00' + CAST( downtime_seconds % 60         AS NVARCHAR(2)), 2)
                                            AS downtime_hms
FROM  line_downtime
ORDER BY downtime_seconds DESC;
GO




