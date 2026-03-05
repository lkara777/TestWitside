/* ============================================================================


   THE LOGIC 
   ───────────────────
   The raw event table has three types of rows per line:

       START  → marks the beginning of a production run
       ON     → heartbeat signal every 15 min (line still running)
       STOP   → marks the end of a production run

   To reconstruct sessions we need to PAIR each START with the very next STOP
   on the same line.  SQL Server's LEAD() window function lets us "peek" at the
   next row's value without a self-join.
   ============================================================================ */

CREATE OR ALTER VIEW production.vw_production_sessions
AS

WITH

/* 
   We only need START and STOP events to pair sessions.
   Result: one row per START or STOP event, per line.                        */
boundary_events AS
(
    SELECT
        production_line_id,
        status,
        event_timestamp
    FROM  production.events
    WHERE status IN ('START', 'STOP')
),

/* ── CTE 2: enriched ───────────────────────────────────────────────────────
   For every row in boundary_events, look ahead at the IMMEDIATELY FOLLOWING
   row for the same production line (PARTITION BY production_line_id) ordered
   by time (ORDER BY event_timestamp).

   LEAD(event_timestamp, 1) → the timestamp of the very next event
   LEAD(status, 1)          → the status   of the very next event

   If there is no next row (the line has no later event), LEAD returns NULL.

         */
enriched AS
(
    SELECT
        production_line_id,
        status,
        event_timestamp,
        LEAD(event_timestamp, 1) OVER (
            PARTITION BY production_line_id   -- look ahead within the same line only
            ORDER BY     event_timestamp      -- pick the chronologically next event
        ) AS next_timestamp,
        LEAD(status, 1) OVER (
            PARTITION BY production_line_id
            ORDER BY     event_timestamp
        ) AS next_status
    FROM  boundary_events
),

/* ── CTE 3: sessions_raw ───────────────────────────────────────────────────
   Keep only START rows.  For each START:
     • If the very next event for that line is a STOP  → normal session end
     • If the very next event is another START          → the first session was
         never closed; record stop_timestamp as NULL
     • If there is no next event at all (NULL)          → session still open;
         record stop_timestamp as NULL

                                                             */
sessions_raw AS
(
    SELECT
        production_line_id,
        event_timestamp            AS start_timestamp,
        CASE
            WHEN next_status = 'STOP'
                THEN next_timestamp   -- normal: next event is the closing STOP
            ELSE
                NULL                  -- open session or consecutive STARTs
        END                        AS stop_timestamp
    FROM  enriched
    WHERE status = 'START'
)

/* ── Final SELECT ──────────────────────────────────────────────────────────
   Add a per-line session counter and compute the duration two ways:
     • duration_seconds  – an integer, easy to aggregate with SUM / AVG
     • duration_hms      – a formatted string "HH:MM:SS" for display

   DATEDIFF(SECOND, start, stop) returns an INT; we convert to BIGINT first
                            */
SELECT
   
    ROW_NUMBER() OVER (
        PARTITION BY production_line_id
        ORDER BY     start_timestamp
    )                                                          AS session_index,

    production_line_id,
    start_timestamp,
    stop_timestamp,

    /* Duration in total seconds  (NULL when session is still open) */
    CASE
        WHEN stop_timestamp IS NOT NULL
            THEN CAST(DATEDIFF(SECOND, start_timestamp, stop_timestamp) AS BIGINT)
        ELSE
            NULL
    END                                                        AS duration_seconds,

   
    CASE
        WHEN stop_timestamp IS NOT NULL
            THEN
                /* Convert total seconds → hours, minutes, remainder-seconds */
                RIGHT('00' + CAST(
                        DATEDIFF(SECOND, start_timestamp, stop_timestamp) / 3600
                      AS NVARCHAR(10)), 2)
                + ':'
                + RIGHT('00' + CAST(
                        (DATEDIFF(SECOND, start_timestamp, stop_timestamp) % 3600) / 60
                      AS NVARCHAR(2)), 2)
                + ':'
                + RIGHT('00' + CAST(
                        DATEDIFF(SECOND, start_timestamp, stop_timestamp) % 60
                      AS NVARCHAR(2)), 2)
        ELSE
            N'open – no STOP yet'
    END                                                        AS duration_hms

FROM  sessions_raw;
GO




SELECT *
FROM   production.vw_production_sessions
ORDER BY production_line_id, start_timestamp;
GO
