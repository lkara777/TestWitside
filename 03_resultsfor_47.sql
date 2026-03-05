/* ============================================================================
 

   QUESTION:
     "For production line 'gr-np-47', give me a table with:
       start_timestamp – when the production process began
       stop_timestamp  – when it terminated after the last initiation
       duration        – the total duration of the production process"
   ============================================================================ */


SELECT
    session_index,
    production_line_id,
    start_timestamp,
    stop_timestamp,
    duration_seconds,
    duration_hms        AS duration
FROM
    production.vw_production_sessions
WHERE
    production_line_id = 'gr-np-47'    
ORDER BY
    start_timestamp;
GO

/* ── EXPECTED RESULT ────────────────────────────────────────────────────────

   session_index | production_line_id | start_timestamp      | stop_timestamp       | duration_seconds | duration
  
   1             | gr-np-47           | 2020-10-07 01:33:20  | 2020-10-07 02:03:20  | 1800             | 00:30:00
   2             | gr-np-47           | 2020-10-07 02:15:02  | 2020-10-07 04:15:02  | 7200             | 02:00:00
   3             | gr-np-47           | 2020-10-07 05:00:00  | 2020-10-07 05:55:17  | 3317             | 00:55:17

  */


