/* ============================================================================
   FILE    : 01_create_raw_table.sql
   PURPOSE : Create the staging table that holds every raw status event
             exactly as it arrives from the production floor system.
   
   ============================================================================ */

IF NOT EXISTS (
    SELECT 1 
    FROM sys.schemas 
    WHERE name = 'production'
)
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA production';
END;
/* ----------------------------------------------------------------------------
   STEP 2 – Create the raw events table
   ---------------------------------------------------------------------------- */

    CREATE TABLE production.events
    (
        
        event_id            BIGINT          NOT NULL  IDENTITY(1,1),

        /* The unique code of the production line (e.g. "gr-np-47") */
        production_line_id  NVARCHAR(50)    NOT NULL,
        status              NVARCHAR(10)    NOT NULL,

        /* Exact moment the status change was recorded on the floor */
        event_timestamp     DATETIME2(0)    NOT NULL,   -- precision: seconds

        /* Audit column – records when this row was written to the DWH */
        ingested_at         DATETIME2(0)    NOT NULL    DEFAULT SYSUTCDATETIME(),

        /* ── Constraints ──────────────────────────────────────────────────── */

        CONSTRAINT PK_events
            PRIMARY KEY CLUSTERED (event_id),

        /* Only three status values are valid  */
        CONSTRAINT CK_events_status
            CHECK (status IN ('START', 'ON', 'STOP')),

        /* The same line cannot have two events of the same status at the same
           second – prevents accidental duplicate loads                       */
        CONSTRAINT UQ_events
            UNIQUE (production_line_id, status, event_timestamp)
);

GO


/* ----------------------------------------------------------------------------
   STEP 3 – Create indexes
   ---------------------------------------------------------------------------- */

/* Primary query pattern: filter by line, order by time */
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE  object_id = OBJECT_ID(N'production.events')
      AND  name      = N'IX_events_line_timestamp'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_events_line_timestamp
        ON production.events (production_line_id, event_timestamp);
END;
GO

/* Useful when scanning for all START or STOP events across all lines */
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE  object_id = OBJECT_ID(N'production.events')
      AND  name      = N'IX_events_status'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_events_status
        ON production.events (status)
        INCLUDE (production_line_id, event_timestamp);
END;
GO


/* ============================================================================
   HOW TO LOAD YOUR DATA
   ============================================================================*/
   CREATE TABLE production.events_staging
(
    production_line_id  NVARCHAR(50),
    status              NVARCHAR(10),
    event_timestamp     NVARCHAR(30)   -- load as string first
);
 
BULK INSERT production.events_staging
FROM 'C:\Users\kosgi\Documents\ΙΤ260material\test\dataset.csv'
WITH (
    FIRSTROW        = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
--Move from staging to main table with proper type conversion
INSERT INTO production.events (production_line_id, status, event_timestamp)
SELECT
    production_line_id,
    status,
    CAST(event_timestamp AS DATETIME2(0))
FROM production.events_staging;

/* ============================================================================
   check
   ============================================================================*/
SELECT
    production_line_id,
    COUNT(*)            AS total_events,
    MIN(event_timestamp)AS first_event,
    MAX(event_timestamp)AS last_event
FROM production.events
GROUP BY production_line_id
ORDER BY production_line_id;
GO
  /* ============================================================================
   μετά από την έρευνα των δεδομένων είδα ότι μονο το 47 έχει start/stop τα υπολοιπα δεν είχαν,
   το 55 μόνο on, το 22 start χωρις stop και το 08 on/stop. Οπότε θεώρησα ότι αυτά τα καταγράψαμε
   σε μια περιοδο που χάσαμε τα events αυτά και επειδή στην διάρκεια που έχουμε μόνο δούλευαν
   πρόσθεσα τα start/stop για να μπορέσω να δουλέψω. Το start είναι ένα λεπτό πριν το πρώτο on, και το stop
   ένα λεπτό πριν το τελευταίο on.
   ============================================================================*/

   INSERT INTO production.events (production_line_id, status, event_timestamp)
VALUES
    
    ('gr-np-55', 'START', '2020-10-07 00:59:00'),
    ('gr-np-55', 'STOP',  '2020-10-07 06:01:00'),
    ('gr-np-22', 'STOP',  '2020-10-07 05:45:02'),
    ('gr-np-08', 'START', '2020-10-07 01:00:00');