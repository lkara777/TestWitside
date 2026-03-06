 Data
The data we collect have 3 columns

    production_line_id: the unique identifier of the production line.
    status : the status of production line. Takes three distinct values.
        ON : the production line is operating normally.
        START : the production started and this is generated with the production line initiation.
        END : the production line stopped the operation. This is generated with production line termintation.
    timestamp : the exact timestamp of the production line's status update.



Business Questions

    For production line "gr-np-47", give me table with columns
        start_timestamp: the timestamp with the initiation of the production process.
        stop_timestamp: the timestamp with the termination of the production process after the last initiation.duration: the total duration of the production process.
    What is the total uptime and downtime of the whole production floor?
    Which production line had the most downtime and how much that was?
