



WITH series AS
(
	SELECT  toDate(now()) - number AS date
	FROM numbers
	(10
	)
)
SELECT  toYear(date)                                                        AS yyyy
       ,toMonth(date)                                                       AS mm
       ,toDayOfMonth(date)                                                  AS dd
       ,LPAD(toString(toHour(toDateTime(date))),2,'0') AS hh -- Format hour AS hh
       ,toString(date)                                                      AS date_str
FROM series;


WITH range AS
(
	SELECT  now() - INTERVAL number SECOND AS range
	FROM numbers(10)
)
SELECT  toYear(range)                                                                AS yyyy
       ,LPAD(toString(toMonth(range)),2,'0')                                         AS mm
       ,LPAD(toString(toDayOfMonth(range)),2,'0')                                    AS dd
       ,LPAD(toString(toHour(range)),2,'0')                                          AS hh
       ,formatDateTime(range,'%Y-%m-%d %H:%i:%S')                                    AS dt_str
       ,formatDateTime(toStartOfDay(range),'%Y-%m-%d %H:%i:%S')                      AS dt_trunc_day_str
       ,formatDateTime(toStartOfHour(range),'%Y-%m-%d %H:%i:%S')                     AS dt_trunc_hour_str
       ,formatDateTime(range + INTERVAL 1 SECOND,'%Y-%m-%d %H:%i:%S')                AS next_dt_str
       ,formatDateTime(toStartOfHour(range + INTERVAL 1 SECOND),'%Y-%m-%d %H:%i:%S') AS next_dt_trunc_hour_str
       ,formatDateTime(toStartOfDay(range + INTERVAL 1 SECOND),'%Y-%m-%d %H:%i:%S')  AS next_dt_trunc_day_str
FROM range;


{{ $table := "rill_ui_telemetry_source" }}
WITH parts AS
(
	SELECT  DISTINCT toDateTime(partition) AS partition
	FROM system.parts
	WHERE database = '{{ .env.database }}'
	AND TABLE = '{{ $table }}'
	AND active = 1
)
SELECT  DISTINCT dates.*
       ,parts.partition
       ,CASE WHEN '{{ .state.max_event_time }}' = '{{ .env.initial_state_value }}' THEN NULL  ELSE '{{ .state.max_event_time }}' AS state
FROM gen_partitions dates
LEFT JOIN parts
ON toString(parts.partition) = toString(dates.dt_trunc_day_str)
WHERE (toDate(parts.partition) <= toDate('1970-01-01')) OR parts.partition IS NULL OR (toDate(dates.dt_trunc_day_str) > toDate({{ .state.max_event_time }}) - INTERVAL 3 {{ .env.granularity }})
ORDER BY dates.dt_trunc_day_str DESC
