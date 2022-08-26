create external table mendix.git_commits
 `commit_date_ts` string COMMENT 'commit_date_ts',
 `commit_date` string COMMENT 'date when git event commited',
 `commit_event` string COMMENT 'commit_event'
 )
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
'output/paht'
;