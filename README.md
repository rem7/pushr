
This project uses `gb` to build and `gb vendor` manage dependencies.

```bash
$ git clone git@github.com:rem7/pushr.git
$ cd pushr
$ gb vendor restore
$ gb build all
# drops binary in pushr/bin

# cross compile to linux:
$ GOOS=linux gb build all
```


Create a table with this schema: 
```sql
CREATE TABLE app_log(
	app VARCHAR(16) ENCODE LZO,
	app_ver VARCHAR(16) ENCODE LZO,
	ingest_datetime TIMESTAMP ENCODE LZO,
	event_datetime TIMESTAMP ENCODE LZO,
	hostname  VARCHAR(64) ENCODE LZO,
	filename  VARCHAR(256) ENCODE LZO,
	log_level VARCHAR(16) ENCODE LZO,
	device_tag VARCHAR(64) ENCODE LZO,
	user_tag VARCHAR(64) ENCODE LZO,
	remote_address VARCHAR(64) ENCODE LZO,
	response_bytes INTEGER ENCODE LZO,
	response_ms DOUBLE PRECISION ENCODE BYTEDICT,
	device_type VARCHAR(32) ENCODE LZO,
	os VARCHAR(16) ENCODE LZO,
	os_ver VARCHAR(16) ENCODE LZO,
	browser VARCHAR(32) ENCODE LZO,
	browser_ver VARCHAR(16) ENCODE LZO,
	country VARCHAR(64) ENCODE LZO,
	language VARCHAR(16) ENCODE LZO,
	log_line VARCHAR(MAX) ENCODE LZO
)
DISTSTYLE even
COMPOUND SORTKEY (app,ingest_datetime);
```
```sql
CREATE USER foo_firehose PASSWORD 'bar';

grant select on table app_log to foo_firehose;
```

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Stmt1462251290000",
            "Effect": "Allow",
            "Action": [
                "firehose:PutRecord",
                "firehose:PutRecordBatch"
            ],
            "Resource": [
                "arn:aws:firehose:us-west-2:298262171104:deliverystream/my-firehose"
            ]
        }
    ]
}
```

Firehose Options:
```
GZIP CSV TRUNCATECOLUMNS NULL AS '\\N' TIMEFORMAT 'auto';

app,app_ver,ingest_datetime,event_datetime,hostname,filename,log_level,device_tag,user_tag,remote_address,response_bytes,response_ms,device_type,os,os_ver,browser,browser_ver,country,language,log_line
```


Nginx access.log with optional response time in seconds at the end
```regex
^(?P<remote_address>[^ ]*)\ \-\ (?P<remote_user>[^ ]*)\ \[(?P<event_datetime>[^\]]*)\] \"[^\"]*\"\ (?P<log_level>[\d]*)\ (?P<response_bytes>[-\d]*)\ \"(?P<http_referer>[^\"]*)\"\ \"(?P<http_user_agent>[^\"]*)\"\s?(?P<response_s>[-\d\.]+)?


^(?P<remote_address>[^ ]*)\s\-\s\-\s\[(?P<event_datetime>[^\]]*)\]\s\"[^"]*\"\s(?P<log_level>\d+)\s(?P<response_bytes>\d+)\s\"[^\"]*\"\s\"(?P<user_agent>[^\"]*)\"\s?(?P<response_s>[-\d\.]+)?
```

express-morgan
```regex
^(?P<remote_address>[^\ ]*)\ -\ - \[(?P<event_datetime>[^\]]*)\]\ \"[^\"]*\"\ (?P<log_level>\d+)\ (?P<response_ms>[^\(]*)\(ms\)\ \"[^\"]*\"\ \"(?P<user_agent>[^\"]*)\"
```
