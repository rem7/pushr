## configuration

### YAML
Recommended config file is in YAML 
```yaml
aws_region: us-west-2
aws_access_key: XXXXXXXXXXXXXXXXXXXX
aws_secret_access_key: ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ
app: my-app
app_ver: 1.0

files:
  - name: nginx-access
    file: /var/log/nginx/access.log
    parse_mode: regex
    time_format: 02/Jan/2006:15:04:05 +0000
    retry_file_open: true 
    line_regex: #nginx-access ^(?P<remote_address>[^ ]*)\ \-\ (?P<remote_user>[^ ]*)\ \[(?P<event_datetime>[^\]]*)\] \"[^\"]*\"\ (?P<log_level>[\d]*)\ (?P<response_bytes>[-\d]*)\ \"(?P<http_referer>[^\"]*)\"\ \"(?P<http_user_agent>[^\"]*)\"\s?(?P<response_s>[-\d\.]+)?
    stream: app-log
    buffer_multi_lines: false
  - name: my-app-json
    file: /var/log/node.out.log
    parse_mode: json
    time_format: 2006-01-02T15:04:05.000Z
    stream: app-log
    buffer_multi_lines: false 
    field_mappings:
      log_level: severity
      event_datetime: timestamp
      remote_address: remoteIp
      device_type: deviceModel
      device_tag: deviceId
      user_tag: userId
      country: country
      os: platform

streams:
  - stream_name: app-log
    name: app-log-firehose
    type: firehose
    record_format:
    - {key: app, type: string, length: 16}
    - {key: app_ver, type: string, length: 16}
    - {key: ingest_datetime, type: timestamp}
    - {key: event_datetime, type: timestamp}
    - {key: hostname, type: string, length: 64}
    - {key: filename, type: string, length: 256}
    - {key: log_level, type: string, length: 16}
    - {key: device_tag, type: string, length: 64}
    - {key: user_tag, type: string, length: 64}
    - {key: remote_address, type: string, length: 64}
    - {key: response_bytes, type: integer}
    - {key: response_ms, type: double}
    - {key: device_type, type: string, length: 32}
    - {key: os, type: string, length: 16}
    - {key: os_ver, type: string, length: 16}
    - {key: browser, type: string, length: 32}
    - {key: browser_ver, type: string, length: 16}
    - {key: country, type: string, length: 64}
    - {key: language, type: string, length: 16}
    - {key: log_line, type: string}
```


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
