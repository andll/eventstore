# Create series class
``` bash
curl -X 'PUT' -u 'id:secret' \
	https://us0.eventdb.io/<your_id>/house/ \
	-d '{"index" : [
	    {"/electricity" : "sum"}, 
	    {"/temperature" : ["avg", "max"]}
	]}'
```
>Data series with same class shares same index set and policies

# Fill series with data
``` bash
curl -X 'POST' -u 'id:secret' \
	https://us0.eventdb.io/<your_id>/house/alice/ \
	-d '{"electricity" : 15, "temperature" : 20}'
```
#### ... or provide your own timestamps
``` bash
curl -X 'PUT' -u 'id:secret' \
	https://us0.eventdb.io/<your_id>/house/alice/2015.01.30T02:00:00.0000 \
	-d '{"electricity" : 15, "temperature" : 20}'
```
# Query aggregated data
``` bash
curl -X 'POST' -u 'id:secret' \
	https://us0.eventdb.io/<your_id>/house/alice/ \
	-d '{"$from" : "2015.01.30T02:00:00",
	    "$to" : "2015.01.30T03:00:00",
	    "/electricity" : "sum",
	    "/temperature" : "avg"}'
```
``` json
{
    "/electricity" : 15,
    "/temperature" : 20
}
```
### ... and group it
``` bash
curl -X 'POST' -u 'id:secret' \
	https://us0.eventdb.io/<your_id>/house/alice/ \
	-d '{"$from" : "2015.01.30T02:00:00",
	    "$to" : "2015.01.30T03:00:00",
	    "$group" : "1h"
	    "/electricity" : "sum",
	    "/temperature" : "avg"}'
```
``` json
{
    "2015.01.30T02:00:00": {
        "/electricity" : 15,
        "/temperature" : 20
    }
}
```
### ... or fetch it as is
``` bash
curl -u 'id:secret' \
    https://us0.eventdb.io/<your_id>/house/alice/2015.01.30T02:00:00.0000
```
``` json
{
    "electricity" : 15,
    "temperature" : 20
}
```
### ... or request range
``` bash
curl -u 'id:secret' \
https://us0.eventdb.io/<your_id>/house/alice/2015.01.30T02:00:00.0000-2015.01.30T02:00:00.0000
```
``` json
{"2015.01.30T02:00:00.0000": {
    "electricity" : 15,
    "temperature" : 20
}}
```
# Share your data
``` bash
curl -X 'POST' -u 'id:secret' \
	https://us0.eventdb.io/<your_id>/house/alice/?share=read,write
```
``` bash
https://us0.eventdb.io/~jd0xmk8sL
```
### ... and use it from client devices
``` bash
curl -X 'POST' \
	https://us0.eventdb.io/~jd0xmk8sL \
	-d '{"electricity" : 15, "temperature" : 20}'
```


# Questions
* Region in url?