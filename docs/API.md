---
title: Kairo Distributed Key-Value Cluster API v1.0.0
language_tabs:
  - shell: Shell
  - http: HTTP
  - javascript: JavaScript
  - ruby: Ruby
  - python: Python
  - php: PHP
  - java: Java
  - go: Go
toc_footers: []
includes: []
search: true
highlight_theme: darkula
headingLevel: 2

---

<!-- Generator: Widdershins v4.0.1 -->

<h1 id="kairo-distributed-key-value-cluster-api">Kairo Distributed Key-Value Cluster API v1.0.0</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

Production-grade HTTP and Server-Sent Events (SSE) API for the **Kairo** distributed,
linearizable key-value storage engine powered by the Raft consensus protocol.

### Key Capabilities
- **Strict Linearizability**: Raft §8 ReadIndex protocol guarantees linearizable reads across node failovers.
- **Ultra-low Latency Stale Reads**: Optional local in-memory reads bypass consensus for sub-millisecond reads.
- **Distributed Optimistic Concurrency Control (OCC)**: Server-enforced atomic `CompareAndSwap` (CAS).
- **Real-Time Streaming Watches**: Server-Sent Events (`text/event-stream`) with automatic resume and historical replay.
- **Dynamic Cluster Membership**: Online learner onboarding, voter promotion, and node eviction.

Base URLs:

* <a href="http://127.0.0.1:9001">http://127.0.0.1:9001</a>

* <a href="http://127.0.0.1:9002">http://127.0.0.1:9002</a>

* <a href="http://127.0.0.1:9003">http://127.0.0.1:9003</a>

* <a href="http://127.0.0.1:8999">http://127.0.0.1:8999</a>

Web: <a href="https://github.com/ABUDIYAAAA/kairo">Kairo Team</a> 
License: <a href="https://www.apache.org/licenses/LICENSE-2.0.html">Apache-2.0</a>

# Authentication

- HTTP Authentication, scheme: basic HTTP Basic Authentication enforced on all operational endpoints.
Credentials persist across reboots in `data/auth.json`. Default: `kairo:kairo-secret`.

<h1 id="kairo-distributed-key-value-cluster-api-default">Default</h1>

## Liveness probe

<a id="opIdgetHealthz"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:9001/healthz \
  -H 'Accept: application/json'

```

```http
GET http://127.0.0.1:9001/healthz HTTP/1.1
Host: 127.0.0.1:9001
Accept: application/json

```

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/healthz',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'application/json'
}

result = RestClient.get 'http://127.0.0.1:9001/healthz',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('http://127.0.0.1:9001/healthz', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('GET','http://127.0.0.1:9001/healthz', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/healthz");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("GET", "http://127.0.0.1:9001/healthz", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`GET /healthz`

Returns HTTP 200 if the node HTTP server process is running. Bypasses authentication.

> Example responses

> 200 Response

```json
{
  "status": "ok"
}
```

<h3 id="liveness-probe-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Node is alive|Inline|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|Node process unresponsive|[ErrorResponse](#schemaerrorresponse)|

<h3 id="liveness-probe-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» status|string|false|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Kubernetes liveness probe

<a id="opIdgetLivez"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:9001/livez \
  -H 'Accept: application/json'

```

```http
GET http://127.0.0.1:9001/livez HTTP/1.1
Host: 127.0.0.1:9001
Accept: application/json

```

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/livez',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'application/json'
}

result = RestClient.get 'http://127.0.0.1:9001/livez',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('http://127.0.0.1:9001/livez', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('GET','http://127.0.0.1:9001/livez', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/livez");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("GET", "http://127.0.0.1:9001/livez", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`GET /livez`

Alias of `/healthz`. Bypasses authentication.

> Example responses

> 200 Response

```json
{
  "status": "ok"
}
```

<h3 id="kubernetes-liveness-probe-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Node is alive|Inline|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|Node process unresponsive|[ErrorResponse](#schemaerrorresponse)|

<h3 id="kubernetes-liveness-probe-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» status|string|false|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Node and cluster status

<a id="opIdgetStatus"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:9001/status \
  -H 'Accept: application/json'

```

```http
GET http://127.0.0.1:9001/status HTTP/1.1
Host: 127.0.0.1:9001
Accept: application/json

```

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/status',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'application/json'
}

result = RestClient.get 'http://127.0.0.1:9001/status',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('http://127.0.0.1:9001/status', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('GET','http://127.0.0.1:9001/status', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/status");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("GET", "http://127.0.0.1:9001/status", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`GET /status`

Returns the current Raft role, term, leader ID, topology revision, and peer heartbeat health.

> Example responses

> 200 Response

```json
{
  "node_id": 1,
  "role": "Leader",
  "term": 4,
  "leader_id": 1,
  "conf_rev": 3,
  "peer_health": {
    "2": true,
    "3": true
  }
}
```

<h3 id="node-and-cluster-status-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Operational node status|[StatusResponse](#schemastatusresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|X-Kairo-Leader-Id|integer||Node ID of the currently acknowledged Raft leader.|
|200|X-Kairo-Term|integer||Current monotonic Raft election term.|
|200|X-Kairo-Conf-Rev|integer||Monotonic configuration revision counter for cluster topology.|
|200|X-Kairo-Active-Peers|string||Comma-separated list of active peer IDs currently reachable by the leader.|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Retrieve key-value pair

<a id="opIdgetKey"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:9001/get?key=configs%2Fcluster-name \
  -H 'Accept: application/json'

```

```http
GET http://127.0.0.1:9001/get?key=configs%2Fcluster-name HTTP/1.1
Host: 127.0.0.1:9001
Accept: application/json

```

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/get?key=configs%2Fcluster-name',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'application/json'
}

result = RestClient.get 'http://127.0.0.1:9001/get',
  params: {
  'key' => 'string'
}, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('http://127.0.0.1:9001/get', params={
  'key': 'configs/cluster-name'
}, headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('GET','http://127.0.0.1:9001/get', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/get?key=configs%2Fcluster-name");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("GET", "http://127.0.0.1:9001/get", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`GET /get`

Retrieves the value, per-key version, and cluster modification revision for a key.
- **Linearizable read (default)**: Quorum confirmed via Raft §8 ReadIndex.
- **Stale read (`stale=true` or `consistent=false`)**: Read directly from local in-memory state machine.

<h3 id="retrieve-key-value-pair-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|key|query|string|true|Target key to retrieve.|
|stale|query|boolean|false|When `true`, serves sub-millisecond read directly from local memory without Raft round-trip.|
|consistent|query|boolean|false|When `false`, acts as alias for `stale=true`.|

> Example responses

> 200 Response

```json
{
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "version": 1,
  "mod_revision": 42
}
```

<h3 id="retrieve-key-value-pair-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Key found and retrieved successfully|[Item](#schemaitem)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Missing key query parameter|[ErrorResponse](#schemaerrorresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Key does not exist|[ErrorResponse](#schemaerrorresponse)|
|503|[Service Unavailable](https://tools.ietf.org/html/rfc7231#section-6.6.4)|Cluster has no elected leader|[ErrorResponse](#schemaerrorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|X-Kairo-Leader-Id|integer||Node ID of the currently acknowledged Raft leader.|
|200|X-Kairo-Term|integer||Current monotonic Raft election term.|
|200|X-Kairo-Conf-Rev|integer||Monotonic configuration revision counter for cluster topology.|
|200|X-Kairo-Active-Peers|string||Comma-separated list of active peer IDs currently reachable by the leader.|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Upsert key or atomic Compare-And-Swap (CAS)

<a id="opIdputKey"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:9001/put \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```http
POST http://127.0.0.1:9001/put HTTP/1.1
Host: 127.0.0.1:9001
Content-Type: application/json
Accept: application/json

```

```javascript
const inputBody = '{
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "expected_version": 0
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/put',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Content-Type' => 'application/json',
  'Accept' => 'application/json'
}

result = RestClient.post 'http://127.0.0.1:9001/put',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('http://127.0.0.1:9001/put', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Content-Type' => 'application/json',
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('POST','http://127.0.0.1:9001/put', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/put");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Content-Type": []string{"application/json"},
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("POST", "http://127.0.0.1:9001/put", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`POST /put`

Applies a write to the distributed state machine via Raft consensus.
If `expected_version` is supplied:
- `0`: Succeeds only if key does not exist (Create If Not Present).
- `> 0`: Succeeds only if current version equals `expected_version`.
Returns HTTP 409 Conflict if current version does not match.

> Body parameter

```json
{
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "expected_version": 0
}
```

<h3 id="upsert-key-or-atomic-compare-and-swap-(cas)-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[PutRequest](#schemaputrequest)|true|none|

> Example responses

> 200 Response

```json
{
  "status": "ok",
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "version": 2,
  "mod_revision": 43
}
```

<h3 id="upsert-key-or-atomic-compare-and-swap-(cas)-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Mutation committed to Raft quorum|[PutResponse](#schemaputresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid JSON or empty key|[ErrorResponse](#schemaerrorresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Version mismatch (CAS conflict)|[ErrorResponse](#schemaerrorresponse)|
|503|[Service Unavailable](https://tools.ietf.org/html/rfc7231#section-6.6.4)|Cluster has no elected leader|[ErrorResponse](#schemaerrorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|X-Kairo-Leader-Id|integer||Node ID of the currently acknowledged Raft leader.|
|200|X-Kairo-Term|integer||Current monotonic Raft election term.|
|200|X-Kairo-Conf-Rev|integer||Monotonic configuration revision counter for cluster topology.|
|200|X-Kairo-Active-Peers|string||Comma-separated list of active peer IDs currently reachable by the leader.|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Delete key

<a id="opIddeleteKey"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:9001/delete \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```http
POST http://127.0.0.1:9001/delete HTTP/1.1
Host: 127.0.0.1:9001
Content-Type: application/json
Accept: application/json

```

```javascript
const inputBody = '{
  "key": "configs/cluster-name"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/delete',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Content-Type' => 'application/json',
  'Accept' => 'application/json'
}

result = RestClient.post 'http://127.0.0.1:9001/delete',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('http://127.0.0.1:9001/delete', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Content-Type' => 'application/json',
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('POST','http://127.0.0.1:9001/delete', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/delete");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Content-Type": []string{"application/json"},
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("POST", "http://127.0.0.1:9001/delete", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`POST /delete`

Removes a key from the state machine via Raft consensus. Returns the deleted record metadata.

> Body parameter

```json
{
  "key": "configs/cluster-name"
}
```

<h3 id="delete-key-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[DeleteRequest](#schemadeleterequest)|true|none|

> Example responses

> 200 Response

```json
{
  "status": "deleted",
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "version": 3,
  "mod_revision": 44
}
```

<h3 id="delete-key-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Key deleted successfully|[DeleteResponse](#schemadeleteresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid JSON or empty key|[ErrorResponse](#schemaerrorresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Key not found|[ErrorResponse](#schemaerrorresponse)|
|503|[Service Unavailable](https://tools.ietf.org/html/rfc7231#section-6.6.4)|Cluster has no elected leader|[ErrorResponse](#schemaerrorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|X-Kairo-Leader-Id|integer||Node ID of the currently acknowledged Raft leader.|
|200|X-Kairo-Term|integer||Current monotonic Raft election term.|
|200|X-Kairo-Conf-Rev|integer||Monotonic configuration revision counter for cluster topology.|
|200|X-Kairo-Active-Peers|string||Comma-separated list of active peer IDs currently reachable by the leader.|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Real-time key modification watch stream (SSE)

<a id="opIdwatchKeys"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:9001/watch?key=configs%2F \
  -H 'Accept: text/event-stream'

```

```http
GET http://127.0.0.1:9001/watch?key=configs%2F HTTP/1.1
Host: 127.0.0.1:9001
Accept: text/event-stream

```

```javascript

const headers = {
  'Accept':'text/event-stream'
};

fetch('http://127.0.0.1:9001/watch?key=configs%2F',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'text/event-stream'
}

result = RestClient.get 'http://127.0.0.1:9001/watch',
  params: {
  'key' => 'string'
}, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'text/event-stream'
}

r = requests.get('http://127.0.0.1:9001/watch', params={
  'key': 'configs/'
}, headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'text/event-stream',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('GET','http://127.0.0.1:9001/watch', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/watch?key=configs%2F");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"text/event-stream"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("GET", "http://127.0.0.1:9001/watch", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`GET /watch`

Subscribes to mutation events (PUT, DELETE) for a key or prefix via Server-Sent Events (SSE).
- **Historical Replay**: Specify `since_rev` to replay missed events from past Raft commits.
- **Single Event Mode**: Set `stream=false` to return the first matching event as regular JSON.
- **Prefix Watching**: Set `prefix=true` to match all keys beginning with the query string.

<h3 id="real-time-key-modification-watch-stream-(sse)-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|key|query|string|true|none|
|prefix|query|boolean|false|none|
|since_rev|query|integer|false|Replay mutations occurring strictly after this global revision.|
|stream|query|boolean|false|If `false`, delivers a single JSON event and terminates connection.|

> Example responses

> 200 Response

> 400 Response

```json
{
  "error": "key not found"
}
```

<h3 id="real-time-key-modification-watch-stream-(sse)-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Continuous Server-Sent Events (SSE) stream|[WatchEvent](#schemawatchevent)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Missing key parameter|[ErrorResponse](#schemaerrorresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|
|410|[Gone](https://tools.ietf.org/html/rfc7231#section-6.5.9)|Requested revision has been compacted and pruned from memory|[ErrorResponse](#schemaerrorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Cluster topology and membership event stream (SSE)

<a id="opIdclusterEvents"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:9001/cluster/events \
  -H 'Accept: text/event-stream'

```

```http
GET http://127.0.0.1:9001/cluster/events HTTP/1.1
Host: 127.0.0.1:9001
Accept: text/event-stream

```

```javascript

const headers = {
  'Accept':'text/event-stream'
};

fetch('http://127.0.0.1:9001/cluster/events',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'text/event-stream'
}

result = RestClient.get 'http://127.0.0.1:9001/cluster/events',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'text/event-stream'
}

r = requests.get('http://127.0.0.1:9001/cluster/events', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'text/event-stream',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('GET','http://127.0.0.1:9001/cluster/events', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/cluster/events");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"text/event-stream"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("GET", "http://127.0.0.1:9001/cluster/events", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`GET /cluster/events`

Real-time SSE stream of cluster state changes (leader elections, node heartbeats, membership updates).
Immediately yields an initial `init` event containing the current cluster snapshot upon connection.

> Example responses

> 200 Response

> 401 Response

```json
{
  "error": "key not found"
}
```

<h3 id="cluster-topology-and-membership-event-stream-(sse)-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Continuous Server-Sent Events stream|[ClusterEvent](#schemaclusterevent)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Trigger manual snapshot and log compaction

<a id="opIdcreateSnapshot"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:9001/snapshot \
  -H 'Accept: application/json'

```

```http
POST http://127.0.0.1:9001/snapshot HTTP/1.1
Host: 127.0.0.1:9001
Accept: application/json

```

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/snapshot',
{
  method: 'POST',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'application/json'
}

result = RestClient.post 'http://127.0.0.1:9001/snapshot',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.post('http://127.0.0.1:9001/snapshot', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('POST','http://127.0.0.1:9001/snapshot', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/snapshot");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("POST", "http://127.0.0.1:9001/snapshot", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`POST /snapshot`

Persists the state machine to disk and compacts preceding Raft log entries.

> Example responses

> 200 Response

```json
{
  "status": "ok"
}
```

<h3 id="trigger-manual-snapshot-and-log-compaction-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Snapshot completed successfully|Inline|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|
|500|[Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1)|Snapshot execution failed|[ErrorResponse](#schemaerrorresponse)|

<h3 id="trigger-manual-snapshot-and-log-compaction-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» status|string|false|none|none|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Cluster membership list

<a id="opIdlistMembers"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:9001/members \
  -H 'Accept: application/json'

```

```http
GET http://127.0.0.1:9001/members HTTP/1.1
Host: 127.0.0.1:9001
Accept: application/json

```

```javascript

const headers = {
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/members',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'application/json'
}

result = RestClient.get 'http://127.0.0.1:9001/members',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'application/json'
}

r = requests.get('http://127.0.0.1:9001/members', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('GET','http://127.0.0.1:9001/members', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/members");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("GET", "http://127.0.0.1:9001/members", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`GET /members`

Returns current active leader, voters, learners, and peer network endpoints.

> Example responses

> 200 Response

```json
{
  "leader_id": 1,
  "term": 4,
  "conf_rev": 3,
  "voters": [
    1,
    2,
    3
  ],
  "learners": [],
  "peers": {
    "1": "127.0.0.1:8001",
    "2": "127.0.0.1:8002",
    "3": "127.0.0.1:8003"
  },
  "peer_health": {
    "2": true,
    "3": true
  }
}
```

<h3 id="cluster-membership-list-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Cluster membership status|[MembersResponse](#schemamembersresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|X-Kairo-Leader-Id|integer||Node ID of the currently acknowledged Raft leader.|
|200|X-Kairo-Term|integer||Current monotonic Raft election term.|
|200|X-Kairo-Conf-Rev|integer||Monotonic configuration revision counter for cluster topology.|
|200|X-Kairo-Active-Peers|string||Comma-separated list of active peer IDs currently reachable by the leader.|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Add learner node to cluster

<a id="opIdaddMember"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:9001/member/add \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```http
POST http://127.0.0.1:9001/member/add HTTP/1.1
Host: 127.0.0.1:9001
Content-Type: application/json
Accept: application/json

```

```javascript
const inputBody = '{
  "id": 4,
  "addr": "127.0.0.1:8004"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/member/add',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Content-Type' => 'application/json',
  'Accept' => 'application/json'
}

result = RestClient.post 'http://127.0.0.1:9001/member/add',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('http://127.0.0.1:9001/member/add', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Content-Type' => 'application/json',
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('POST','http://127.0.0.1:9001/member/add', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/member/add");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Content-Type": []string{"application/json"},
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("POST", "http://127.0.0.1:9001/member/add", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`POST /member/add`

Adds a non-voting learner node to catch up on log replication prior to voter promotion.

> Body parameter

```json
{
  "id": 4,
  "addr": "127.0.0.1:8004"
}
```

<h3 id="add-learner-node-to-cluster-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[MemberAddRequest](#schemamemberaddrequest)|true|none|

> Example responses

> 200 Response

```json
{
  "status": "learner_added",
  "node_id": 4
}
```

<h3 id="add-learner-node-to-cluster-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Learner node successfully added|Inline|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Invalid node ID or address|[ErrorResponse](#schemaerrorresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Another membership change is pending commit or node already exists|[ErrorResponse](#schemaerrorresponse)|

<h3 id="add-learner-node-to-cluster-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» status|string|false|none|none|
|» node_id|integer|false|none|none|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Promote learner to voting member

<a id="opIdpromoteMember"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:9001/member/promote \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```http
POST http://127.0.0.1:9001/member/promote HTTP/1.1
Host: 127.0.0.1:9001
Content-Type: application/json
Accept: application/json

```

```javascript
const inputBody = '{
  "id": 4
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/member/promote',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Content-Type' => 'application/json',
  'Accept' => 'application/json'
}

result = RestClient.post 'http://127.0.0.1:9001/member/promote',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('http://127.0.0.1:9001/member/promote', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Content-Type' => 'application/json',
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('POST','http://127.0.0.1:9001/member/promote', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/member/promote");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Content-Type": []string{"application/json"},
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("POST", "http://127.0.0.1:9001/member/promote", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`POST /member/promote`

Promotes a fully caught-up learner node to a voting Raft quorum member.

> Body parameter

```json
{
  "id": 4
}
```

<h3 id="promote-learner-to-voting-member-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[MemberIDRequest](#schemamemberidrequest)|true|none|

> Example responses

> 200 Response

```json
{
  "status": "promoted",
  "node_id": 4
}
```

<h3 id="promote-learner-to-voting-member-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Node promoted to voter|Inline|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Learner is not caught up with leader log yet|[ErrorResponse](#schemaerrorresponse)|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Learner node not found|[ErrorResponse](#schemaerrorresponse)|

<h3 id="promote-learner-to-voting-member-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» status|string|false|none|none|
|» node_id|integer|false|none|none|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Remove node from cluster

<a id="opIdremoveMember"></a>

> Code samples

```shell
# You can also use wget
curl -X POST http://127.0.0.1:9001/member/remove \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'

```

```http
POST http://127.0.0.1:9001/member/remove HTTP/1.1
Host: 127.0.0.1:9001
Content-Type: application/json
Accept: application/json

```

```javascript
const inputBody = '{
  "id": 4
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('http://127.0.0.1:9001/member/remove',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Content-Type' => 'application/json',
  'Accept' => 'application/json'
}

result = RestClient.post 'http://127.0.0.1:9001/member/remove',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

r = requests.post('http://127.0.0.1:9001/member/remove', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Content-Type' => 'application/json',
    'Accept' => 'application/json',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('POST','http://127.0.0.1:9001/member/remove', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/member/remove");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("POST");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Content-Type": []string{"application/json"},
        "Accept": []string{"application/json"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("POST", "http://127.0.0.1:9001/member/remove", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`POST /member/remove`

Demotes and removes a node from the cluster quorum.

> Body parameter

```json
{
  "id": 4
}
```

<h3 id="remove-node-from-cluster-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[MemberIDRequest](#schemamemberidrequest)|true|none|

> Example responses

> 200 Response

```json
{
  "status": "removed",
  "node_id": 4
}
```

<h3 id="remove-node-from-cluster-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Node removed from cluster|Inline|
|401|[Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)|Missing or invalid HTTP Basic Authentication credentials|[ErrorResponse](#schemaerrorresponse)|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Node not found|[ErrorResponse](#schemaerrorresponse)|

<h3 id="remove-node-from-cluster-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» status|string|false|none|none|
|» node_id|integer|false|none|none|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|401|WWW-Authenticate|string||none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
basicAuth
</aside>

## Interactive API documentation (Scalar)

<a id="opIdgetApiDocs"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:9001/docs \
  -H 'Accept: text/html'

```

```http
GET http://127.0.0.1:9001/docs HTTP/1.1
Host: 127.0.0.1:9001
Accept: text/html

```

```javascript

const headers = {
  'Accept':'text/html'
};

fetch('http://127.0.0.1:9001/docs',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'text/html'
}

result = RestClient.get 'http://127.0.0.1:9001/docs',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'text/html'
}

r = requests.get('http://127.0.0.1:9001/docs', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'text/html',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('GET','http://127.0.0.1:9001/docs', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/docs");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"text/html"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("GET", "http://127.0.0.1:9001/docs", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`GET /docs`

Serves interactive API reference UI rendered with Scalar. Bypasses authentication.

> Example responses

> 200 Response

```
"string"
```

> 404 Response

```json
{
  "error": "key not found"
}
```

<h3 id="interactive-api-documentation-(scalar)-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|HTML API Reference documentation|string|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Documentation page not found|[ErrorResponse](#schemaerrorresponse)|

<aside class="success">
This operation does not require authentication
</aside>

## Raw OpenAPI 3.1 specification

<a id="opIdgetOpenApiSpec"></a>

> Code samples

```shell
# You can also use wget
curl -X GET http://127.0.0.1:9001/openapi.yaml \
  -H 'Accept: text/yaml'

```

```http
GET http://127.0.0.1:9001/openapi.yaml HTTP/1.1
Host: 127.0.0.1:9001
Accept: text/yaml

```

```javascript

const headers = {
  'Accept':'text/yaml'
};

fetch('http://127.0.0.1:9001/openapi.yaml',
{
  method: 'GET',

  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

```ruby
require 'rest-client'
require 'json'

headers = {
  'Accept' => 'text/yaml'
}

result = RestClient.get 'http://127.0.0.1:9001/openapi.yaml',
  params: {
  }, headers: headers

p JSON.parse(result)

```

```python
import requests
headers = {
  'Accept': 'text/yaml'
}

r = requests.get('http://127.0.0.1:9001/openapi.yaml', headers = headers)

print(r.json())

```

```php
<?php

require 'vendor/autoload.php';

$headers = array(
    'Accept' => 'text/yaml',
);

$client = new \GuzzleHttp\Client();

// Define array of request body.
$request_body = array();

try {
    $response = $client->request('GET','http://127.0.0.1:9001/openapi.yaml', array(
        'headers' => $headers,
        'json' => $request_body,
       )
    );
    print_r($response->getBody()->getContents());
 }
 catch (\GuzzleHttp\Exception\BadResponseException $e) {
    // handle exception or api errors.
    print_r($e->getMessage());
 }

 // ...

```

```java
URL obj = new URL("http://127.0.0.1:9001/openapi.yaml");
HttpURLConnection con = (HttpURLConnection) obj.openConnection();
con.setRequestMethod("GET");
int responseCode = con.getResponseCode();
BufferedReader in = new BufferedReader(
    new InputStreamReader(con.getInputStream()));
String inputLine;
StringBuffer response = new StringBuffer();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
System.out.println(response.toString());

```

```go
package main

import (
       "bytes"
       "net/http"
)

func main() {

    headers := map[string][]string{
        "Accept": []string{"text/yaml"},
    }

    data := bytes.NewBuffer([]byte{jsonReq})
    req, err := http.NewRequest("GET", "http://127.0.0.1:9001/openapi.yaml", data)
    req.Header = headers

    client := &http.Client{}
    resp, err := client.Do(req)
    // ...
}

```

`GET /openapi.yaml`

Serves this raw OpenAPI specification in YAML format. Bypasses authentication.

> Example responses

> 200 Response

> 404 Response

```json
{
  "error": "key not found"
}
```

<h3 id="raw-openapi-3.1-specification-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OpenAPI 3.1 YAML document|string|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Specification not found|[ErrorResponse](#schemaerrorresponse)|

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_Item">Item</h2>
<!-- backwards compatibility -->
<a id="schemaitem"></a>
<a id="schema_Item"></a>
<a id="tocSitem"></a>
<a id="tocsitem"></a>

```json
{
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "version": 1,
  "mod_revision": 42
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|key|string|true|none|none|
|value|string|true|none|none|
|version|integer|true|none|Per-key mutation counter. Increments monotonically on every mutation of this specific key.|
|mod_revision|integer|true|none|Global monotonic cluster revision at the time of modification.|

<h2 id="tocS_PutRequest">PutRequest</h2>
<!-- backwards compatibility -->
<a id="schemaputrequest"></a>
<a id="schema_PutRequest"></a>
<a id="tocSputrequest"></a>
<a id="tocsputrequest"></a>

```json
{
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "expected_version": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|key|string|true|none|Unique key identifier.|
|value|string|true|none|Data payload.|
|expected_version|integer|false|none|Optional expected version for atomic Compare-And-Swap (CAS):<br>- `0`: Create key only if it does not already exist.<br>- `> 0`: Mutate key only if current version matches `expected_version`.<br>- `null` / omitted: Unconditional Put (upsert).|

<h2 id="tocS_PutResponse">PutResponse</h2>
<!-- backwards compatibility -->
<a id="schemaputresponse"></a>
<a id="schema_PutResponse"></a>
<a id="tocSputresponse"></a>
<a id="tocsputresponse"></a>

```json
{
  "status": "ok",
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "version": 2,
  "mod_revision": 43
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|status|string|true|none|none|
|key|string|true|none|none|
|value|string|true|none|none|
|version|integer|true|none|none|
|mod_revision|integer|true|none|none|

<h2 id="tocS_DeleteRequest">DeleteRequest</h2>
<!-- backwards compatibility -->
<a id="schemadeleterequest"></a>
<a id="schema_DeleteRequest"></a>
<a id="tocSdeleterequest"></a>
<a id="tocsdeleterequest"></a>

```json
{
  "key": "configs/cluster-name"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|key|string|true|none|none|

<h2 id="tocS_DeleteResponse">DeleteResponse</h2>
<!-- backwards compatibility -->
<a id="schemadeleteresponse"></a>
<a id="schema_DeleteResponse"></a>
<a id="tocSdeleteresponse"></a>
<a id="tocsdeleteresponse"></a>

```json
{
  "status": "deleted",
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "version": 3,
  "mod_revision": 44
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|status|string|true|none|none|
|key|string|true|none|none|
|value|string|true|none|none|
|version|integer|true|none|none|
|mod_revision|integer|true|none|none|

<h2 id="tocS_WatchEvent">WatchEvent</h2>
<!-- backwards compatibility -->
<a id="schemawatchevent"></a>
<a id="schema_WatchEvent"></a>
<a id="tocSwatchevent"></a>
<a id="tocswatchevent"></a>

```json
{
  "type": "PUT",
  "key": "configs/cluster-name",
  "value": "us-east-prod",
  "prev_value": "us-east-staging",
  "version": 2,
  "mod_revision": 43
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|type|string|true|none|none|
|key|string|true|none|none|
|value|string|false|none|none|
|prev_value|string|false|none|none|
|version|integer|true|none|none|
|mod_revision|integer|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|type|PUT|
|type|DELETE|
|type|PROGRESS|
|type|SYNC|
|type|ERROR|

<h2 id="tocS_ClusterEvent">ClusterEvent</h2>
<!-- backwards compatibility -->
<a id="schemaclusterevent"></a>
<a id="schema_ClusterEvent"></a>
<a id="tocSclusterevent"></a>
<a id="tocsclusterevent"></a>

```json
{
  "type": "leader_change",
  "leader_id": 1,
  "term": 4,
  "conf_rev": 3,
  "peers": {
    "1": "127.0.0.1:8001",
    "2": "127.0.0.1:8002",
    "3": "127.0.0.1:8003"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|type|string|true|none|none|
|leader_id|integer|false|none|none|
|term|integer|true|none|none|
|conf_rev|integer|true|none|none|
|peers|object|false|none|none|
|» **additionalProperties**|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|type|init|
|type|leader_change|
|type|peer_state|
|type|conf_change|

<h2 id="tocS_StatusResponse">StatusResponse</h2>
<!-- backwards compatibility -->
<a id="schemastatusresponse"></a>
<a id="schema_StatusResponse"></a>
<a id="tocSstatusresponse"></a>
<a id="tocsstatusresponse"></a>

```json
{
  "node_id": 1,
  "role": "Leader",
  "term": 4,
  "leader_id": 1,
  "conf_rev": 3,
  "peer_health": {
    "2": true,
    "3": true
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|node_id|integer|true|none|none|
|role|string|true|none|none|
|term|integer|true|none|none|
|leader_id|integer|true|none|none|
|conf_rev|integer|true|none|none|
|peer_health|object|true|none|none|
|» **additionalProperties**|boolean|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|role|Follower|
|role|Candidate|
|role|Leader|
|role|Learner|

<h2 id="tocS_MembersResponse">MembersResponse</h2>
<!-- backwards compatibility -->
<a id="schemamembersresponse"></a>
<a id="schema_MembersResponse"></a>
<a id="tocSmembersresponse"></a>
<a id="tocsmembersresponse"></a>

```json
{
  "leader_id": 1,
  "term": 4,
  "conf_rev": 3,
  "voters": [
    1,
    2,
    3
  ],
  "learners": [],
  "peers": {
    "1": "127.0.0.1:8001",
    "2": "127.0.0.1:8002",
    "3": "127.0.0.1:8003"
  },
  "peer_health": {
    "2": true,
    "3": true
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|leader_id|integer|true|none|none|
|term|integer|true|none|none|
|conf_rev|integer|true|none|none|
|voters|[integer]|true|none|none|
|learners|[integer]|true|none|none|
|peers|object|true|none|none|
|» **additionalProperties**|string|false|none|none|
|peer_health|object|true|none|none|
|» **additionalProperties**|boolean|false|none|none|

<h2 id="tocS_MemberAddRequest">MemberAddRequest</h2>
<!-- backwards compatibility -->
<a id="schemamemberaddrequest"></a>
<a id="schema_MemberAddRequest"></a>
<a id="tocSmemberaddrequest"></a>
<a id="tocsmemberaddrequest"></a>

```json
{
  "id": 4,
  "addr": "127.0.0.1:8004"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer|true|none|Unique positive node ID.|
|addr|string|true|none|Internal Raft transport address (host:port).|

<h2 id="tocS_MemberIDRequest">MemberIDRequest</h2>
<!-- backwards compatibility -->
<a id="schemamemberidrequest"></a>
<a id="schema_MemberIDRequest"></a>
<a id="tocSmemberidrequest"></a>
<a id="tocsmemberidrequest"></a>

```json
{
  "id": 4
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer|true|none|none|

<h2 id="tocS_ErrorResponse">ErrorResponse</h2>
<!-- backwards compatibility -->
<a id="schemaerrorresponse"></a>
<a id="schema_ErrorResponse"></a>
<a id="tocSerrorresponse"></a>
<a id="tocserrorresponse"></a>

```json
{
  "error": "key not found"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|error|string|true|none|none|

