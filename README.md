# Data Directory and Filepath Lookup

Service to provide consistent file numbering and naming for unrelated data
acquisition applications.

## Running Locally

The service is written in rust and requires the default toolchain and recent
version of the compiler (1.81+). This is available from [rustup.rs][_rustup].

[_rustup]:https://rustup.rs

1. Clone this repository

    ```
    $ git clone git@github.com:DiamondLightSource/data-endpoint-lookup.git
    Cloning into 'data-endpoint-lookup'...
    ...
    $ cd data-endpoint-lookup
    ```

2. Build the project

    ```
    $ cargo build
    Compiling numtracker v0.1.0 (./path/to/data-endpoint-lookup)
    ...
    Finished 'dev' profile [unoptimized + debuginfo] target(s) in 11.56s
    ```
3. Run the service

    ```
    $ cargo run serve
    2024-11-04T11:29:05.887214Z  INFO connect{filename="numtracker.db"}: numtracker::db_service: Connecting to SQLite DB
    ```

At this point the service is running and can be queried via the graphQL
endpoints (see [the graphiql][_graphiql] front-end available at
`localhost:8000/graphiql` by default) but there are no beamlines configured.

Additional logging output is available via `-v` verbose flags.

|Flags   |Level|
|--------|-----|
| `-q`   |None |
|        |Error|
| `-v`   |Info |
| `-vv`  |Debug|
| `-vvv` |Trace|

## Schema

The schema is available via the `schema` command. This is also available via the
graphiql interface.
```bash
cargo run schema
```

## Queries

<details>
<summary>Testing queries from terminal</summary>

While the graphiql front-end can be useful for exploring the API schema, running
from the terminal is sometimes quicker/easier. This only requires `curl`
although <a href="https://jqlang.github.io/jq/">jq</a> can make it
easier to parse output.

The query to run should be made as a POST request to `/graphql` wrapped in a
JSON object as `{"query": "<query-string>"}` taking care to escape quotes as
required. Using `curl` and a basic visit directory query (see below), this
looks something like
```bash
echo '{                                                                                        17:15:56
     "query": "{
         paths(beamline: \"i22\", visit: \"cm37278-5\") {
             directory
         }
     }"
 }'| curl -s -X POST 127.0.0.1:8000/graphql -H "Content-Type: application/json" -d @- | jq
```

</details>

### Queries (read-only)
There are two read only queries, one to get the visit directory for a given
visit and beamline and one to get the current configuration for a given
beamline.

#### paths
Get the visit directory for a beamline and visit

##### Query
```graphql
{
  paths(beamline: "i22", visit: "cm12345-6") {
    directory
    visit
  }
}
```
##### Response
```json
{
  "paths": {
    "directory": "/data/i22/data/2024/cm37278-5",
    "visit": "cm37278-5"
  }
}
```

#### configurations
Get the current configuration values for the given beamline

##### Query
```graphql
{
  configurations(beamlineFilter: "i22") {
    visitTemplate
    scanTemplate
    detectorTemplate
    latestScanNumber
  }
}
```

##### Response
```json
{
  "configurations": [
    {
      "visitTemplate": "/tmp/{instrument}/data/{year}/{visit}",
      "scanTemplate": "{subdirectory}/{instrument}-{scan_number}",
      "detectorTemplate": "{subdirectory}/{instrument}-{scan_number}-{detector}",
      "latestScanNumber": 12345
    }
  ]
}
```

## Mutations (read-write)

#### scan

##### Query

```graphql
mutation {
  scan(beamline: "i22", visit: "cm12345-2", subdirectory: "sub/tree") {
      scanFile
      scanNumber
      detectors(names: ["det1", "det2"] ) {
          name
          path
      }
  }
}
```

##### Response
```json
{
  "scan": {
    "scanFile": "sub/tree/i22-20840",
    "scanNumber": 20840,
    "detectors": [
      {
        "name": "det1",
        "path": "sub/tree/i22-20840-det1"
      },
      {
        "name": "det2",
        "path": "sub/tree/i22-20840-det2"
      }
    ]
  }
}
```

#### configure
##### Query
```graphql
mutation {
  configure(beamline: "i11", config: {
      visit:"/tmp/{instrument}/data/{year}/{visit}"
      scan:"{subdirectory}/{instrument}-{scan_number}"
      detector:"{subdirectory}/{instrument}-{scan_number}-{detector}"
      scanNumber: 12345
    }) {
      visitTemplate
      scanTemplate
      detectorTemplate
      latestScanNumber
    }
  }
}
```
##### Response
```json
{
  "configure": {
    "visitTemplate": "/tmp/{instrument}/data/{year}/{visit}",
    "scanTemplate": "{subdirectory}/{instrument}-{scan_number}",
    "detectorTemplate": "{subdirectory}/{instrument}-{scan_number}-{detector}",
    "latestScanNumber": 12345
  }
}
```

[_graphiql]:https://github.com/graphql/graphiql/
[_jq]:https://jqlang.github.io/jq/
