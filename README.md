# Data Directory and Filepath Lookup

Service to provide consistent file numbering and naming for unrelated data
acquisition applications.

## Running Locally

The service is written in rust and requires the default toolchain and recent
version of the compiler. This is available from [rustup.rs][_rustup].

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
endpoints (see [the graphiql][_graphiql] front-end) but there are no beamlines
configured.

## Configuring Beamlines

### Querying current configuration

The current configuration can be shown via the `info` subcommand.

```
$ cargo run info
```

### Adding a new beamline

```
$ cargo run config beamline --new
```

[_graphiql]:localhost:8000/graphiql
