# JdbcPostgres - SIARD Postgres JDBC Wrapper

This package contains the JDBC Wrapper for Postgres DBMS for SIARD.

## Prerequisites
For building the binaries, Java JDK (1.8 or higher) must be installed.
In order to run the tests, you have to start a PostgreSQL DB first:

### Start PostgreSQL DB
```shell
docker compose up -d
```

### Build the project
```shell
./gradlew clean build
```

### Create a release
This creates a new tag and pushes the tag to main branch.
```shell
./gradlew release
```

## Documentation
- [User's Manual](./doc/manual/user/index.html)
- [Developer's Manual](./doc/manual/user/index.html) 

## Declaration
Contributions to the codebase have been made with the support of Codeium. Codeium is AI-powered code completion tool, that is trained exclusively on natural language and source code data with [permissive licenses](https://codeium.com/blog/copilot-trains-on-gpl-codeium-does-not ). 

