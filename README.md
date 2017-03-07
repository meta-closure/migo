# Migration tool By JSON Schema

## Install

```sh:

go get -u github.com/meta-closure/migo/cmd/migo

```

## Usage
Read the (extended) JSON Schema file and migrate database.

```sh:
migo -y /path/to/schema.yml -s /path/to/internal.yml -d /path/to/dbconig.yml -e environment run
```

With the Plan command you can see the plan what migo will do

```sh;
migo -y /path/to/schema.yml -s /path/to/internal.yml -d /path/to/dbconig.yml -e environment plan
```

## Sample Schema Description

### Database configure Sample

```yaml:
default:
    user: sample user
    passwd: passwd
    addr: host:port
    dbname: db

production:
    user: hoge
    passwd: hoge
    addr: host:port
    dbname: db

master:
    user: foo
    passwd: foo
    addr: host:port
    dbname: db

```
migo can read the setting from the specified environment, by default
it will be "default" environment
Make DSN(Data Source Name) from database setting to connect your database.

### Table Configuration Sample

Primary Key and Index need to specify column key name.


```yaml:
key:
    name: sample
    primary_key:
        pk_name:
            - pk1
            - pk2
    index:
        index_name:
            - index1
            - index2
```

### Column Configuration Sample

```yaml:

column:
    not_null: true
    name: column_sample
    foreign_key:
        name: fk_id(should be unique)
        target_table: #/definition/table/path
        target_column: column_key

```

migo support some flags.

- auto_increment(bool)
- auto_update(bool, for datetime or timestamp type column)
- not_null(bool)
- unique(bool)
- default

## How to Setup to use migo

migo have init command, it create initial state file and
create database if not exist.

```sh:
migo -d database.yml -e develop init
```

## Seeding

migo have seed command, it enable you to insert initial record.
for sample description.

```yaml:

# table
foo_table:
      # column
    - bar: 1
      hoge: hoge
    - other: 111
      hoge: hogehoge

# other table
bar:
    - hoge: hogehoge

```

## Recovery

If migo fails migration, such that type converting error or irregular operation,
then migo attempt to recover before database states, execute reverse operation what migo done.

## Test

Need MySQL docker container.

```
./bin/test
```
