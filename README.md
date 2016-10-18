# Migration tool generaterd from JSON Schema file

## Install

```sh:

go get -u github.com/meta-closure/migo/cmd/migo

```

## Usage
Read a (extended) JSON Schema file and migrate database table status.

```sh:
migo -y /path/to/schema.yml -s /path/to/internal.yml -d /path/to/dbconig.yml -e environment run
```

Before migrate, you might check a operation what migo do,
Plan command enable to see operation list

```sh;
migo -y /path/to/schema.yml -s /path/to/internal.yml -d /path/to/dbconig.yml -e environment plan
```

## Sample Schema Description

### Database configure Sample

If specify some database config file, such that 

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

then migo read configure that you specify environment. if empty then migo read
"default" environment.
Read db tag and create DSN(Data Source Name) to connect your database.

### Table Configure Sample

Read table tag and modify a table setting, set a primary key or a index key.
migo identify each table and column in their hash table key,
so if you want to change a table name, you just overide name.

primary key and index only specify key name.


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

### Column Configure Sample

Read column tag and modify a column setting, set foreign key.
If you want to use foreign key in using migo, you should set the foreign key name,
to modify foreign key setting.
if this table definitions is in definitions or properties, then target_table specification is
JSON Referencens, in links, then it have extra format, "/link" + Href + (schema|target_schema) 

```yaml:

column:
    not_null: true
    name: column_sample
    foreign_key:
        name: fk_identifier
        target_table: /link/base/schema
        target_column: fk_column
        
```

## How to Init to use migo 

migo have init command that create initial state file and 
create database if not exist,

```sh:
migo -d database.yml -e develop init
```

## Seeding

migo have seed command that enable you to insert initial seed record.
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

then migo convert all data into SQL and insert to DB.

## Fail Revocery

If migo failed a migrate operation, such that type converting error or irregular operation,
then he attempt to recover before database states, quering reverse operation what migo done.    

## Rollback

If you might to Rollback. you just run specify a previous JSON Schema file or State file.

## Test

```
go test -v .
```

or

```
./bin/test
```
