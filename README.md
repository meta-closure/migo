
## Install

```sh:

go get -u github.com/meta-closure/migo/cmd/migo

```

## Usage
Easy to migrate.

```sh:
migo -y /path/to/schema.yml -s /path/to/internal.yml -d /path/to/dbconig.yml -e environment run
```

You can see the details of what migo does.

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

### Table Configuration Sample

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

```sh:
migo -d database.yml -e develop init
```

## Seeding

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

## Test

Need MySQL docker container.

```
./bin/test
```
