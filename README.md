# Migration tool generaterd from JSON Schema file

## Install

```sh:

go get -u github.com/meta-closure/migo/cmd/migo

```

## Usage
Read a (extended) JSON Schema file and migrate database table status.

```sh:
migo -y /path/to/schema.yml -s /path/to/internal.yml run
```

Before migrate, you might check a operation what migo do,
Plan command enable to see operation list

```sh;
migo -y /path/to/schema.yml -s /path/to/internal.yml plan
```

## Sample Schema Description

### Database configure Sample

Read db tag and create DSN(Data Source Name) to connect your database.

```yaml:
db:
    user: sample user
    passwd: passwd
    addr: host:port
    dbname: db

```

### Table Configure Sample

Read table tag and modify a table setting, set a primary key or a index key.
migo identify each table and column in their hash table key,
so if you want to change a table name, you just overide name.

primary key and index only specify key name.


```yaml:
key:
    name: sample
    primary_key:
        - pk1
        - pk2
    index:
        - index1
        - index2
```

### Column Configure Sample

Read column tag and modify a column setting, set foreign key.
If you want to use foreign key in using migo, you should set the foreign key name,
to modify foreign key setting.

```yaml:

column:
    not_null: true
    name: column_sample
    foreign_key:
        name: fk_identifier
        target_table: fk_table
        target_column: fk_column
        
```
## Fail Revocery

If migo failed a migrate operation, such that type converting error or irregular operation,
then he attempt to recover before database states, quering reverse operation what migo done.    

## Rollback

If you might to Rollback. you just run specify a previous JSON Schema file or State file.