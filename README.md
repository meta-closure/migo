# Migration tool generaterd from JSON Schema file

## Usage
Read a (extended) JSON Schema file and migrate database table status.
```sh:
mig -y /path/to/schema.yml -i /path/to/internal.yml
```

## Sample Schema Description

### Database configure Sample

Read db tag and create DSN(Data Source Name) to connect Database.

```yaml:
db:
    user: sample user
    passwd: passwd
    addr: host:port
    dbname: db

```

### Table Configure Sample

Read table tag and Modify Table setting, set Primary Key or Index Key.

```yaml:
table:
    name: sample
    before_name: use_for_rename_table
    primary_key:
        - pk1
        - pk2
    index:
        - index1
        - index2
```

### Column Configure Sample

Read column tag and Modify Column setting, set Foreing Key.
If you se foreign key using mig, Should set foreign key name
to modify key setting.

```yaml:

column:
    before_name: use_for_rename_table
    not_null: true
    name: column_sample
    foreign_key:
        name: fk_identifier
        target_table: fk_table
        target_column: fk_column
        
```