# pipeline

### compile code
```
sbt assembly
```

### process data

```
spark-submit --driver-memory=2g --executor-memory=2g --master <spark host> --class tic.Transform2 <sbt assembly output> --mapping_input_file <mapping file> --data_input_file <data file> --data_dictionary_input_file <data dictionary file> --output_dir <output dir> [--redcap_application_token <token>]
```

### install csvkit

```
pip install csvkit
pip install psycopg2-binary
```

### create db

```
create user <uid> with password '<pwd>';
create database "<db>";
grant all on database "<db>" to <uid>;
```

### populate db
In output dir, execute

```
csvsql --db "postgresql://<uid>:<pwd>@<host>/<db>" --insert --no-create -p \\ -e utf8 --date-format "%y-%M-%d" tables/*
```


