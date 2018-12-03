# tic

### compile code
```
sbt package
```

### process data
```
python3 src/main/python/run.py <spark host> <sbc cache dir> --mapping_input_file <mapping file> --data_input_file <data file> --output_dir <output dir>
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
csvsql --db "postgresql://<uid>:<pwd>@<host>/<db>" --insert --overwrite -p \\ -e utf8 tables/*
```
