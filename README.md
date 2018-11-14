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
pip install pymysql

### populate db
```
in output dir,

```
csvsql --db "mysql+pymysql://<uid>:<pwd>@<host>/<db>" --insert tables/*
```
