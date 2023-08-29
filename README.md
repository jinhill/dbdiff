dbdiff v1.0.0
Compare tables row by row and output the differences between them, only supports MySQL.
Usage: dbdiff [-cfhstv] [--host1 DB_HOST] [--host2 DB_HOST] [--user DB_USER] [--password DB_PASSWORD] [--table DB1.TABLE1,DB2.TABLE2...] [--conf CONF_FILE] [--log LOG_DIR]
                -c      compare tables
                -h      print help
                -f      force overwrite of table records (There may be a risk of data loss)
                -s      sync table records based on differences
                -t      two-way sync
                -v      print version
