 ## Sumary
 - Building an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for sparkify analytics team to continue finding insights into what songs their users are listening to.


## How to run the Python scripts
```
    cd /path/to/create_tables.py
    python3 create_tables.py
    python3 etl.py

```

## Files in the repository
- dwh.cfg: config infor for REDSHIFT, IAM ROLE, S3 data bucket.
- create_tables.py: create fact and dimension tables for the star schema in Redshift.
- etl.ipynb: load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
- sql_queries.py: define SQL statements, which will be imported into the two other files above.
