# Submitting to EMR and Databricks

## AWS EMR

### Prerequisites
- AWS CLI configured
- EMR cluster running with Spark
- S3 bucket for scripts and data

### Submit job
```bash
aws emr add-steps \
  --cluster-id j-XXXXXXXXX \
  --steps Type=Spark,Name="Read to MySQL",ActionOnFailure=CONTINUE,Args=[s3://your-bucket/a_read_to_mysql.py]
```

Or using spark-submit on cluster:
```bash
ssh -i your-key.pem ec2-user@<cluster-master-dns>
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages mysql:mysql-connector-java:8.0.33 \
  s3://your-bucket/a_read_to_mysql.py
```

### For MySQL in private VPC
Update the script to use internal MySQL hostname or RDS endpoint:
```python
mysql_host = "your-rds-endpoint.rds.amazonaws.com"
```

---

## Databricks

### Prerequisites
- Databricks workspace
- Databricks CLI configured
- Compute cluster running

### Upload scripts
```bash
databricks workspace import_directory ./pipelines /Users/me/pipelines --overwrite
databricks fs cp pipelines/data/sample.csv dbfs:/pipelines/data/sample.csv
```

### Submit via UI
1. Create a job with notebook or JAR task
2. Reference the uploaded script: `/Users/me/pipelines/a_read_to_mysql.py`
3. Set cluster and run

### Submit via CLI
```bash
databricks jobs create --json '{
  "name": "spark-read-to-mysql",
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2
  },
  "spark_python_task": {
    "python_file": "dbfs:/pipelines/a_read_to_mysql.py"
  }
}'
```

### For Databricks SQL Warehouse / External MySQL
Databricks has built-in connectors. Update script:
```python
# Use Databricks SQL connector
from databricks import sql
```

Or configure secrets:
```python
mysql_password = dbutils.secrets.get(scope="spark-lab", key="mysql_password")
```

---

## Environment Variables

For portability, the example scripts already use environment variables:
```python
mysql_host = os.getenv("MYSQL_HOST", "mysql")
mysql_user = os.getenv("MYSQL_USER", "spark")
mysql_password = os.getenv("MYSQL_PASSWORD", "sparkpass")
```

Set these in:
- **EMR**: Job step parameters or cluster configuration
- **Databricks**: Cluster environment variables or job parameters
- **Local**: Via `podman compose` or shell exports

---

## Key Differences

| Feature | Local | EMR | Databricks |
|---------|-------|-----|-----------|
| Master URL | `spark://spark-master:7077` | `yarn` | Auto-managed |
| File paths | Local/mounted | S3 | DBFS |
| Packages | `spark-defaults.conf` | `--packages` flag | Workspace libraries |
| Secrets | Env vars | AWS Secrets Manager | Databricks Secrets |
