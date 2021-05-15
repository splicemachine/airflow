from os import environ as env_vars

spark_defaults = {
    "executor_memory": "4g",
    "executor_cores": 5,
    "jars": "https://splice-releases.s3.amazonaws.com/3.1.0.2009/cluster/nsds/splice_spark2-3.1.0.2009-shaded-dbaas3.0.jar"
}
