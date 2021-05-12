from os import environ as env_vars

spark_defaults = {
    "executor_memory": "4G",
    "executor_cores": 5,
    "jars": "https://splice-releases.s3.amazonaws.com/3.1.0.2009/cluster/nsds/splice_spark2-3.1.0.2009-shaded-dbaas3.0.jar",
    "num_executors": 2,
    "conf": {
        "spark.submit.deployMode": "client",
        "spark.dynamicAllocation.enabled": False,
        "spark.kubernetes.container.image": env_vars['SPARK_DOCKER_IMAGE'],
        "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
        "spark.executorEnv.RELEASE_NAME": env_vars['SPLICE_REL_NAME'],
        "spark.kubernetes.driver.label.release": env_vars['SPLICE_REL_NAME'],
        "spark.kubernetes.executor.label.component": "sparkexec",
        "spark.kubernetes.namespace": env_vars['SPLICE_NAMESPACE'],
        "spark.executorEnv.TZ": env_vars['TZ'],
        "spark.blacklist.enabled": False,
        "spark.jars.packages": "org.apache.hadoop:hadoop-common:2.9.0,org.apache.hadoop:hadoop-hdfs:2.9.0"
    }
}
