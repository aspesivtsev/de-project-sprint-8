# security config for Kafka
kafka_security_settings = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
}

# connection settings for PostgreSQL local DB in docker
postgresql_settings_docker = {
    'user': 'jovyan',
    'password': 'jovyan',
    'url': f'jdbc:postgresql://localhost:5432/postgres',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.create_subscribers_feedback',
}

# connection settings for PostgreSQL in Cloud
postgresql_settings_cloud = {
    'user': 'student',
    'password': 'de-student',
    'url': f'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'subscribers_restaurants',
}

# libraries for integrations between Spark, Kafka and PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

topic_in = 'student.topic.cohort20.tolique7_in'
topic_out = 'student.topic.cohort20.tolique7_out'
