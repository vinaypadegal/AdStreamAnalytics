from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
admin_client.delete_topics(admin_client.list_topics())