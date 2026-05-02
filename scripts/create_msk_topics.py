#!/opt/kafka-venv/bin/python
"""Create MSK Kafka topics using IAM auth. Idempotent (TopicAlreadyExistsError ignored)."""
import os, sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider(AbstractTokenProvider):
    def __init__(self, region, **kwargs):
        super().__init__(**kwargs)
        self.region = region
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token

REGION = os.environ.get("AWS_REGION", "us-east-1")
BOOTSTRAP = os.environ.get(
    "MSK_BOOTSTRAP",
    "b-1.saiflinkkafka.uzsagf.c4.kafka.us-east-1.amazonaws.com:9098,"
    "b-2.saiflinkkafka.uzsagf.c4.kafka.us-east-1.amazonaws.com:9098,"
    "b-3.saiflinkkafka.uzsagf.c4.kafka.us-east-1.amazonaws.com:9098",
)

admin = KafkaAdminClient(
    bootstrap_servers=BOOTSTRAP.split(","),
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
    sasl_oauth_token_provider=MSKTokenProvider(REGION),
    api_version=(3, 6, 0),
    client_id="topic-creator",
)

# Names from CLI args, default to standard set
default_topics = ["orders_topic", "orders_cdc_topic", "orders_softdel_topic"]
topic_names = sys.argv[1:] or default_topics
new_topics = [NewTopic(name=t, num_partitions=3, replication_factor=3) for t in topic_names]

for nt in new_topics:
    try:
        admin.create_topics([nt])
        print(f"created: {nt.name}")
    except TopicAlreadyExistsError:
        print(f"exists:  {nt.name}")
    except Exception as e:
        print(f"ERROR    {nt.name}: {type(e).__name__}: {e}")

print("All topics:", sorted(admin.list_topics()))
admin.close()
