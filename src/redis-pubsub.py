import redis
import time
#import argparse

#import apache_beam as beam
#from apache_beam.options.pipeline_options import PipelineOptions
#from apache_beam.options.pipeline_options import GoogleCloudOptions
#from apache_beam.options.pipeline_options import StandardOptions

from google.cloud import pubsub_v1



def fetch_data_from_redis(r):
    """Fetch record for filebeat key from redis."""
    return r.lindex('filebeat',0)

def publish_messages(project, topic_name,r):
    """Publishes messages to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project,topic_name)

    data = fetch_data_from_redis(r)
    data = data.encode('utf-8')
    publisher.publish(topic_path, data=data)

    print('Published message')

def receive_messages(project, subscription_name):
    """Receives messages from a pull subscription."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        print('Recieved message: {}'.format(message))
        message.ack()

    subscriber.subscribe(subscription_path,callback=callback)

    print('Listening for messages on {}'.format(subscription_path))
    while True:
        time.sleep(60)
       

"""Data Streaming with PubSub not supported in Python SDK. Use Java SDK"""

"""def read_from_pubsub(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic', dest='input_topic', required=True,
        help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
    parser.add_argument(
        '--output_topic', dest='output_topic', required=True,
        help='Output PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    with beam.Pipeline(argv=pipeline_args) as p:
        lines = p | beam.io.ReadStringsFromPubSub(known_args.input_topic)
        # Capitalize the characters in each line.
        transformed = (lines
                    | 'capitalize' >> (beam.Map(lambda x: x.upper())))

        # Write to PubSub.
        # pylint: disable=expression-not-assigned
        transformed | beam.io.WriteStringsToPubSub(known_args.output_topic)"""

if __name__ == '__main__':
    project = 'cloudcover-sandbox'
    topic_name = 'redis-pubsub'
    subscription_name = 'redis-pubsub'
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    publish_messages(project, topic_name,r)
    receive_messages(project,subscription_name)
    