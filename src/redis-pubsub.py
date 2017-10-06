import redis
import time

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



if __name__ == '__main__':
    project = 'cloudcover-sandbox'
    topic_name = 'redis-pubsub'
    subscription_name = 'redis-pubsub'
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    publish_messages(project, topic_name,r)
    receive_messages(project,subscription_name)