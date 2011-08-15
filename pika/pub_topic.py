import pika
import time
from pika.adapters import SelectConnection

pika.log.setup(color=True)

channel = None
connection = None

def create_conn():
    credentials = pika.PlainCredentials('guest', 'guest')
    params = pika.ConnectionParameters(credentials=credentials,
                                       host='127.0.0.1',
                                       virtual_host='/')
    return SelectConnection(params, on_connected)

def on_connected(connection):
    pika.log.info("pub_topic: Connected to RabbitMQ")
    connection.channel(on_channel_open)

def on_channel_open(_channel):
    pika.log.info("pub_topic: Received our Channel")

    global channel
    channel = _channel
    channel.exchange_declare(exchange='test.topic', type='topic')
    channel.queue_declare(queue="test_topic", durable=True,
                          exclusive=False, auto_delete=False,
                          callback=on_queue_declared)

def on_queue_declared(frame):
    pika.log.info("pub_topic: Queue Declared")
    channel.queue_bind(exchange='test.topic',
                        queue='test_topic',
                        routing_key='test.topic.*',
                        callback=on_queue_bind)

def on_queue_bind(_args):
    pika.log.info("pub_topic: Queue binded")
    for x in xrange(0, 10):
        message = "Hello World #%i: %.8f" % (x, time.time())
        topic = 'a' + str(x)
        pika.log.info("Sending: %s to topic %s" % (message, topic))
        # set delivery_mode to 2 will make the message to persistent
        channel.basic_publish(exchange='test.topic',
                              routing_key="test.topic."+topic,
                              body=message,
                              properties=pika.BasicProperties(
                              content_type="text/plain",
                              delivery_mode=1))
    # Close our connection
    connection.close()

def main():
    global connection
    connection = create_conn()
    connection.ioloop.start()

if __name__ == '__main__':
    main()
