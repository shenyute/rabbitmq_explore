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
    pika.log.info("sub_topic: Connected to RabbitMQ")
    connection.channel(on_channel_open)

def on_channel_open(_channel):
    pika.log.info("sub_topic: Channel opened")

    global channel
    channel = _channel
    channel.basic_consume(on_message_come,
            no_ack=True,
            queue='test_topic')

def on_message_come(channel, method_frame, header_frame, body):
    pika.log.info("sub_topic: from %s new message come --> %s" %
            (method_frame.routing_key, body))
    # if no_ack set to False, you need to call basic_ack to notify server
    # channel.basic_ack(delivery_tag=method_frame.delivery_tag)

def main():
    global connection
    connection = create_conn()
    connection.ioloop.start()

if __name__ == '__main__':
    main()
