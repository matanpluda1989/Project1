import pika

#Deitals which will sent to queue - sqlite path, country, year
#path = "C:/Program Files/sqlite/db/chinook.db"
path = "C:/Users/matanp/Desktop/chinook.db"
country = "USA"
year = "2012"

msg = path + "*" + country + "&" + year


#Establish connection to queue of RabbitMQ. Deafult host of RabbitMQ mangement - http://localhost:15672 (U+P: guest)
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
queueName = 'msg1'
channel.queue_declare(queue=queueName)

#Sending message - msg to queue - queueName
channel.basic_publish(exchange='',
                      routing_key=queueName,
                      body=msg)

connection.close()