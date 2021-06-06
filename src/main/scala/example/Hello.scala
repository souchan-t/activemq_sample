package example

import java.net.URI
import java.io.File

import javax.jms.ConnectionFactory
import javax.jms.Session
import javax.jms.Queue
import javax.jms.TextMessage
import javax.jms.QueueConnection
import javax.jms.QueueSender
import javax.jms.QueueSession
import javax.jms.Message
import javax.jms.MessageListener

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerFactory
import org.apache.activemq.broker.BrokerService
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter

@main
def main():Unit =

  val brokerName = "test_broker"
  val brokerUri = URI("vm:/test_broker")
  val queueName = "test_queue"

  val broker = MyBroker(URI("tcp://localhost:61616"),brokerName)
  broker.start()
  
  val consumer1 = MyConsumer(brokerUri,queueName){
    (message:Message) =>
      println(s"[C1 Receive Message]:${message.asInstanceOf[TextMessage].getText}")
  }
  val consumer2 = MyConsumer(brokerUri,queueName){
    (message:Message) =>
      println(s"[C2 Receive Message]:${message.asInstanceOf[TextMessage].getText}")
  }

  val producer1 = MyProducer(brokerUri,queueName)
  

  var command:String = ""
  while (command != "exit")
    command = io.StdIn.readLine().strip()
    command match
      case mes => producer1.send(mes)

  consumer1.close()
  consumer2.close()
  producer1.close()
  broker.stop()

class MyBroker(val uri:URI,val name:String):
  val broker = new BrokerService()
  broker.addConnector(uri)
  broker.setBrokerName(name)
  broker.setPersistenceAdapter(new KahaDBPersistenceAdapter())
  broker.setPersistent(true)

  def start():Unit = broker.start()
  def stop():Unit = broker.stop()

class MyProducer(uri:URI,queueName:String):
  val conn = ActiveMQConnectionFactory(uri).createQueueConnection()
  val session = conn.createQueueSession(false,Session.AUTO_ACKNOWLEDGE)
  val queue = session.createQueue(queueName)
  val sender = session.createSender(queue)
  conn.start()

  def send(message:String):Unit = sender.send(session.createTextMessage(message))

  def close():Unit=
    sender.close()
    session.close()
    conn.close()

class MyConsumer(uri:URI,queueName:String)(body:MessageListener):
  val conn = ActiveMQConnectionFactory(uri).createQueueConnection()
  val session = conn.createQueueSession(false,Session.AUTO_ACKNOWLEDGE)
  val queue = session.createQueue(queueName)
  val receiver = session.createReceiver(queue)
  conn.start()

  receiver.setMessageListener(body)

  def close():Unit =
    receiver.close()
    session.close()
    conn.close()

