package com.umesh.receiver;

import com.ibm.jms.JMSMessage;
import com.ibm.mq.jms.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.ByteArrayOutputStream;
import java.util.Enumeration;

/**
 * Created by uchaudh on 12/31/2015.
 */
public class CustomWMQReceiver extends Receiver<String> {

    String host = null;
    int port = -1;
    String qm=null;
    String qn=null;
    String channel=null;
    transient MQQueueConnection qCon= null;
    Enumeration enumeration =null;

    public CustomWMQReceiver(String host , int port, String qm, String channel, String qn) {
        super(StorageLevel.MEMORY_ONLY_2());
        this.host = host;
        this.port = port;
        this.qm=qm;
        this.qn=qn;
        this.channel=channel;

    }

    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread()  {
            @Override public void run() {
                try {
                    initConnection();
                    receive();
                }
                catch (JMSException ex)
                {
                    ex.printStackTrace();
                }
            }
        }.start();
    }

    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself isStopped() returns false
    }

    /** Create a MQ connection and receive data until receiver is stopped */
    private void receive() {
        System.out.print("Started receiving messages from MQ");

        try {

            JMSMessage receivedMessage= null;

            while (!isStopped() && enumeration.hasMoreElements() )
            {

                receivedMessage= (JMSMessage) enumeration.nextElement();
                String userInput = convertStreamToString(receivedMessage);
                //System.out.println("Received data :'" + userInput + "'");
                store(userInput);
            }

            // Restart in an attempt to connect again when server is active again
            //restart("Trying to connect again");

            stop("No More Messages To read !");
            qCon.close();
            System.out.println("Queue Connection is Closed");

        }
        catch(Exception e)
        {
            e.printStackTrace();
            restart("Trying to connect again");
        }
        catch(Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }



    }

    public void initConnection() throws JMSException
    {
        System.out.println("Initiating Queue Connection!");
        MQQueueConnectionFactory conFactory= new MQQueueConnectionFactory();
        conFactory.setHostName(host);
        conFactory.setPort(port);
        conFactory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
        conFactory.setQueueManager(qm);
        conFactory.setChannel(channel);

        qCon= (MQQueueConnection) conFactory.createQueueConnection();
        MQQueueSession qSession=(MQQueueSession) qCon.createQueueSession(false, 1);
        MQQueue queue=(MQQueue) qSession.createQueue(qn);
        MQQueueBrowser browser = (MQQueueBrowser) qSession.createBrowser(queue);
        qCon.start();
        enumeration= browser.getEnumeration();
        System.out.println("Queue Connection Established successfully!");

    }

    @Override
    public StorageLevel storageLevel() {
        return StorageLevel.MEMORY_ONLY_2();
    }

    /**
     * Convert stream to string
     *
     * @param jmsMsg
     * @return
     * @throws Exception
     */
    private static String convertStreamToString(final Message jmsMsg) throws Exception {
        String stringMessage = "";
        BytesMessage bMsg= (BytesMessage) jmsMsg;
        byte[] buffer = new byte[40620];
        int byteRead;
        ByteArrayOutputStream bout = new java.io.ByteArrayOutputStream();
        while ((byteRead = bMsg.readBytes(buffer)) != -1) {
            bout.write(buffer, 0, byteRead);
        }
        bout.flush();
        stringMessage = new String(bout.toByteArray());
        bout.close();
        return stringMessage;
    }


}
