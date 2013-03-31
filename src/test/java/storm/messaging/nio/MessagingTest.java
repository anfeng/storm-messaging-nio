package storm.messaging.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MessagingTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(MessagingTest.class);
    private final String TEST_MSG = new String("0123456789abcdefghijklmnopqrstuvwxyz");
    private final int port = 6789;
    
    public void test_server_started() {
        try {
            //create a context
            Context context = new Context();
            context.prepare(null);
            
            //set up a server
            IConnection server = context.bind(null, port);
            
            //set up a client
            IConnection client = context.connect(null, "localhost", port);

            //client sends a message
            client.send(0, TEST_MSG.getBytes());
            
            //server receives a message
            TaskMessage message = server.recv(0);
            
            assertEquals(message.task(), 0);
            assertEquals(new String(message.message()), TEST_MSG);
            //terminate
            context.term();
        } catch (RuntimeException e) {
            assertTrue(e != null);
        }
    }

    public void test_server_delayed() {
        try {
            //create a context
            Context context = new Context();
            context.prepare(null);

            //set up a client
            IConnection client = context.connect(null, "localhost", port);

            //client sends a message
            client.send(0, TEST_MSG.getBytes());
 
            //delay
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //set up a server
            IConnection server = context.bind(null, port);
            
            //server receives a message
            TaskMessage message = server.recv(0);
            
            assertEquals(message.task(), 0);
            assertEquals(new String(message.message()), TEST_MSG);

            //terminate
            context.term();
        } catch (RuntimeException e) {
            assertTrue(e != null);
        }
    }
}