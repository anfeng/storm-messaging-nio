package storm.messaging.nio;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.nio.channels.CompletionHandler;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

public class Client implements IConnection {
    private static final int MAX_RETRIES_LIMIT = 29;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private AsynchronousSocketChannel client;
    private SocketAddress remote_addr;
    private String host;
    private int port;
    private LinkedBlockingQueue<TaskMessage> message_queue;

    @SuppressWarnings("unchecked")
    Client(Map storm_conf, String host, int port) {
        this.host = host;
        this.port = port;
        message_queue = new LinkedBlockingQueue<TaskMessage>();

        try {
            client = AsynchronousSocketChannel.open();
            remote_addr = new InetSocketAddress(host, port);
            client.connect(remote_addr, (Object)null, 
                    new Handler(1, MAX_RETRIES_LIMIT, DEFAULT_MAX_SLEEP_MS));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LOG.debug("Client created");
    }

    public void send(int task, byte[] message) {
        LOG.debug("Client send()");
        try {
            message_queue.put(new TaskMessage(task, message));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        LOG.debug("Client close()");
        try {
            client.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public TaskMessage recv(int flags) {
        throw new RuntimeException("Client connection should not receive any messages");
    }

    class Handler implements CompletionHandler {
        private final Random random = new Random();
        private final int baseSleepTimeMs;
        private final int maxSleepMs;
        private int retryCount;

        /**
         * @param baseSleepTimeMs initial amount of time to wait between retries
         * @param maxRetries max number of times to retry
         * @param maxSleepMs max time in ms to sleep on each retry
         */
        public Handler(int baseSleepTimeMs, int maxRetries, int maxSleepMs)
        {
            validateMaxRetries(maxRetries);
            retryCount = 0;
            this.baseSleepTimeMs = baseSleepTimeMs;
            this.maxSleepMs = maxSleepMs;
        }

        @Override
        public void completed(Object result, Object attachment) {
            LOG.info("successfully connected to server");
            try {
                while (true) {
                    //take a message from queue
                    TaskMessage message = message_queue.take();
                    // send a message to the server
                    LOG.debug("message to be sent task:"+message.task()+" payload:"+new String(message.message()));
                    ByteBuffer buffer = message.serialize();
                    buffer.flip();
                    client.write(buffer);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }                 
        }

        @SuppressWarnings("unchecked")
        @Override
        public void failed(Throwable exc, Object attachment) {
            try {
                client.close();

                Thread.sleep(getSleepTimeMs()*1000);
                retryCount++;

                LOG.info("Retry connection to  host:"+host+" port:"+port+ " ["+retryCount+"]");            
                client = AsynchronousSocketChannel.open();
                client.connect(remote_addr, (Object)null, this);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public int getBaseSleepTimeMs()
        {
            return baseSleepTimeMs;
        }

        protected int getSleepTimeMs()
        {
            // copied from Hadoop's RetryPolicies.java
            int sleepMs = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)));
            if ( sleepMs > maxSleepMs )
            {
                LOG.warn(String.format("Sleep extension too large (%d). Pinning to %d", sleepMs, maxSleepMs));
                sleepMs = maxSleepMs;
            }
            return sleepMs;
        }

        private int validateMaxRetries(int maxRetries)
        {
            if ( maxRetries > MAX_RETRIES_LIMIT )
            {
                LOG.warn(String.format("maxRetries too large (%d). Pinning to %d", maxRetries, MAX_RETRIES_LIMIT));
                maxRetries = MAX_RETRIES_LIMIT;
            }
            return maxRetries;
        }    
    }
}




