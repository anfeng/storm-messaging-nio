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
import static java.net.StandardSocketOptions.*;

class Client implements IConnection {
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
            client.setOption(SO_SNDBUF, 1024*1024);
            client.setOption(SO_RCVBUF, 1024*1024);
            remote_addr = new InetSocketAddress(host, port);
            client.connect(remote_addr, null, 
                    new ClientHandler(1, MAX_RETRIES_LIMIT, DEFAULT_MAX_SLEEP_MS));
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
            if (client != null) {
                client.close();
                client = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public TaskMessage recv(int flags) {
        throw new RuntimeException("Client connection should not receive any messages");
    }

    class ClientHandler implements CompletionHandler<Void,Void> {
        private final Random random = new Random();
        private final int baseSleepTimeMs;
        private final int maxSleepMs;
        private int retryCount;
        private ByteBuffer resp_buffer;
        private ByteBuffer header_buffer; 
        
        /**
         * @param baseSleepTimeMs initial amount of time to wait between retries
         * @param maxRetries max number of times to retry
         * @param maxSleepMs max time in ms to sleep on each retry
         */
        ClientHandler(int baseSleepTimeMs, int maxRetries, int maxSleepMs)
        {
            validateMaxRetries(maxRetries);
            retryCount = 0;
            this.baseSleepTimeMs = baseSleepTimeMs;
            this.maxSleepMs = maxSleepMs;
            resp_buffer = ByteBuffer.allocate(1024);
            header_buffer = ByteBuffer.allocate(Integer.SIZE);
        }

        @Override
        public void completed(Void result, Void attachment) {
            LOG.debug("successfully connected to server");
            try {
                while (true) {
                    //take a message from queue
                    TaskMessage message = message_queue.take();

                    // send a message to the server
                    LOG.debug("message to be sent task:"+message.task()+" payload size:"+message.message().length);
                    
                    //header buffer
                    header_buffer.clear();
                    int payload_size = 2 + message.message().length;
                    LOG.debug("payload size:"+payload_size);
                    header_buffer.putInt(payload_size);
                    header_buffer.flip();
                    client.write(header_buffer).get();
                    
                    //payload buffer
                    ByteBuffer payload_buffer = message.serialize();
                    payload_buffer.flip();
                    client.write(payload_buffer).get();

                    
                    //get ack response
                    recvAckResp();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                close();
            }                 
        }

        private void recvAckResp() throws InterruptedException, ExecutionException {
            resp_buffer.clear();
            client.read(resp_buffer).get();
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void failed(Throwable exc, Void attachment) {
            try {
                close();

                Thread.sleep(getSleepTimeMs()*1000);
                retryCount++;

                LOG.info("Retry connection to  host:"+host+" port:"+port+ " ["+retryCount+"]");            
                client = AsynchronousSocketChannel.open();
                client.connect(remote_addr, (Void)null, this);
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




