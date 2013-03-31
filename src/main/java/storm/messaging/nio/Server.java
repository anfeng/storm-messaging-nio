package storm.messaging.nio;

import java.net.InetSocketAddress;
import java.util.Map;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;


class Server implements IConnection {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    private static final int BUFFER_SIZE = 1024;
    private static final int THREAD_POOL_SIZE = 2;
    Map storm_conf;
    int port;
    private LinkedBlockingQueue<TaskMessage> message_queue;
    AsynchronousChannelGroup thread_group;
    AsynchronousServerSocketChannel server;

    Server(Map storm_conf, int port) {
        this.storm_conf = storm_conf;
        this.port = port;
        message_queue = new LinkedBlockingQueue<TaskMessage>();

        // open a server channel and bind to a free address, then accept a connection
        try {
            // create a channel group
            AsynchronousChannelGroup thread_group = AsynchronousChannelGroup.withFixedThreadPool(THREAD_POOL_SIZE, Executors.defaultThreadFactory());
            // and pass to a channel to use
            server = AsynchronousServerSocketChannel.open(thread_group).bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
            server.accept(null, new ServerHandler());
        } catch (IOException e) {
            LOG.error("Server error ", e);
            throw new RuntimeException(e);
        } 
    }

    public synchronized TaskMessage recv(int flags)  {
        LOG.debug("Server recv()");
        if (flags==1) { //non-blocking
            return message_queue.poll();
        } else {
            try {
                return message_queue.take();
            } catch (InterruptedException e) {
                LOG.info("exception within msg receiving", e);
                return null;
            }
        }
    }

    public void close() {
        LOG.debug("Server close()");
        try {
            if (server != null) { 
                server.close();
                server = null;
            }
            thread_group.shutdownNow();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void send(int task, byte[] message) {
        throw new RuntimeException("Server connection should not send any messages");
    }

    class ServerHandler implements CompletionHandler<AsynchronousSocketChannel,Void> {
        private final TaskMessage ACK_RESP = new TaskMessage(-1, new String("ack").getBytes());

        @Override
        public void completed(AsynchronousSocketChannel channel, Void att) {
            // accept the next connection
            server.accept(null, this);

            // handle this connection
            try {
                while (true) {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
                    channel.read(buffer).get();
                    if (buffer.position()<=0) break;
                    
                    buffer.flip();
                    TaskMessage message = new TaskMessage(0, null);
                    message.deserialize(buffer);
                    message_queue.put(message);
                    LOG.debug("message received with task:"+message.task()+" payload:\'"+new String(message.message())+ "\' size:"+message.message().length);
                    
                    //send ack
                    sendAckResp(channel);                    
                }
            } catch (InterruptedException e) {
                closeChannel(channel);
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                closeChannel(channel);
            } 
            
            closeChannel(channel);
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            //LOG.debug("server failure", exc);
            try {
                if (server != null) { 
                    server.close();
                    server = null;
                }
            } catch (IOException e) {
            }
        }

        private void sendAckResp(AsynchronousSocketChannel channel) throws InterruptedException, ExecutionException {
            ByteBuffer ack_buffer = ACK_RESP.serialize();
            ack_buffer.flip();
            channel.write(ack_buffer).get();
        }
        
        private void closeChannel(AsynchronousSocketChannel channel) {
            try {
                if (channel.isOpen())
                    channel.close();
            } catch (IOException e) { 
                throw new RuntimeException(e);
            }
        }
    }
}
