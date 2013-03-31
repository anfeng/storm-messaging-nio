package storm.messaging.nio;

import java.net.InetSocketAddress;
import java.util.Map;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server implements IConnection {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    private static final int BUFFER_SIZE = 1024;
    Map storm_conf;
    int port;
    AsynchronousServerSocketChannel server;
    Future<AsynchronousSocketChannel> worker_future;
    private final TaskMessage ACK_RESP = new TaskMessage(-1, new String("ack").getBytes());
    
    Server(Map storm_conf, int port) {
        this.storm_conf = storm_conf;
        this.port = port;
        
        // open a server channel and bind to a free address, then accept a connection
        try {
            LOG.debug("Open server channel");
            server = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getLocalHost(), port));
            worker_future = server.accept();
        } catch (IOException e) {
            LOG.error("Server error ", e);
            throw new RuntimeException(e);
        } 
    }

    public synchronized TaskMessage recv(int flags)  {
        LOG.debug("Server recv()");
        ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        AsynchronousSocketChannel channel;
        try {
            if (flags==1) { //non-blocking
                if (worker_future.isDone()) return null;
                channel = worker_future.get();
                Future<Integer> read_future = channel.read(buffer);
                if (!read_future.isDone()) return null;
                read_future.get();
            } else {
                channel = worker_future.get();
                channel.read(buffer).get();
            }
            
            //send ack
            sendAckResp(channel);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } 
        
        buffer.flip();
        TaskMessage message = new TaskMessage(0, null);
        message.deserialize(buffer);
        LOG.debug("message received with task:"+message.task()+" payload:\'"+new String(message.message())+ "\' size:"+message.message().length);

        return message;
    }

    private void sendAckResp(AsynchronousSocketChannel channel) throws InterruptedException, ExecutionException {
        ByteBuffer ack_buffer = ACK_RESP.serialize();
        ack_buffer.flip();
        channel.write(ack_buffer).get();
    }
    
    public void close() {
        LOG.debug("Server close()");
        try {
            worker_future.cancel(true);
            server.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void send(int task, byte[] message) {
        throw new RuntimeException("Server connection should not send any messages");
    }
}
