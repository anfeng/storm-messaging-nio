package storm.messaging.nio;

import java.util.Map;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

public class Context implements IContext {
    private static final Logger LOG = LoggerFactory.getLogger(Context.class);
    private Map storm_conf;
    private Vector<IConnection> server_connections;
    private Vector<IConnection> client_connections;
    
    public void prepare(Map storm_conf) {
       LOG.debug("Context prepare()");

       this.storm_conf = storm_conf;
       server_connections = new Vector<IConnection>(); 
       client_connections = new Vector<IConnection>(); 
    }

    public IConnection bind(String storm_id, int port) {
        LOG.debug("Context bind()");

        IConnection server = new Server(storm_conf, port);
        server_connections.add(server);
        return server;
    }

    public IConnection connect(String storm_id, String host, int port) {        
        LOG.debug("Context connect()");

        IConnection client =  new Client(storm_conf, host, port);
        client_connections.add(client);
        return client;
    }

    public void term() {
        LOG.debug("Context term()");

        for (IConnection con : client_connections) {
            con.close();
        }
        for (IConnection con : server_connections) {
            con.close();
        }
    }

}
