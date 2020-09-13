
import com.sun.net.httpserver.HttpServer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Configurable;
import org.glassfish.jersey.logging.LoggingFeature;

@ApplicationPath("/")
public class Main extends Application {
    // Base URI the HTTP server will listen to

    /**
     * Starts a server, initializes and keeps the server alive
     * @param args
     * @throws IOException
     */


    /**
     * Default constructor
     */
    public Main() {
        super();
    }

    /**
     * Initialize the web application
     */
    @PostConstruct
    public static void initialize() throws SQLException {
        System.out.println("Server starting, invoking initializing methods....");

        //creating Connection Pool, registering for ZK information changes
        DBUtil.initDataSource();

        //connections can not be obtained by invoking DBUtil.getConnection()
    }

    /**
     * Define the set of "Resource" classes for the javax.ws.rs.core.Application
     */
    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> h = new HashSet<>();
        h.add(Dummy.class);
        return h;
    }
}