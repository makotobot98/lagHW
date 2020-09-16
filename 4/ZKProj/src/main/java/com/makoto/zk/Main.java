package com.makoto.zk;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.ApplicationPath;

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

        //connections can not be obtained by invoking com.makoto.zk.DBUtil.getConnection()
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