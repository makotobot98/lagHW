package com.makoto.zk;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/helloworld")
public class Dummy {

    @GET
    @Produces("text/plain")
    public String getClichedMessage() {
        return "com.makoto.zk.Dummy Page counter";
    }
}
