import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/helloworld")
public class Dummy {

    @GET
    @Produces("text/plain")
    public String getClichedMessage() {
        return "Dummy Page counter: " + Main.myCounter;
    }
}
