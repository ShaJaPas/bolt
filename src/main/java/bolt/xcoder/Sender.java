package bolt.xcoder;

import java.io.IOException;

/**
 * Created by keen on 27/02/16.
 */
public interface Sender {


    void send(Object obj, long destId) throws IOException;


}
