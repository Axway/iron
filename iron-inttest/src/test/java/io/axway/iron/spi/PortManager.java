package io.axway.iron.spi;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class PortManager {
    private static final Set<Integer> PORT_USED = new HashSet<>();

    public static int acquireAvailablePort() {
        int port = getNewRandomPort();
        synchronized (PortManager.class) {
            while (PORT_USED.contains(port) || !isPortAvailable(port)) {
                port = getNewRandomPort();
            }
            PORT_USED.add(port);
        }
        return port;
    }

    /**
     * Checks to see if a specific port is available.
     * FROM http://svn.apache.org/viewvc/camel/trunk/components/camel-test/src/main/java/org/apache/camel/test/AvailablePortFinder.java?view=markup
     *
     * @param port the port to check for availability
     */
    public static boolean isPortAvailable(int port) {
        try (ServerSocket ss = new ServerSocket(port); //
             DatagramSocket ds = new DatagramSocket(port)) {
            ss.setReuseAddress(true);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException ignored) {
        }

        return false;
    }

    private static int getNewRandomPort() {
        // avoid to use FlexMojo compilation ports (13539 & 13540)
        // avoid to use ephemeral port ( >32767 on linux)
        return ThreadLocalRandom.current().nextInt(13550, 32767);
    }

    private PortManager() {
    }
}

