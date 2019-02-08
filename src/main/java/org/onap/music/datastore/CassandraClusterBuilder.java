package org.onap.music.datastore;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;

public class CassandraClusterBuilder {
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CassandraClusterBuilder.class);

    private static Cluster createCassandraCluster(String address) throws NoHostAvailableException {
        Cluster cluster = Cluster.builder().withPort(9042)
                .withCredentials(MusicUtil.getCassName(), MusicUtil.getCassPwd())
                .addContactPoint(address).build();
        Metadata metadata = cluster.getMetadata();
        logger.info(EELFLoggerDelegate.applicationLogger, "Connected to cassa cluster "
                + metadata.getClusterName() + " at " + address);
        return cluster;
    }
    /**
     *
     * @return
     */
    private static ArrayList<String> getAllPossibleLocalIps() {
        ArrayList<String> allPossibleIps = new ArrayList<String>();
        try {
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
            while (en.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) en.nextElement();
                Enumeration<InetAddress> ee = ni.getInetAddresses();
                while (ee.hasMoreElements()) {
                    InetAddress ia = (InetAddress) ee.nextElement();
                    allPossibleIps.add(ia.getHostAddress());
                }
            }
        } catch (SocketException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.CONNCECTIVITYERROR, ErrorSeverity.ERROR, ErrorTypes.CONNECTIONERROR);
        }catch(Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), ErrorSeverity.ERROR, ErrorTypes.GENERALSERVICEERROR);
        }
        return allPossibleIps;
    }

    /**
     * This method iterates through all available local IP addresses and tries to connect to first successful one
     */
    public static Cluster connectToLocalCassandraCluster() {
        ArrayList<String> localAddrs = getAllPossibleLocalIps();
        localAddrs.add(0, "localhost");
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Connecting to cassa cluster: Iterating through possible ips:"
                        + getAllPossibleLocalIps());
        for (String address: localAddrs) {
            try {
                return createCassandraCluster(address);
            } catch (NoHostAvailableException e) {
                logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.HOSTUNAVAILABLE, ErrorSeverity.ERROR, ErrorTypes.CONNECTIONERROR);
            }
        }
        return null;
    }

    /**
     * This method connects to cassandra cluster on specific address.
     *
     * @param address
     */
    public static Cluster connectToRemoteCassandraCluster(String address) throws MusicServiceException {
        try {
            return createCassandraCluster(address);
        } catch (Exception ex) {
            logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(),AppMessages.CASSANDRACONNECTIVITY, ErrorSeverity.ERROR, ErrorTypes.SERVICEUNAVAILABLE);
            throw new MusicServiceException(
                    "Error while connecting to Cassandra cluster.. " + ex.getMessage());
        }
    }

    public static Cluster connectSmart(String cassaHost) throws MusicServiceException {
        if (cassaHost.equals("localhost")) {
            Cluster cluster = CassandraClusterBuilder.connectToLocalCassandraCluster();
            return cluster;
        } else {
            Cluster cluster = CassandraClusterBuilder.connectToRemoteCassandraCluster(MusicUtil.getMyCassaHost());
            return cluster;
        }

    }
}
