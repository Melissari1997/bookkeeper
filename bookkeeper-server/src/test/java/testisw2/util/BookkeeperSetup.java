package testisw2.util;



import java.io.File;
import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.metastore.InMemoryMetaStore;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.PortManager;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rivisitazione della classe di test BookKeperClusterTestCase.
 */
public abstract class BookkeeperSetup {

    static final Logger LOG = LoggerFactory.getLogger(BookkeeperSetup.class);

    protected final ZooKeeperSetup zkUtil;
    protected ZooKeeper zkc;
    protected String metadataServiceUri;

    protected final List<File> tmpDirs = new LinkedList<File>();
    protected BookKeeper bkc;

    protected  ServerConfiguration baseConf ;
    protected final ClientConfiguration baseClientConf = new ClientConfiguration();


    public BookkeeperSetup() throws SocketException, UnknownHostException{
        zkUtil = new ZooKeeperSetup();
        //definisco una configurazione base per i Bookie server, settando solo la listening interface
        ServerConfiguration conf= new ServerConfiguration();
        conf.setListeningInterface(getLoopbackInterfaceName());
        conf.setAllowLoopback(true);
        this.baseConf = conf;
    }

    protected void setUp() throws Exception {
        LOG.info("Setting up test {}", getClass());
        InMemoryMetaStore.reset();
        setMetastoreImplClass(baseConf);
        setMetastoreImplClass(baseClientConf);

        try {
            // inizializza Zookeeper
        	zkUtil.startZookeeper();
           
            this.metadataServiceUri = zkUtil.getMetadataServiceUri("/ledgers");
            
            //inizializza Bookkeeper
            baseConf.setMetadataServiceUri(metadataServiceUri);
            baseClientConf.setMetadataServiceUri(metadataServiceUri);
            baseClientConf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
            startNewBookie();

        } catch (Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }
    }

    protected File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tmpDirs.add(dir);
        return dir;
    }


    /**
     * Crea una configurazione per un BookieServer a partire dalla configurazione base
     * @return configurazione per un BookieServer
     * @throws Exception
     */
    protected ServerConfiguration newServerConfiguration() throws Exception {
        File f = createTempDir("bookie", "test");

        int port;
        if (baseConf.isEnableLocalTransport() || !baseConf.getAllowEphemeralPorts()) {
            port = PortManager.nextFreePort();
        } else {
            port = 0;
        }
        File[] ledgerDirs = new File[] { f };
        ServerConfiguration conf = new ServerConfiguration(baseConf);
        conf.setBookiePort(port);
        conf.setJournalDirName(f.getPath());
        String[] ledgerDirNames = new String[ledgerDirs.length];
        for (int i = 0; i < ledgerDirs.length; i++) {
            ledgerDirNames[i] = ledgerDirs[i].getPath();
        }
        conf.setLedgerDirNames(ledgerDirNames);
        conf.setEnableTaskExecutionStats(true);
        conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        return conf;
    }

  /**
   * Inizializzazione di un BookieServer
   * @return port associata al BookieServer
   * @throws Exception
   */
    public int startNewBookie()
            throws Exception {
    	ServerConfiguration conf = newServerConfiguration();
        LOG.info("Starting new bookie on port: {}", conf.getBookiePort());
        BookieServer server = startBookie(conf);
        server.start();

        return server.getLocalAddress().getPort();
    }
    
    /**
     * Bookie start
     *
     * @param conf
     *            Server Configuration Object
     *
     */
    protected BookieServer startBookie(ServerConfiguration conf)
            throws Exception {
        TestStatsProvider provider = new TestStatsProvider();
        BookieServer server = new BookieServer(conf, provider.getStatsLogger(""),
                                                BookieServiceInfo.NO_INFO);
        if (bkc == null) {
            bkc = new BookKeeper(baseClientConf);
        }
        return server;
    }


    public void setMetastoreImplClass(AbstractConfiguration conf) {
        conf.setMetastoreImplClass(InMemoryMetaStore.class.getName());
    }
    
/**
 * Genera l'interfaccia a cui connettersi con il BookieServer
 * @return nome dell'interfaccia di LoopBack
 */
    private static String getLoopbackInterfaceName() {
        try {
            Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface nif : Collections.list(nifs)) {
                if (nif.isLoopback()) {
                    return nif.getName();
                }
            }
        } catch (SocketException se) {
            LOG.warn("Exception while figuring out loopback interface. Will use null.", se);
            return null;
        }
        LOG.warn("Unable to deduce loopback interface. Will use null");
        return null;
    }
}
