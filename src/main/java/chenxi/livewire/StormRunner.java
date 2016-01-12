package chenxi.livewire;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import chenxi.livewire.storm.bolt.MongoWriterBolt;
import chenxi.livewire.storm.bolt.QuoteParseBolt;
import chenxi.livewire.storm.spout.YahooFinanceSpout;
import org.apache.log4j.Logger;

/**
 * livewire-storm
 * <p>
 * Created by chenxili on 06/01/2016.
 */
public class StormRunner {
    private static final String YAHOO_FINANCE_SPOUT = "YAHOO_FINANCE_SPOUT";
    private static final String MONGO_WRITER_BOLT = "MONGO_WRITER_BOLT";
    private static final String QUOTE_PARSE_BOLT = "QUOTE_PARSE_BOLT";

    private static final Logger LOGGER = Logger.getLogger(StormRunner.class);

    private static LocalCluster cluster;

    public StormRunner(){}

    public static void main(String[] args){

        StormRunner stormRunner = new StormRunner();

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);
        config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        config.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1);
        config.setMaxSpoutPending(1);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(YAHOO_FINANCE_SPOUT, new YahooFinanceSpout(), 1).setNumTasks(1);

        topologyBuilder.setBolt(QUOTE_PARSE_BOLT, new QuoteParseBolt(), 2).shuffleGrouping(YAHOO_FINANCE_SPOUT, "");
        //topologyBuilder.setBolt(MONGO_WRITER_BOLT, new MongoWriterBolt(), 1).allGrouping(QUOTE_PARSE_BOLT, "");

        cluster = new LocalCluster();

        cluster.submitTopology("Livewire", config, topologyBuilder.createTopology());

        stormRunner.attachShutDownHook();
    }

    /**
     * attachShutDownHook method will attach a shutdown hook that will be run when a
     * graceful shutdown occurs. Note that kill -9 or terminating the application abruptly
     * may not invoke this method. You should use kill -15 instead to give the JVM a chance
     * to respond.
     * <p/>
     * There is currently a bug in Storm that prevents the clean up of residue files in your
     * Users/SOEID/AppData/Local/Temp directory. This should not happen on Unix but you should
     * clean up these files locally when on Windows.
     */
    public void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOGGER.info("Shutdown Hook is Running");
                if (cluster != null) {
                    cluster.killTopology("Livewire");
                    cluster.shutdown();
                }
            }
        });
        LOGGER.info("Shut Down Hook Attached");
    }
}
