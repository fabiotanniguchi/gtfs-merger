import org.onebusaway.gtfs.impl.GtfsDaoImpl;
import org.onebusaway.gtfs.serialization.GtfsReader;
import org.onebusaway.gtfs.serialization.GtfsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class GtfsMergerApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(GtfsMergerApplication.class);

    public static void main(String args[]) throws Exception{
        LOGGER.info("Reading the priority GTFS: " + args[0]);
        GtfsReader reader1 = new GtfsReader();
        reader1.setInputLocation(new File(args[0]));
        GtfsDaoImpl priorityGtfsStore = new GtfsDaoImpl();
        reader1.setEntityStore(priorityGtfsStore);
        reader1.run();

        LOGGER.info("Reading the other GTFS: " + args[1]);
        GtfsReader reader2 = new GtfsReader();
        reader2.setInputLocation(new File(args[1]));
        GtfsDaoImpl otherGtfsStore = new GtfsDaoImpl();
        reader2.setEntityStore(otherGtfsStore);
        reader2.run();

        GtfsMerger merger = new GtfsMerger();
        GtfsDaoImpl merged = merger.merge(priorityGtfsStore, otherGtfsStore);

        LOGGER.info("Writing the final GTFS");
        GtfsWriter writer = new GtfsWriter();
        writer.setOutputLocation(new File("MERGED_GTFS.zip"));
        writer.run(merged);
        writer.close();
    }
}
