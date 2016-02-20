package bolt.statistic;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to keep some statistics about a Bolt connection.
 */
public class BoltStatistics {

    private final AtomicInteger numberOfSentDataPackets = new AtomicInteger(0);
    private final AtomicInteger numberOfReceivedDataPackets = new AtomicInteger(0);
    private final AtomicInteger numberOfDuplicateDataPackets = new AtomicInteger(0);
    private final AtomicInteger numberOfMissingDataEvents = new AtomicInteger(0);
    private final AtomicInteger numberOfNAKSent = new AtomicInteger(0);
    private final AtomicInteger numberOfNAKReceived = new AtomicInteger(0);
    private final AtomicInteger numberOfRetransmittedDataPackets = new AtomicInteger(0);
    private final AtomicInteger numberOfACKSent = new AtomicInteger(0);
    private final AtomicInteger numberOfACKReceived = new AtomicInteger(0);
    private final AtomicInteger numberOfCCSlowDownEvents = new AtomicInteger(0);
    private final AtomicInteger numberOfCCWindowExceededEvents = new AtomicInteger(0);

    private final String componentDescription;
    private final List<MeanValue> metrics = new ArrayList<MeanValue>();
    private final List<StatisticsHistoryEntry> statsHistory = new ArrayList<StatisticsHistoryEntry>();
    boolean first = true;
    private volatile long roundTripTime;
    private volatile long roundTripTimeVariance;
    private volatile long packetArrivalRate;
    private volatile long estimatedLinkCapacity;
    private volatile double sendPeriod;
    private volatile long congestionWindowSize;
    private long initialTime;

    public BoltStatistics(String componentDescription) {
        this.componentDescription = componentDescription;
    }

    public int getNumberOfSentDataPackets() {
        return numberOfSentDataPackets.get();
    }

    public int getNumberOfReceivedDataPackets() {
        return numberOfReceivedDataPackets.get();
    }

    public int getNumberOfDuplicateDataPackets() {
        return numberOfDuplicateDataPackets.get();
    }

    public int getNumberOfNAKSent() {
        return numberOfNAKSent.get();
    }

    public int getNumberOfNAKReceived() {
        return numberOfNAKReceived.get();
    }

    public int getNumberOfRetransmittedDataPackets() {
        return numberOfRetransmittedDataPackets.get();
    }

    public int getNumberOfACKSent() {
        return numberOfACKSent.get();
    }

    public int getNumberOfACKReceived() {
        return numberOfACKReceived.get();
    }

    public void incNumberOfSentDataPackets() {
        numberOfSentDataPackets.incrementAndGet();
    }

    public void incNumberOfReceivedDataPackets() {
        numberOfReceivedDataPackets.incrementAndGet();
    }

    public void incNumberOfDuplicateDataPackets() {
        numberOfDuplicateDataPackets.incrementAndGet();
    }

    public void incNumberOfMissingDataEvents() {
        numberOfMissingDataEvents.incrementAndGet();
    }

    public void incNumberOfNAKSent() {
        numberOfNAKSent.incrementAndGet();
    }

    public void incNumberOfNAKReceived() {
        numberOfNAKReceived.incrementAndGet();
    }

    public void incNumberOfRetransmittedDataPackets() {
        numberOfRetransmittedDataPackets.incrementAndGet();
    }

    public void incNumberOfACKSent() {
        numberOfACKSent.incrementAndGet();
    }

    public void incNumberOfACKReceived() {
        numberOfACKReceived.incrementAndGet();
    }

    public void incNumberOfCCWindowExceededEvents() {
        numberOfCCWindowExceededEvents.incrementAndGet();
    }

    public void incNumberOfCCSlowDownEvents() {
        numberOfCCSlowDownEvents.incrementAndGet();
    }

    public void setRTT(long rtt, long rttVar) {
        this.roundTripTime = rtt;
        this.roundTripTimeVariance = rttVar;
    }

    public void setPacketArrivalRate(long rate, long linkCapacity) {
        this.packetArrivalRate = rate;
        this.estimatedLinkCapacity = linkCapacity;
    }

    public double getSendPeriod() {
        return sendPeriod;
    }

    public void setSendPeriod(double sendPeriod) {
        this.sendPeriod = sendPeriod;
    }

    public long getCongestionWindowSize() {
        return congestionWindowSize;
    }

    public void setCongestionWindowSize(long congestionWindowSize) {
        this.congestionWindowSize = congestionWindowSize;
    }

    public long getPacketArrivalRate() {
        return packetArrivalRate;
    }

    /**
     * add a metric
     *
     * @param m - the metric to add
     */
    public void addMetric(MeanValue m) {
        metrics.add(m);
    }

    /**
     * get a read-only list containing all metrics
     *
     * @return
     */
    public List<MeanValue> getMetrics() {
        return Collections.unmodifiableList(metrics);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Statistics for ").append(componentDescription).append("\n");
        sb.append("Sent data packets: ").append(getNumberOfSentDataPackets()).append("\n");
        sb.append("Received data packets: ").append(getNumberOfReceivedDataPackets()).append("\n");
        sb.append("Duplicate data packets: ").append(getNumberOfDuplicateDataPackets()).append("\n");
        sb.append("ACK received: ").append(getNumberOfACKReceived()).append("\n");
        sb.append("NAK received: ").append(getNumberOfNAKReceived()).append("\n");
        sb.append("Retransmitted data: ").append(getNumberOfRetransmittedDataPackets()).append("\n");
        sb.append("NAK sent: ").append(getNumberOfNAKSent()).append("\n");
        sb.append("ACK sent: ").append(getNumberOfACKSent()).append("\n");
        if (roundTripTime > 0) {
            sb.append("RTT ").append(roundTripTime).append(" var. ").append(roundTripTimeVariance).append("\n");
        }
        if (packetArrivalRate > 0) {
            sb.append("Packet rate: ").append(packetArrivalRate).append("/sec., link capacity: ").append(estimatedLinkCapacity).append("/sec.\n");
        }
        if (numberOfMissingDataEvents.get() > 0) {
            sb.append("Sender without data events: ").append(numberOfMissingDataEvents.get()).append("\n");
        }
        if (numberOfCCSlowDownEvents.get() > 0) {
            sb.append("CC rate slowdown events: ").append(numberOfCCSlowDownEvents.get()).append("\n");
        }
        if (numberOfCCWindowExceededEvents.get() > 0) {
            sb.append("CC window slowdown events: ").append(numberOfCCWindowExceededEvents.get()).append("\n");
        }
        sb.append("CC parameter SND:  ").append((int) sendPeriod).append("\n");
        sb.append("CC parameter CWND: ").append(congestionWindowSize).append("\n");
        for (MeanValue v : metrics) {
            sb.append(v.getName()).append(": ").append(v.getFormattedMean()).append("\n");
        }
        return sb.toString();
    }

    /**
     * take a snapshot of relevant parameters for later storing to
     * file using {@link #writeParameterHistory(File)}
     */
    public void storeParameters() {
        synchronized (statsHistory) {
            if (first) {
                first = false;
                statsHistory.add(new StatisticsHistoryEntry(true, 0, metrics));
                initialTime = System.currentTimeMillis();
            }
            statsHistory.add(new StatisticsHistoryEntry(false, System.currentTimeMillis() - initialTime, metrics));
        }
    }

    /**
     * write saved parameters to disk
     *
     * @param toFile
     */
    public void writeParameterHistory(File toFile) throws IOException {
        FileWriter fos = new FileWriter(toFile);
        try {
            synchronized (statsHistory) {
                for (StatisticsHistoryEntry s : statsHistory) {
                    fos.write(s.toString());
                    fos.write('\n');
                }
            }
        } finally {
            fos.close();
        }
    }

}
