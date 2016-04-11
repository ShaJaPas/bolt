package io.lyracommunity.bolt.statistic;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to keep some statistics about a Bolt connection.
 */
public class BoltStatistics {

    private final AtomicInteger numberOfSentDataPackets          = new AtomicInteger(0);
    private final AtomicInteger numberOfReceivedDataPackets      = new AtomicInteger(0);
    private final AtomicInteger numberOfDuplicateDataPackets     = new AtomicInteger(0);
    private final AtomicInteger numberOfMissingDataEvents        = new AtomicInteger(0);
    private final AtomicInteger numberOfNAKSent                  = new AtomicInteger(0);
    private final AtomicInteger numberOfNAKReceived              = new AtomicInteger(0);
    private final AtomicInteger numberOfRetransmittedDataPackets = new AtomicInteger(0);
    private final AtomicInteger numberOfACKSent                  = new AtomicInteger(0);
    private final AtomicInteger numberOfACKReceived              = new AtomicInteger(0);
    private final AtomicInteger numberOfCCSlowDownEvents         = new AtomicInteger(0);
    private final AtomicInteger numberOfCCWindowExceededEvents   = new AtomicInteger(0);
    private final AtomicInteger numberOfArtificialDrops          = new AtomicInteger(0);
    private final String componentDescription;
    private final List<StatisticsHistoryEntry> statsHistory = new ArrayList<>();
    private final Map<Integer, AtomicLong> receivedByClass = new HashMap<>();

    // Sender metrics
    private final MeanValue dgSendTime     = new MeanValue("SENDER: Datagram send time");
    private final MeanValue dgSendInterval = new MeanValue("SENDER: Datagram send interval");
    private final MeanThroughput throughput;

    // Receiver metrics
    private final MeanValue dgReceiveInterval  = new MeanValue("RECEIVER: Bolt receive interval");
    private final MeanValue dataPacketInterval = new MeanValue("RECEIVER: Data packet interval");
    private final MeanValue processTime        = new MeanValue("RECEIVER: Bolt packet process time");
    private final MeanValue dataProcessTime    = new MeanValue("RECEIVER: Data packet process time");
    private volatile long   roundTripTime;
    private volatile long   roundTripTimeVariance;
    private volatile long   packetArrivalRate;
    private volatile long   estimatedLinkCapacity;
    private volatile double sendPeriod;
    private volatile long   congestionWindowSize;
    private          long   initialTime;


    public BoltStatistics(final String componentDescription, final int datagramSize) {
        this.componentDescription = componentDescription;
        this.throughput = new MeanThroughput("SENDER: Throughput", datagramSize);
    }

    public int getNumberOfSentDataPackets() {
        return numberOfSentDataPackets.get();
    }

    private int getNumberOfArtificialDrops() {
        return numberOfArtificialDrops.get();
    }

    public int getNumberOfReceivedDataPackets() {
        return numberOfReceivedDataPackets.get();
    }

    private int getNumberOfDuplicateDataPackets() {
        return numberOfDuplicateDataPackets.get();
    }

    private int getNumberOfNAKSent() {
        return numberOfNAKSent.get();
    }

    private int getNumberOfNAKReceived() {
        return numberOfNAKReceived.get();
    }

    private int getNumberOfRetransmittedDataPackets() {
        return numberOfRetransmittedDataPackets.get();
    }

    private int getNumberOfACKSent() {
        return numberOfACKSent.get();
    }

    private int getNumberOfACKReceived() {
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

    public void incNumberOfArtificialDrops() {
        numberOfArtificialDrops.incrementAndGet();
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

    public void addReceived(final Integer classID, final long bytesReceived) {
        incNumberOfReceivedDataPackets();
        AtomicLong totalByClass = receivedByClass.get(classID);
        if (totalByClass == null) {
            receivedByClass.putIfAbsent(classID, new AtomicLong(0));
            totalByClass = receivedByClass.get(classID);
        }
        totalByClass.addAndGet(bytesReceived);
    }

    public void setRTT(final long rtt, final long rttVar) {
        this.roundTripTime = rtt;
        this.roundTripTimeVariance = rttVar;
    }

    public void setPacketArrivalRate(final long rate, final long linkCapacity) {
        this.packetArrivalRate = rate;
        this.estimatedLinkCapacity = linkCapacity;
    }

    public double getSendPeriod() {
        return sendPeriod;
    }

    public void setSendPeriod(final double sendPeriod) {
        this.sendPeriod = sendPeriod;
    }

    public void setCongestionWindowSize(final long congestionWindowSize) {
        this.congestionWindowSize = congestionWindowSize;
    }

    public long getPacketArrivalRate() {
        return packetArrivalRate;
    }

    /**
     * @return a read-only list containing all metrics.
     */
    private List<MeanValue> getMetrics() {
        return Collections.unmodifiableList(Arrays.asList(dgSendInterval, dgSendTime, throughput, dgReceiveInterval,
                dataPacketInterval, processTime, dataProcessTime));
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Statistics for ").append(componentDescription).append("\n");
        sb.append("Sent data packets: ").append(getNumberOfSentDataPackets()).append("\n");
        sb.append("Received data packets: ").append(getNumberOfReceivedDataPackets()).append("\n");
        sb.append("Duplicate data packets: ").append(getNumberOfDuplicateDataPackets()).append("\n");
        sb.append("ACK received: ").append(getNumberOfACKReceived()).append("\n");
        sb.append("NAK received: ").append(getNumberOfNAKReceived()).append("\n");
        sb.append("Retransmitted data packets: ").append(getNumberOfRetransmittedDataPackets()).append("\n");
        sb.append("NAK sent: ").append(getNumberOfNAKSent()).append("\n");
        sb.append("ACK sent: ").append(getNumberOfACKSent()).append("\n");
        if (roundTripTime > 0) {
            sb.append("RTT ").append(roundTripTime).append(" var. ").append(roundTripTimeVariance).append("\n");
        }
        if (getPacketArrivalRate() > 0) {
            sb.append("Packet rate: ").append(getPacketArrivalRate()).append("/sec., link capacity: ").append(estimatedLinkCapacity).append("/sec.\n");
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
        if (getNumberOfArtificialDrops() > 0) {
            sb.append("Number of artificial drops: ").append(getNumberOfArtificialDrops()).append("\n");
        }
        sb.append("CC parameter SND:  ").append((int) sendPeriod).append("\n");
        sb.append("CC parameter CWND: ").append(congestionWindowSize).append("\n");
        for (final MeanValue v : getMetrics()) {
            sb.append(v.getName()).append(": ").append(v.getFormattedMean()).append("\n");
        }
        sb.append("Breakdown by class:").append("\n");
        for (Map.Entry<Integer, AtomicLong> bytesReceived : receivedByClass.entrySet()) {
            sb.append("  Class ").append(bytesReceived.getKey()).append(" receive total of ")
                    .append(bytesReceived.getValue()).append(" bytes.").append("\n");
        }
        sb.append("-------------------").append("\n");
        return sb.toString();
    }

    /**
     * Take a snapshot of relevant parameters for later storing to
     * file using {@link #writeParameterHistory(File)}
     */
    public void storeParameters() {
        final List<MeanValue> metrics = getMetrics();
        final boolean first = (initialTime == 0L);
        if (first) {
            statsHistory.add(new StatisticsHistoryEntry(true, 0, metrics));
            initialTime = System.currentTimeMillis();
        }
        statsHistory.add(new StatisticsHistoryEntry(false, System.currentTimeMillis() - initialTime, metrics));
    }

    /**
     * Write saved parameters to disk.
     *
     * @param toFile
     */
    private void writeParameterHistory(File toFile) throws IOException {
        try (final FileWriter fos = new FileWriter(toFile)) {
            for (StatisticsHistoryEntry s : new ArrayList<>(statsHistory)) {
                fos.write(s.toString());
                fos.write('\n');
            }
        }
    }

    public void beginSend() {
        dgSendInterval.end();
        dgSendTime.begin();
    }

    public void endSend() {
        dgSendTime.end();
        dgSendInterval.begin();
        throughput.end();
        throughput.begin();
    }

    public void beginDataProcess() {
        dataPacketInterval.end();
        dataProcessTime.begin();
    }

    public void endDataProcess() {
        dataProcessTime.end();
        dataPacketInterval.begin();
    }

    public void beginProcess() {
        processTime.begin();
    }

    public void endProcess() {
        processTime.end();
    }

    public void beginReceive() {
        dgReceiveInterval.end();
    }

    public void endReceive() {
        dgReceiveInterval.begin();
    }


}
