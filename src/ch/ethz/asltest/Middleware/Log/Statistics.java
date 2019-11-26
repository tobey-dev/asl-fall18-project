package ch.ethz.asltest.Middleware.Log;

import ch.ethz.asltest.Middleware.Environment.Environment;
import ch.ethz.asltest.Middleware.Global.Global;
import ch.ethz.asltest.Middleware.Global.Parameters;
import ch.ethz.asltest.Middleware.Job.Job;
import ch.ethz.asltest.Middleware.Result.Result;
import ch.ethz.asltest.Middleware.Result.ResultMerger;
import ch.ethz.asltest.Middleware.Threading.ShutdownThread;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/*
    This class is used by both the NetThread and WorkerThreads to output their aggregated statistics.
    The NetThread uses a static interface to put out its collected Thinking times.
    All WorkerThreads hold two instances (one for sets, one for gets) and add all Jobs to it they complete.
    Adding a Job to Statistics is constant time and thus doesn't interfere with measurements.
    When the middleware gets shut down, all WorkerThreads consolidate their Statistics and
    write out one file each for all set and get jobs completed by them.
    The MWParameters.xml config file specifies the formats it uses.
    Per-Job, per-second and histogram outputs are available for WorkerThread exports.
 */

public class Statistics {

    public final StatisticsType statisticsType;

    private long totalMissCount;
    private long totalJobCount;

    private long lowestClientSendTime = Long.MAX_VALUE;
    private long highestClientSendTime = Long.MIN_VALUE;

    private List<Job> jobList;

    // Response time histogram going from bucket to number of values in that bucket.
    private HashMap<Integer, Integer> histogram;

    private int errorCount = 0;
    private int serverErrorCount = 0;
    private int clientErrorCount = 0;

    private final double percentile;

    public void submitError(Result error){
        if (error instanceof  Result.ErrorResult){
            errorCount++;
        } else if (error instanceof  Result.ServerErrorResult){
            serverErrorCount++;
        } else if (error instanceof  Result.ClientErrorResult){
            clientErrorCount++;
        } else {
            Log.error("[Statistics] Non-error result submitted as error");
        }
    }

    // Maps clients to their thinking times (ip:port to list of times in ns)
    private static HashMap<String, List<Long>> thinkingTimesMap = new HashMap<>();

     /*
     Maps clients to save state about thinking time.
     For each client, we save the timestamp of the last byte we sent.
     On receiving the first byte, the difference consitutes approximately
     as the thinking time. The initial value of null shall be ignored.
      */
    private static HashMap<String, Long> clientSendTimeMap = new HashMap<>();

    public Statistics(StatisticsType statisticsType){
        this.statisticsType = statisticsType;
        jobList = new LinkedList<>();
        histogram = new HashMap<>();
        // Don't do this statically to avoid race conditions on parameter class parsing
        percentile = Parameters.getInteger("statistics_percentile_percent") / 100D;
    }


    public void submit(Job.SetJob setJob){
        if (statisticsType != StatisticsType.SET){
            Log.error("[Statistics] Adding set job to other type Statistics object");
            return;
        }

        addJob(setJob);
    }

    public void submit(Job.GetJob getJob){
        if (statisticsType != StatisticsType.GET){
            Log.error("[Statistics] Adding get job to other type Statistics object");
            return;
        }

        addJob(getJob);
    }

    // To be called when the first byte of a client connection has been read.
    // Will be used to measure the thinking time of the clients.
    public static void setClientArrivalTime(String client, Long timestamp){
        Long clientSendTime = clientSendTimeMap.get(client);
        if (clientSendTime == null){
            return;
        }
        thinkingTimesMap.putIfAbsent(client, new LinkedList<>());
        thinkingTimesMap.get(client).add((timestamp - clientSendTime) / (Parameters.getInteger("statistics_thinking_times_resolution_ns")));
    }

    // To be called when the last byte of a response has been written to a client connection.
    // Will be used to measure the thinking time of the clients.
    public static void setClientSendTime(String client, long timestamp){
        clientSendTimeMap.put(client, timestamp);
    }

    private void addJob(Job job){

        totalMissCount += job.getMissCount();
        totalJobCount += 1;

        lowestClientSendTime = Math.min(lowestClientSendTime, job.getClientSendTime());
        highestClientSendTime = Math.max(highestClientSendTime, job.getClientSendTime());

        jobList.add(job);
    }

    private static List<ThinkingTimeStatLine> consolidateThinkingTimes(HashMap<String, List<Long>> thinkingTimesMap){
        List<ThinkingTimeStatLine> thinkingTimeStatLineList = new ArrayList<>();
        List<Long> currentThinkingTimesList;
        for (String currentClientString : thinkingTimesMap.keySet()){
            currentThinkingTimesList = thinkingTimesMap.get(currentClientString);
            Collections.sort(currentThinkingTimesList);
            thinkingTimeStatLineList.add(extractThinkingTime(currentClientString, currentThinkingTimesList));
        }
        return thinkingTimeStatLineList;
    }

    private List<JobStatLine> consolidatePerJob(List<Job> jobList){
        ArrayList<JobStatLine> jobStatLineList = new ArrayList<>();
        for (Job currentJob : jobList){
            jobStatLineList.add(extractStatistics(currentJob));
        }
        return jobStatLineList;
    }

    private TimeStatLine[] consolidatePerSecond(List<Job> jobList){
        int bucketCount = 1 + (int) ((highestClientSendTime - lowestClientSendTime) / (1000*1000*Parameters.getInteger("statistics_time_resolution_ms")));

        TimeStatLine[] timeStatLineArray = new TimeStatLine[bucketCount];
        HashMap<Integer, List<Job>> bucketSorter = new HashMap<>(); // For each second, we have a list of jobs that finished therein

        for (int i = 0; i < bucketCount; i++){
            bucketSorter.put(i, new ArrayList<>());
        }

        int currentBucket = 0;
        for (Job currentJob : jobList){
            currentBucket = (int) ((currentJob.getClientSendTime() - lowestClientSendTime) / (1000*1000*Parameters.getInteger("statistics_time_resolution_ms")));
            bucketSorter.get(currentBucket).add(currentJob);
        }

        for (int i = 0; i < bucketCount; i++){
            timeStatLineArray[i] = extractStatistics(bucketSorter.get(i));
        }

        return timeStatLineArray;
    }

    private static ThinkingTimeStatLine extractThinkingTime(String client, List<Long> thinkingTimesList){
        return new ThinkingTimeStatLine(client, getLongAverage(thinkingTimesList), getLongMedian(thinkingTimesList), getLongPercentile(thinkingTimesList, (Parameters.getInteger("statistics_percentile_percent") / 100D)));
    }

    private JobStatLine extractStatistics(Job job){
        long[] serverArrivalTime = new long[Environment.getServerList().size()];
        long[] serverSendTime = new long[Environment.getServerList().size()];
        long clientArrivalTime = 0;
        long clientSendTime = 0;
        long queueTime = 0;
        int enqueueSize = 0;
        int dequeueSize = 0;

        for (int i = 0; i < Environment.getServerList().size(); i++) {
            serverArrivalTime[i] = job.getServerArrivalTime(i);
            serverSendTime[i] = job.getServerSendTime(i);
        }
        clientArrivalTime = job.getClientArrivalTime();
        clientSendTime = job.getClientSendTime();

        queueTime = job.getDequeueTime() - job.getEnqueueTime();
        enqueueSize = job.getEnqueueSize();
        dequeueSize = job.getDequeueSize();

        // Update Histogram

        Integer currentBucket = (int) (job.getClientSendTime() - job.getClientArrivalTime()) / Parameters.getInteger("statistics_histogram_bin_size_ns");
        Integer currentBucketValue = histogram.get(currentBucket);
        if (currentBucketValue == null){
            histogram.put(currentBucket, 1);
        } else {
            histogram.put(currentBucket, currentBucketValue + 1);
        }

        return new JobStatLine(
                serverArrivalTime,
                serverSendTime,
                clientArrivalTime,
                clientSendTime,
                queueTime,
                enqueueSize,
                dequeueSize
        );
    }


    private TimeStatLine extractStatistics(List<Job> jobList){

        int consolidationCount = jobList.size();

        HashMap<Integer, List<Long>> serverResponseTimeMap = new HashMap<>();
        long[] avgServerResponseTime = new long[Environment.getServerList().size()];
        long[] medianServerResponseTime = new long[Environment.getServerList().size()];
        long[] percentileServerResponseTime = new long[Environment.getServerList().size()];

        List<Long> clientResponseTimeList = new ArrayList<>();
        long avgClientResponseTime = 0;
        long medianClientResponseTime = 0;
        long percentileClientResponseTime = 0;

        List<Long> queueTimeList = new ArrayList<>();
        long avgQueueTime = 0;
        long medianQueueTime = 0;
        long percentileQueueTime = 0;

        List<Integer> enqueueSizeList = new ArrayList<>();
        int avgEnqueueSize = 0;
        int medianEnqueueSize = 0;
        int percentileEnqueueSize = 0;
        int maxEnqueueSize = 0;

        List<Integer> dequeueSizeList = new ArrayList<>();
        int avgDequeueSize = 0;
        int medianDequeueSize = 0;
        int percentileDequeueSize = 0;
        int maxDequeueSize = 0;

        // Added for V2
        List <Long> netThreadTimeList = new ArrayList<>();
        long avgNetThreadTime = 0;
        long medianNetThreadTime = 0;
        long percentileNetThreadTime = 0;

        // From the moment a worker thread takes a job from the queue until it is done
        List <Long> processingTimeList = new ArrayList<>();
        long avgProcessingTime = 0;
        long medianProcessingTime = 0;
        long percentileProcessingTime = 0;

        // From the moment a worker thread takes a job from the queue until it is sent to the server
        List <Long> workerTimeList = new ArrayList<>();
        long avgWorkerTime = 0;
        long medianWorkerTime = 0;
        long percentileWorkerTime = 0;



        for (int i = 0; i < Environment.getServerList().size(); i++) {
            serverResponseTimeMap.put(i, new ArrayList<>());
        }

        // Add all values
        for (Job currentJob : jobList){
            for (int i = 0; i < Environment.getServerList().size(); i++) {
                serverResponseTimeMap.get(i).add(currentJob.getServerArrivalTime(i) - currentJob.getServerSendTime(i));
            }
            clientResponseTimeList.add(currentJob.getClientSendTime() - currentJob.getClientArrivalTime());
            queueTimeList.add(currentJob.getDequeueTime() - currentJob.getEnqueueTime());
            enqueueSizeList.add(currentJob.getEnqueueSize());
            dequeueSizeList.add(currentJob.getDequeueSize());

            //v2
            netThreadTimeList.add(currentJob.getEnqueueTime() - currentJob.getClientArrivalTime());
            processingTimeList.add(currentJob.getClientSendTime() - currentJob.getDequeueTime());

            long lastServerSendTime = 0;
            for (int i = 0; i < Environment.getServerList().size(); i++) {
                lastServerSendTime = Math.max(lastServerSendTime, currentJob.getServerSendTime(i));
            }
            workerTimeList.add(lastServerSendTime - currentJob.getDequeueTime());
        }

        // Sort the values because median and percentile calculation need sorted lists.
        for (int i = 0; i < Environment.getServerList().size(); i++) {
            Collections.sort(serverResponseTimeMap.get(i));
        }
        Collections.sort(clientResponseTimeList);
        Collections.sort(queueTimeList);
        Collections.sort(enqueueSizeList);
        Collections.sort(dequeueSizeList);

        //v2
        Collections.sort(netThreadTimeList);
        Collections.sort(processingTimeList);
        Collections.sort(workerTimeList);


        // Get the median and mean values
        for (int i = 0; i < Environment.getServerList().size(); i++) {
            avgServerResponseTime[i] = getLongAverage(serverResponseTimeMap.get(i));
            medianServerResponseTime[i] = getLongMedian(serverResponseTimeMap.get(i));
            percentileServerResponseTime[i] = getLongPercentile(serverResponseTimeMap.get(i), percentile);
        }

        avgClientResponseTime = getLongAverage(clientResponseTimeList);
        medianClientResponseTime = getLongMedian(clientResponseTimeList);
        percentileClientResponseTime = getLongPercentile(clientResponseTimeList, percentile);

        avgQueueTime = getLongAverage(queueTimeList);
        medianQueueTime = getLongMedian(queueTimeList);
        percentileQueueTime = getLongPercentile(queueTimeList, percentile);

        avgEnqueueSize = getIntegerAverage(enqueueSizeList);
        medianEnqueueSize = getIntegerMedian(enqueueSizeList);
        percentileEnqueueSize = getIntegerPercentile(enqueueSizeList, percentile);
        maxEnqueueSize = getIntegerMax(enqueueSizeList);

        avgDequeueSize = getIntegerAverage(dequeueSizeList);
        medianDequeueSize = getIntegerMedian(dequeueSizeList);
        percentileDequeueSize = getIntegerPercentile(dequeueSizeList, percentile);
        maxDequeueSize = getIntegerMax(dequeueSizeList);

        avgNetThreadTime = getLongAverage(netThreadTimeList);
        medianNetThreadTime = getLongMedian(netThreadTimeList);
        percentileNetThreadTime = getLongPercentile(netThreadTimeList, percentile);

        avgProcessingTime = getLongAverage(processingTimeList);
        medianProcessingTime = getLongMedian(processingTimeList);
        percentileProcessingTime = getLongPercentile(processingTimeList, percentile);

        avgWorkerTime = getLongAverage(workerTimeList);
        medianWorkerTime = getLongMedian(workerTimeList);
        percentileWorkerTime = getLongPercentile(workerTimeList, percentile);

        return new TimeStatLine(
                jobList.size(),
                avgServerResponseTime,
                medianServerResponseTime,
                percentileServerResponseTime,

                avgClientResponseTime,
                medianClientResponseTime,
                percentileClientResponseTime,

                avgQueueTime,
                medianQueueTime,
                percentileQueueTime,

                avgEnqueueSize,
                medianEnqueueSize,
                percentileEnqueueSize,
                maxEnqueueSize,

                avgDequeueSize,
                medianDequeueSize,
                percentileDequeueSize,
                maxDequeueSize,

                avgNetThreadTime,
                medianNetThreadTime,
                percentileNetThreadTime,

                avgProcessingTime,
                medianProcessingTime,
                percentileProcessingTime,

                avgWorkerTime,
                medianWorkerTime,
                percentileWorkerTime
        );
    }


    public void export(){
        try {
            if (totalJobCount == 0){
                Log.info("[Statistics] Not exporting " + (statisticsType == StatisticsType.SET ? "set" : "get") + " statistics, no data available.");
                return;
            }
            String perJobFileName = "./result/" + Global.launchTime + "_PJ_" + Thread.currentThread().getName() + "_" + (statisticsType == StatisticsType.SET ? "set" : "get" ) + ".stat";
            String perSecondFileName = "./result/" + Global.launchTime + "_PS_" + Thread.currentThread().getName() + "_" + (statisticsType == StatisticsType.SET ? "set" : "get" ) + ".stat";
            String histogramFileName = "./result/" + Global.launchTime + "_HG_" + Thread.currentThread().getName() + "_" + (statisticsType == StatisticsType.SET ? "set" : "get" ) + ".stat";

            // Consolidate here to ensure all data is processed, regardless of output settings
            List<JobStatLine> statLineListPerJob = consolidatePerJob(jobList);
            TimeStatLine[] statLineListPerSecond = consolidatePerSecond(jobList);

            // PER JOB
            if (Parameters.getBoolean("statistics_per_job_export")) {

                PrintWriter perJobPrintWriter = new PrintWriter(perJobFileName, "US-ASCII");

                perJobPrintWriter.print(totalMissCount);
                perJobPrintWriter.print(",");
                perJobPrintWriter.println(totalJobCount);

                perJobPrintWriter.print(errorCount);
                perJobPrintWriter.print(",");
                perJobPrintWriter.print(serverErrorCount);
                perJobPrintWriter.print(",");
                perJobPrintWriter.println(clientErrorCount);

                perJobPrintWriter.print(statLineListPerJob.size());

                for (JobStatLine currentJobStatLine : statLineListPerJob) {
                    perJobPrintWriter.println();
                    perJobPrintWriter.print(currentJobStatLine.id);
                    perJobPrintWriter.print(",");
                    perJobPrintWriter.print(currentJobStatLine.serverArrivalTime.length);
                    perJobPrintWriter.print(",");
                    for (int i = 0; i < currentJobStatLine.serverArrivalTime.length; i++) {
                        perJobPrintWriter.print(currentJobStatLine.serverArrivalTime[i]);
                        perJobPrintWriter.print(",");
                        perJobPrintWriter.print(currentJobStatLine.serverSendTime[i]);
                        perJobPrintWriter.print(",");
                    }

                    perJobPrintWriter.print(currentJobStatLine.clientArrivalTime);
                    perJobPrintWriter.print(",");
                    perJobPrintWriter.print(currentJobStatLine.clientSendTime);
                    perJobPrintWriter.print(",");
                    perJobPrintWriter.print(currentJobStatLine.queueTime);
                    perJobPrintWriter.print(",");

                    perJobPrintWriter.print(currentJobStatLine.enqueueSize);
                    perJobPrintWriter.print(",");
                    perJobPrintWriter.print(currentJobStatLine.dequeueSize);
                }

                perJobPrintWriter.flush();
                perJobPrintWriter.close();
            }

            // PER SECOND
            if (Parameters.getBoolean("statistics_per_second_export")){

                PrintWriter perSecondPrintWriter = new PrintWriter(perSecondFileName, "US-ASCII");

                perSecondPrintWriter.println(statLineListPerSecond.length);

                perSecondPrintWriter.print(totalMissCount);
                perSecondPrintWriter.print(",");
                perSecondPrintWriter.println(totalJobCount);

                perSecondPrintWriter.print(errorCount);
                perSecondPrintWriter.print(",");
                perSecondPrintWriter.print(serverErrorCount);
                perSecondPrintWriter.print(",");
                perSecondPrintWriter.print(clientErrorCount);

                TimeStatLine currentTimeStatLine;
                for (int i = 0; i < statLineListPerSecond.length; i++){
                    currentTimeStatLine = statLineListPerSecond[i];

                    perSecondPrintWriter.println();
                    perSecondPrintWriter.print(i);
                    perSecondPrintWriter.print(",");

                    perSecondPrintWriter.print(currentTimeStatLine.jobsFinished);
                    perSecondPrintWriter.print(",");


                    perSecondPrintWriter.print(currentTimeStatLine.avgServerResponseTime.length);
                    perSecondPrintWriter.print(",");

                    for (int j = 0; j < currentTimeStatLine.avgServerResponseTime.length; j++) {
                        perSecondPrintWriter.print(currentTimeStatLine.avgServerResponseTime[j]);
                        perSecondPrintWriter.print(",");
                        perSecondPrintWriter.print(currentTimeStatLine.medianServerResponseTime[j]);
                        perSecondPrintWriter.print(",");
                        perSecondPrintWriter.print(currentTimeStatLine.percentileServerResponseTime[j]);
                        perSecondPrintWriter.print(",");
                    }

                    perSecondPrintWriter.print(currentTimeStatLine.avgClientResponseTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.medianClientResponseTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.percentileClientResponseTime);
                    perSecondPrintWriter.print(",");

                    perSecondPrintWriter.print(currentTimeStatLine.avgQueueTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.medianQueueTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.percentileQueueTime);
                    perSecondPrintWriter.print(",");

                    perSecondPrintWriter.print(currentTimeStatLine.avgEnqueueSize);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.medianEnqueueSize);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.percentileEnqueueSize);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.maxEnqueueSize);
                    perSecondPrintWriter.print(",");

                    perSecondPrintWriter.print(currentTimeStatLine.avgDequeueSize);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.medianDequeueSize);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.percentileDequeueSize);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.maxDequeueSize);
                    perSecondPrintWriter.print(",");

                    perSecondPrintWriter.print(currentTimeStatLine.avgNetThreadTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.medianNetThreadTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.percentileNetThreadTime);
                    perSecondPrintWriter.print(",");

                    perSecondPrintWriter.print(currentTimeStatLine.avgProcessingTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.medianProcessingTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.percentileProcessingTime);
                    perSecondPrintWriter.print(",");

                    perSecondPrintWriter.print(currentTimeStatLine.avgWorkerTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.medianWorkerTime);
                    perSecondPrintWriter.print(",");
                    perSecondPrintWriter.print(currentTimeStatLine.percentileWorkerTime);
                }

                perSecondPrintWriter.flush();
                perSecondPrintWriter.close();
            }

            // HISTOGRAM
            if (Parameters.getBoolean("statistics_histogram_export")){
                PrintWriter histogramPrintWriter = new PrintWriter(histogramFileName, "US-ASCII");
                histogramPrintWriter.print(histogram.entrySet().size());
                for (Map.Entry<Integer, Integer> entry : histogram.entrySet()) {
                    histogramPrintWriter.println();
                    Integer currentBucket = entry.getKey();
                    Integer currentBucketValue = entry.getValue();
                    histogramPrintWriter.print(currentBucket);
                    histogramPrintWriter.print(",");
                    histogramPrintWriter.print(currentBucketValue);
                }

                histogramPrintWriter.flush();
                histogramPrintWriter.close();
            }

            Log.info("[Statistics] All " + (statisticsType == StatisticsType.SET ? "set" : "get") + " statistics exported successfully");
            return;
        } catch (Exception exception){
            Log.error("[Statistics] Exception during export: " + exception.getMessage());
            Log.error("[Statistics] STACKTRACE: " + stackTraceToString(exception));
            // Don't system.exit, since that will just invoke ShutdownThread and have the system deadlock.
            ShutdownThread.setReturncode(1);
            return;
        }
    }

    public static void exportThinkingTimes(){
        try {
            if (!Parameters.getBoolean("statistics_thinking_times_export")){
                return;
            }
            String thinkingTimesFileName = "./result/" + Global.launchTime + "_TT.stat";

            List<ThinkingTimeStatLine> thinkingTimeStatLineList = consolidateThinkingTimes(thinkingTimesMap);
            PrintWriter thinkingTimesPrintWriter = new PrintWriter(thinkingTimesFileName, "US-ASCII");

            thinkingTimesPrintWriter.print(thinkingTimeStatLineList.size());

            for (ThinkingTimeStatLine currentThinkingTimeStatLine : thinkingTimeStatLineList){
                thinkingTimesPrintWriter.println();
                thinkingTimesPrintWriter.print(currentThinkingTimeStatLine.client);
                thinkingTimesPrintWriter.print(",");
                thinkingTimesPrintWriter.print(currentThinkingTimeStatLine.avgThinkingTime);
                thinkingTimesPrintWriter.print(",");
                thinkingTimesPrintWriter.print(currentThinkingTimeStatLine.medianThinkingTime);
                thinkingTimesPrintWriter.print(",");
                thinkingTimesPrintWriter.print(currentThinkingTimeStatLine.percentileThinkingTime);
            }


            thinkingTimesPrintWriter.flush();
            thinkingTimesPrintWriter.close();

            Log.info("[Statistics] Thinking time statistics exported successfully");

        } catch (Exception exception) {
            Log.error("[Statistics] Exception during export: " + exception.getMessage());
            Log.error("[Statistics] STACKTRACE: " + stackTraceToString(exception));
            // Don't system.exit, since that will just invoke ShutdownThread and have the system deadlock.
            ShutdownThread.setReturncode(1);
            return;
        }
    }

    private static String stackTraceToString(Exception ex){
        StringWriter outError = new StringWriter();
        ex.printStackTrace(new PrintWriter(outError));
        return outError.toString();
    }


    public enum StatisticsType{
        SET, GET
    }

    public static class ThinkingTimeStatLine{

        public final String client;

        public final long avgThinkingTime;
        public final long medianThinkingTime;
        public final long percentileThinkingTime;

        public ThinkingTimeStatLine(String client, long avgThinkingTime, long medianThinkingTime, long percentileThinkingTime){
            this.client = client;
            this.avgThinkingTime = avgThinkingTime;
            this.medianThinkingTime = medianThinkingTime;
            this.percentileThinkingTime = percentileThinkingTime;
        }
    }

    public static class JobStatLine{

        // Keep track of how many lines have been written already (both set and get globally)
        private static volatile Long nextId = 0L;

        private static final Object lock = new Object();

        public final long id;

        public final long[] serverArrivalTime;
        public final long[] serverSendTime;
        public final long clientArrivalTime;
        public final long clientSendTime;
        public final long queueTime;

        public final int enqueueSize;
        public final int dequeueSize;

        public JobStatLine(long[] serverArrivalTime, long[] serverSendTime, long clientArrivalTime, long clientSendTime, long queueTime, int enqueueSize, int dequeueSize){

            synchronized (lock){
                this.id = nextId;
                nextId++;
            }

            this.serverArrivalTime = serverArrivalTime;
            this.serverSendTime = serverSendTime;
            this.clientArrivalTime = clientArrivalTime;
            this.clientSendTime = clientSendTime;
            this.queueTime = queueTime;

            this.enqueueSize = enqueueSize;
            this.dequeueSize = dequeueSize;
        }
    }

    public static class TimeStatLine{

        public final int jobsFinished;

        public final long[] avgServerResponseTime;
        public final long[] medianServerResponseTime;
        public final long[] percentileServerResponseTime;

        public final long avgClientResponseTime;
        public final long medianClientResponseTime;
        public final long percentileClientResponseTime;

        public final long avgQueueTime;
        public final long medianQueueTime;
        public final long percentileQueueTime;

        public final int avgEnqueueSize;
        public final int avgDequeueSize;

        public final int medianEnqueueSize;
        public final int medianDequeueSize;
        public final int percentileEnqueueSize;
        public final int percentileDequeueSize;

        public final int maxEnqueueSize;
        public final int maxDequeueSize;

        public final long avgNetThreadTime;
        public final long medianNetThreadTime;
        public final long percentileNetThreadTime;

        public final long avgProcessingTime;
        public final long medianProcessingTime;
        public final long percentileProcessingTime;

        public final long avgWorkerTime;
        public final long medianWorkerTime;
        public final long percentileWorkerTime;



        public TimeStatLine(int jobsFinished, long[] avgServerResponseTime, long[] medianServerResponseTime, long[] percentileServerResponseTime, long avgClientResponseTime, long medianClientResponseTime, long percentileClientResponseTime, long avgQueueTime, long medianQueueTime, long percentileQueueTime, int avgEnqueueSize, int medianEnqueueSize, int percentileEnqueueSize,
                             int maxEnqueueSize, int avgDequeueSize, int medianDequeueSize, int percentileDequeueSize, int maxDequeueSize,long avgNetThreadTime, long medianNetThreadTime, long percentileNetThreadTime, long avgProcessingTime, long medianProcessingTime, long percentileProcessingTime,long avgWorkerTime, long medianWorkerTime, long percentileWorkerTime){

            this.jobsFinished = jobsFinished;

            this.avgServerResponseTime = avgServerResponseTime;
            this.medianServerResponseTime = medianServerResponseTime;
            this.percentileServerResponseTime = percentileServerResponseTime;

            this.avgClientResponseTime = avgClientResponseTime;
            this.medianClientResponseTime = medianClientResponseTime;
            this.percentileClientResponseTime = percentileClientResponseTime;

            this.avgQueueTime = avgQueueTime;
            this.medianQueueTime = medianQueueTime;
            this.percentileQueueTime = percentileQueueTime;

            this.avgEnqueueSize = avgEnqueueSize;
            this.avgDequeueSize = avgDequeueSize;

            this.medianEnqueueSize = medianEnqueueSize;
            this.percentileEnqueueSize = percentileEnqueueSize;
            this.medianDequeueSize = medianDequeueSize;
            this.percentileDequeueSize = percentileDequeueSize;

            this.maxEnqueueSize = maxEnqueueSize;
            this.maxDequeueSize = maxDequeueSize;

            this.avgNetThreadTime = avgNetThreadTime;
            this.medianNetThreadTime = medianNetThreadTime;
            this.percentileNetThreadTime = percentileNetThreadTime;
            this.avgProcessingTime = avgProcessingTime;
            this.medianProcessingTime = medianProcessingTime;
            this.percentileProcessingTime = percentileProcessingTime;
            this.avgWorkerTime = avgWorkerTime;
            this.medianWorkerTime = medianWorkerTime;
            this.percentileWorkerTime = percentileWorkerTime;
        }
    }


    static Long getLongMedian(List<Long> list){
        Long median;
        if (list.size() == 0){
            return 0L;
        }
        if ((list.size() % 2) == 1){
            median = list.get(list.size() / 2);
        } else {
            Long lowerMedian = list.get((list.size() / 2) - 1);
            Long upperMedian = list.get(list.size() / 2);
            median = (upperMedian + lowerMedian) / 2;
        }
        return median;
    }
    static Integer getIntegerMedian(List<Integer> list){
        Integer median;
        if (list.size() == 0){
            return 0;
        }
        if ((list.size() % 2) == 1){
            median = list.get(list.size() / 2);
        } else {
            Integer lowerMedian = list.get((list.size() / 2) - 1);
            Integer upperMedian = list.get(list.size() / 2);
            median = (upperMedian + lowerMedian) / 2;
        }
        return median;
    }

    static Long getLongPercentile(List<Long> list, double percentile){
        if (list.size() == 0){
            return 0L;
        }
        return list.get((int) (list.size() * percentile));
    }

    static Integer getIntegerPercentile(List<Integer> list, double percentile){
        if (list.size() == 0){
            return 0;
        }
        return list.get((int) (list.size() * percentile));
    }

    static Long getLongMax(List<Long> list){
        Long max = Long.MIN_VALUE;
        if (list.size() == 0){
            return 0L;
        }
        for (Long currentLong : list){
            max = Math.max(max, currentLong);
        }
        return max;
    }

    static Integer getIntegerMax(List<Integer> list){
        Integer max = Integer.MIN_VALUE;
        if (list.size() == 0){
            return 0;
        }
        for (Integer currentInteger : list){
            max = Math.max(max, currentInteger);
        }
        return max;
    }

    static Long getLongAverage(List<Long> list){
        Long sum = 0L;
        if (list.size() == 0){
            return 0L;
        }
        for (Long currentLong : list){
            sum += currentLong;
        }
        return sum / list.size();
    }

    static Integer getIntegerAverage(List<Integer> list){
        Integer sum = 0;
        if (list.size() == 0){
            return 0;
        }
        for (Integer currentInteger : list){
            sum += currentInteger;
        }
        return sum / list.size();
    }


}
