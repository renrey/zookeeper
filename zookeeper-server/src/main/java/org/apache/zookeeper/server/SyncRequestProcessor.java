/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.quorum.SendAckRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);

    private static final Request REQUEST_OF_DEATH = Request.requestOfDeath;

    /** The number of log entries to log before starting a snapshot */
    private static int snapCount = ZooKeeperServer.getSnapCount();

    /**
     * The total size of log entries before starting a snapshot
     */
    private static long snapSizeInBytes = ZooKeeperServer.getSnapSizeInBytes();

    /**
     * Random numbers used to vary snapshot timing
     */
    private int randRoll;
    private long randSize;

    private final BlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    private final Semaphore snapThreadMutex = new Semaphore(1);

    private final ZooKeeperServer zks;

    private final RequestProcessor nextProcessor;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    private final Queue<Request> toFlush;
    private long lastFlushTime;

    public SyncRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        this.toFlush = new ArrayDeque<>(zks.getMaxBatchSize());
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    private long getRemainingDelay() {
        long flushDelay = zks.getFlushDelay();
        long duration = Time.currentElapsedTime() - lastFlushTime;
        // 如果没配置flushDelay，只会返回0
        if (duration < flushDelay) {
            return flushDelay - duration;
        }
        return 0;
    }

    /** If both flushDelay and maxMaxBatchSize are set (bigger than 0), flush
     * whenever either condition is hit. If only one or the other is
     * set, flush only when the relevant condition is hit.
     */
    private boolean shouldFlush() {
        // 延迟时间，默认0无
        long flushDelay = zks.getFlushDelay();
        // 一次执行的个数，默认1000
        long maxBatchSize = zks.getMaxBatchSize();
        // 是否到时间, 但是默认没有设置flushDelay
        if ((flushDelay > 0) && (getRemainingDelay() == 0)) {
            return true;
        }
        // 大小是否够
        return (maxBatchSize > 0) && (toFlush.size() >= maxBatchSize);
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param size
     */
    public static void setSnapSizeInBytes(long size) {
        snapSizeInBytes = size;
    }

    private boolean shouldSnapshot() {
        // 当前db使用的log文件的txn请求 数量
        int logCount = zks.getZKDatabase().getTxnCount();
        // 当前log文件大小
        long logSize = zks.getZKDatabase().getTxnSize();
        /**
         * 符合任一条件就能快照
         * 1。txn请求数量 > （SNAPCOUNT /2 + 随机数） ， SNAPCOUNT默认100000，随机数是SNAPCOUNT /2以内的，就是不超过SNAPCOUNT
         * 2。log文件大小 >  (snapSizeInBytes/2 + 随机数) snapSizeInBytes默认4GB，随机数也是snapSizeInBytes/2以内的
         */
        return (logCount > (snapCount / 2 + randRoll))
               || (snapSizeInBytes > 0 && logSize > (snapSizeInBytes / 2 + randSize));
    }

    private void resetSnapshotStats() {
        randRoll = ThreadLocalRandom.current().nextInt(snapCount / 2);
        randSize = Math.abs(ThreadLocalRandom.current().nextLong() % (snapSizeInBytes / 2));
    }

    @Override
    public void run() {
        try {
            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            resetSnapshotStats();
            lastFlushTime = Time.currentElapsedTime();
            while (true) {
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_SIZE.add(queuedRequests.size());

                // 依赖于flushDelay，但是默认没配置，所以pollTime=0
                // RemainingDelay: 上次flush的时间点到flushDelay还有多久
                long pollTime = Math.min(zks.getMaxWriteQueuePollTime(), getRemainingDelay());
                /**
                 * 有时间的阻塞获取，主要为了加入请求后，超时flush
                 * 但默认是没有flushDelay，所以这里直接获取
                 */
                Request si = queuedRequests.poll(pollTime, TimeUnit.MILLISECONDS);

                if (si == null) {
                    /* We timed out looking for more writes to batch, go ahead and flush immediately */
                    /**
                     * 超时刷盘：
                     * 万一超过pollTime，没有其他请求提交到队列，需要刷盘flush，直接flush刷盘
                     * 但是默认没开启超时，所以如果没有连续log写入，就直接刷盘了
                     */
                    flush();
                    /**
                     * 阻塞获取
                      */
                    si = queuedRequests.take();
                }

                if (si == REQUEST_OF_DEATH) {
                    break;
                }

                long startProcessTime = Time.currentElapsedTime();
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_TIME.add(startProcessTime - si.syncQueueStartTime);

                // track the number of records written to the log
                /**
                 * append：尝试向本地的db新增这条请求日志(把请求并写入当前log文件os cache，并未刷盘，如果当前未有指定的log文件，创建log.zxid的log文件，)
                 *
                 * 如果连续写入，是不会创建新的log文件，而是继续写在原来的log
                  */
                if (!si.isThrottled() && zks.getZKDatabase().append(si)) {
                    // 写入log的字节流后，判断是否需要生成快照
                    if (shouldSnapshot()) {
                        // 重新生成快照的随机数参数
                        resetSnapshotStats();
                        // roll the log
                        /**
                         * 需要生成快照，把当前log的缓存先flush到页缓存，对这个log的流引用清除
                         * rollLog就是需要创建新的log文件前，对现有log的处理工作,代表需要新建log
                         * 切换log文件的前置！！！
                         */
                        zks.getZKDatabase().rollLog();
                        // take a snapshot
                        /**
                         * 启动一个线程去生成快照，如果有线程在生成，就不会执行
                         */
                        if (!snapThreadMutex.tryAcquire()) {
                            LOG.warn("Too busy to snap, skipping");
                        } else {
                            new ZooKeeperThread("Snapshot Thread") {
                                public void run() {
                                    try {
                                        zks.takeSnapshot();
                                    } catch (Exception e) {
                                        LOG.warn("Unexpected exception", e);
                                    } finally {
                                        snapThreadMutex.release();
                                    }
                                }
                            }.start();
                        }
                    }
                    /**
                     * read请求（append返回false）且前面没有待flush的write(toFlush.isEmpty())
                     * 为啥判断有没有待flush？为了请求顺序执行，flush执行了，才会执行读
                     *
                     * 可忽略：leader在Proposal时已判断非write不会进入，follower进来这里前处理的就是write
                     */
                } else if (toFlush.isEmpty()) {
                    // optimization for read heavy workloads
                    // iff this is a read or a throttled request(which doesn't need to be written to the disk),
                    // and there are no pending flushes (writes), then just pass this to the next processor
                    /**
                     * follower
                     *
                     * @see SendAckRequestProcessor#processRequest(org.apache.zookeeper.server.Request)
                     */
                    if (nextProcessor != null) {
                        nextProcessor.processRequest(si);
                        if (nextProcessor instanceof Flushable) {
                            ((Flushable) nextProcessor).flush();
                        }
                    }
                    continue;
                }
                /**
                 * 放入toFlush (待刷盘的请求队列) ：
                 * 1. write请求
                 * 2. 前面有待flush的read（保证执行顺序）
                 */
                toFlush.add(si);
                /**
                 * 判断是否可以flush刷盘(请求内容刷盘)：
                 * 1. toFlush请求数量等于1000
                 * 2. 是否超时（但默认没开启）
                 */
                if (shouldFlush()) {
                    flush();
                }
                /**
                 * 因此，默认触发刷盘有2种情况：
                 * 1. 连续进行1000条请求写入
                 * 2. 写入一条log到缓冲区后，没有连续的log，就直接flush了
                 */
                ServerMetrics.getMetrics().SYNC_PROCESS_TIME.add(Time.currentElapsedTime() - startProcessTime);
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush() throws IOException, RequestProcessorException {
        if (this.toFlush.isEmpty()) {
            return;
        }

        ServerMetrics.getMetrics().BATCH_SIZE.add(toFlush.size());

        long flushStartTime = Time.currentElapsedTime();
        /**
         * 1. 把toFlush的log原始文件流强制刷盘（fsync），确保文件内容已写入磁盘
         * 通过FileChannel.force实现
         */
        zks.getZKDatabase().commit();
        ServerMetrics.getMetrics().SYNC_PROCESSOR_FLUSH_TIME.add(Time.currentElapsedTime() - flushStartTime);

        if (this.nextProcessor == null) {
            this.toFlush.clear();
        } else {
            /**
             * 对每个已经刷盘的请求Proposal按顺序执行下一个Processor
             *
             * leader：
             * 实际在ProposalProcessor定义的
             * 作用是在log刷盘后，往当前Proposal的ackset添加本节点的ack（leader的ack），代表log写入完成
             * @see org.apache.zookeeper.server.quorum.AckRequestProcessor#processRequest(org.apache.zookeeper.server.Request)
             * follower
             * 发送ack请求给leader
             * @see SendAckRequestProcessor#processRequest(org.apache.zookeeper.server.Request)
             */
            while (!this.toFlush.isEmpty()) {
                final Request i = this.toFlush.remove();
                long latency = Time.currentElapsedTime() - i.syncQueueStartTime;
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_AND_FLUSH_TIME.add(latency);
                this.nextProcessor.processRequest(i);
            }
            if (this.nextProcessor instanceof Flushable) {
                ((Flushable) this.nextProcessor).flush();
            }
        }
        lastFlushTime = Time.currentElapsedTime();
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(REQUEST_OF_DEATH);
        try {
            this.join();
            this.flush();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for {} to finish", this);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(final Request request) {
        Objects.requireNonNull(request, "Request cannot be null");

        request.syncQueueStartTime = Time.currentElapsedTime();

        /**
         * request提交到自己队列中,等待自己的线程执行
         * 主要是把这个请求刷盘
         * @see SyncRequestProcessor#run()
         */
        queuedRequests.add(request);
        ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUED.add(1);
    }

}
