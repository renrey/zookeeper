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

package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and
 * SyncRequestProcessor.
 */
public class ProposalRequestProcessor implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ProposalRequestProcessor.class);

    LeaderZooKeeperServer zks;

    RequestProcessor nextProcessor;

    SyncRequestProcessor syncProcessor;

    // If this property is set, requests from Learners won't be forwarded
    // to the CommitProcessor in order to save resources
    public static final String FORWARD_LEARNER_REQUESTS_TO_COMMIT_PROCESSOR_DISABLED =
          "zookeeper.forward_learner_requests_to_commit_processor_disabled";
    private final boolean forwardLearnerRequestsToCommitProcessorDisabled;

    public ProposalRequestProcessor(LeaderZooKeeperServer zks, RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        // SyncRequestProcessor -> AckRequestProcessor
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);

        forwardLearnerRequestsToCommitProcessorDisabled = Boolean.getBoolean(
                FORWARD_LEARNER_REQUESTS_TO_COMMIT_PROCESSOR_DISABLED);
        LOG.info("{} = {}", FORWARD_LEARNER_REQUESTS_TO_COMMIT_PROCESSOR_DISABLED,
                forwardLearnerRequestsToCommitProcessorDisabled);
    }

    /**
     * initialize this processor
     */
    public void initialize() {
        syncProcessor.start();
    }

    public void processRequest(Request request) throws RequestProcessorException {
        /* In the following IF-THEN-ELSE block, we process syncs on the leader.
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't
         * contain the handler. In this case, we add it to syncHandler, and
         * call processRequest on the next processor.
         */
        // sync操作（一般cli执行的）
        if (request instanceof LearnerSyncRequest) {
            zks.getLeader().processSync((LearnerSyncRequest) request);
        } else {
            // 默认true会执行
            /**
             * 先执行下一个, 这里主要是把请求提交到commitProcessor的queuedRequests队列就回来了，而这条链路需要等到过半ack才会有下文
             * @see CommitProcessor#processRequest(org.apache.zookeeper.server.Request)
             */
            if (shouldForwardToNextProcessor(request)) {
                nextProcessor.processRequest(request);
            }
            /**
             *   实际流程如下
             *    1. CommitProcessor 提交到队列
             *    2. ProposalRequestProcessor.propose 给follower发送PROPOSAL
             *    3. syncProcessor(请求log写入磁盘) -> AckRequestProcessor(加入ackset中)
             *    如果过半后：
             *    tryCommit （发送commit请求给所有follower、请求放到commit的队列） -> CommitProcessor(拿到已认为commit的请求)
             *    -> ToApplied（删掉待执行的proposal） -> final(zk内存数据库处理)
             */
            if (request.getHdr() != null) {
                // We need to sync and get consensus on any transactions
                try {
                    /**
                     * 注意：只有write请求才能发起proposal
                     * 创建proposal（注意zxid在上一步PreRequestProcessor已经生成）, 向其他follower发送PROPOSAL请求
                      */
                    zks.getLeader().propose(request);
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }
                // 使用 syncProcessor 请求写入磁盘
                syncProcessor.processRequest(request);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

    private boolean shouldForwardToNextProcessor(Request request) {
        // 默认为false，即返回true
        if (!forwardLearnerRequestsToCommitProcessorDisabled) {
            return true;
        }
        if (request.getOwner() instanceof LearnerHandler) {
            ServerMetrics.getMetrics().REQUESTS_NOT_FORWARDED_TO_COMMIT_PROCESSOR.add(1);
            return false;
        }
        return true;
    }
}
