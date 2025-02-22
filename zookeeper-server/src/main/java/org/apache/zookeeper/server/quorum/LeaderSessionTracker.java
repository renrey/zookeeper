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

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.KeeperException.UnknownSessionException;
import org.apache.zookeeper.server.SessionTrackerImpl;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The leader session tracker tracks local and global sessions on the leader.
 */
public class LeaderSessionTracker extends UpgradeableSessionTracker {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderSessionTracker.class);

    private final SessionTrackerImpl globalSessionTracker;

    /**
     * Server id of the leader
     */
    private final long serverId;

    public LeaderSessionTracker(SessionExpirer expirer, ConcurrentMap<Long, Integer> sessionsWithTimeouts, int tickTime, long id, boolean localSessionsEnabled, ZooKeeperServerListener listener) {

        this.globalSessionTracker = new SessionTrackerImpl(expirer, sessionsWithTimeouts, tickTime, id, listener);

        this.localSessionsEnabled = localSessionsEnabled;
        if (this.localSessionsEnabled) {
            createLocalSessionTracker(expirer, tickTime, id, listener);
        }
        serverId = id;
    }

    public void removeSession(long sessionId) {
        if (localSessionTracker != null) {
            localSessionTracker.removeSession(sessionId);
        }
        globalSessionTracker.removeSession(sessionId);
    }

    public void start() {
        globalSessionTracker.start();
        if (localSessionTracker != null) {
            localSessionTracker.start();
        }
    }

    public void shutdown() {
        if (localSessionTracker != null) {
            localSessionTracker.shutdown();
        }
        globalSessionTracker.shutdown();
    }

    public boolean isGlobalSession(long sessionId) {
        return globalSessionTracker.isTrackingSession(sessionId);
    }

    public boolean trackSession(long sessionId, int sessionTimeout) {
        boolean tracked = globalSessionTracker.trackSession(sessionId, sessionTimeout);
        if (localSessionsEnabled && tracked) {
            // Only do extra logging so we know what kind of session this is
            // if we're supporting both kinds of sessions
            LOG.info("Tracking global session 0x{}", Long.toHexString(sessionId));
        }
        return tracked;
    }

    /**
     * Synchronized on this to avoid race condition of adding a local session
     * after committed global session, which may cause the same session being
     * tracked on this server and leader.
     */
    public synchronized boolean commitSession(
        long sessionId, int sessionTimeout) {
        boolean added = globalSessionTracker.commitSession(sessionId, sessionTimeout);

        if (added) {
            LOG.info("Committing global session 0x{}", Long.toHexString(sessionId));
        }

        // If the session moved before the session upgrade finished, it's
        // possible that the session will be added to the local session
        // again. Need to double check and remove it from local session
        // tracker when the global session is quorum committed, otherwise the
        // local session might be tracked both locally and on leader.
        //
        // This cannot totally avoid the local session being upgraded again
        // because there is still race condition between create another upgrade
        // request and process the createSession commit, and there is no way
        // to know there is a on flying createSession request because it might
        // be upgraded by other server which owns the session before move.
        if (localSessionsEnabled) {
            removeLocalSession(sessionId);
            finishedUpgrading(sessionId);
        }

        return added;
    }

    public boolean touchSession(long sessionId, int sessionTimeout) {
        if (localSessionTracker != null && localSessionTracker.touchSession(sessionId, sessionTimeout)) {
            return true;
        }
        return globalSessionTracker.touchSession(sessionId, sessionTimeout);
    }

    public long createSession(int sessionTimeout) {
        // 一般没开启
        if (localSessionsEnabled) {
            return localSessionTracker.createSession(sessionTimeout);
        }
        // 正常，创建session，就是id自增
        // 初始id为当前时间<<24>>>8 ｜ myid << 56
        return globalSessionTracker.createSession(sessionTimeout);
    }

    // Returns the serverId from the sessionId (the high order byte)
    public static long getServerIdFromSessionId(long sessionId) {
        return sessionId >> 56;
    }

    public void checkSession(long sessionId, Object owner) throws SessionExpiredException, SessionMovedException, UnknownSessionException {
        if (localSessionTracker != null) {
            try {
                localSessionTracker.checkSession(sessionId, owner);
                // A session can both be a local and global session during
                // upgrade
                if (!isGlobalSession(sessionId)) {
                    return;
                }
            } catch (UnknownSessionException e) {
                // Ignore. We'll check instead whether it's a global session
            }
        }
        try {
            globalSessionTracker.checkSession(sessionId, owner);
            // if we can get here, it is a valid global session
            return;
        } catch (UnknownSessionException e) {
            // Ignore. This may be local session from other servers.
        }

        /*
         * if local session is not enabled or it used to be our local session
         * throw sessions expires
         */
        if (!localSessionsEnabled || (getServerIdFromSessionId(sessionId) == serverId)) {
            throw new SessionExpiredException();
        }
    }

    public void checkGlobalSession(long sessionId, Object owner) throws SessionExpiredException, SessionMovedException {
        try {
            globalSessionTracker.checkSession(sessionId, owner);
        } catch (UnknownSessionException e) {
            // For global session, if we don't know it, it is already expired
            throw new SessionExpiredException();
        }
    }

    public void setOwner(long sessionId, Object owner) throws SessionExpiredException {
        if (localSessionTracker != null) {
            try {
                localSessionTracker.setOwner(sessionId, owner);
                return;
            } catch (SessionExpiredException e) {
                // Ignore. We'll check instead whether it's a global session
            }
        }
        globalSessionTracker.setOwner(sessionId, owner);
    }

    public void dumpSessions(PrintWriter pwriter) {
        if (localSessionTracker != null) {
            pwriter.print("Local ");
            localSessionTracker.dumpSessions(pwriter);
            pwriter.print("Global ");
        }
        globalSessionTracker.dumpSessions(pwriter);
    }

    public void setSessionClosing(long sessionId) {
        // call is no-op if session isn't tracked so safe to call both
        if (localSessionTracker != null) {
            localSessionTracker.setSessionClosing(sessionId);
        }
        globalSessionTracker.setSessionClosing(sessionId);
    }

    public Map<Long, Set<Long>> getSessionExpiryMap() {
        Map<Long, Set<Long>> sessionExpiryMap;
        // combine local and global sessions, getting local first so upgrades
        // to global are caught
        if (localSessionTracker != null) {
            sessionExpiryMap = localSessionTracker.getSessionExpiryMap();
        } else {
            sessionExpiryMap = new TreeMap<Long, Set<Long>>();
        }
        sessionExpiryMap.putAll(globalSessionTracker.getSessionExpiryMap());
        return sessionExpiryMap;
    }

    public Set<Long> globalSessions() {
        return globalSessionTracker.globalSessions();
    }
}
