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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.util.Preconditions;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Test {@link LeaderElectionService} implementation which directly forwards isLeader and notLeader
 * calls to the contender.
 */
public class TestingLeaderElectionService extends AbstractLeaderElectionService {

    private String contenderID = null;
    private LeaderContender contender = null;
    private boolean hasLeadership = false;
    private CompletableFuture<LeaderConnectionInfo> confirmationFuture = null;
    private CompletableFuture<Void> startFuture = new CompletableFuture<>();
    private UUID issuedLeaderSessionId = null;

    /**
     * Gets a future that completes when leadership is confirmed.
     *
     * <p>Note: the future is created upon calling {@link #isLeader(UUID)}.
     */
    public synchronized CompletableFuture<LeaderConnectionInfo> getConfirmationFuture() {
        return confirmationFuture;
    }

    @Override
    public void startLeaderElectionBackend() {
        Preconditions.checkState(!getStartFuture().isDone());
        startFuture.complete(null);
    }

    @Override
    public synchronized void register(String contenderID, LeaderContender contender) {
        this.contenderID = contenderID;
        this.contender = contender;

        if (hasLeadership) {
            contender.grantLeadership(issuedLeaderSessionId);
        }
    }

    @Override
    protected synchronized void remove(String contenderID) {
        Preconditions.checkNotNull(contenderID);
        Preconditions.checkArgument(contenderID.equals(this.contenderID));

        if (contender != null) {
            contender.revokeLeadership();
        }

        this.contenderID = null;
        contender = null;
    }

    @Override
    public synchronized void close() throws Exception {
        hasLeadership = false;
        issuedLeaderSessionId = null;
        startFuture.cancel(false);
        startFuture = new CompletableFuture<>();
    }

    @Override
    protected synchronized void confirmLeadership(
            String contenderID, UUID leaderSessionID, String leaderAddress) {
        if (confirmationFuture != null) {
            confirmationFuture.complete(new LeaderConnectionInfo(leaderSessionID, leaderAddress));
        }
    }

    @Override
    protected synchronized boolean hasLeadership(String contenderID, UUID leaderSessionId) {
        return contenderID.equals(this.contenderID)
                && hasLeadership
                && leaderSessionId.equals(issuedLeaderSessionId);
    }

    public synchronized CompletableFuture<UUID> isLeader(UUID leaderSessionID) {
        if (confirmationFuture != null) {
            confirmationFuture.cancel(false);
        }
        confirmationFuture = new CompletableFuture<>();
        hasLeadership = true;
        issuedLeaderSessionId = leaderSessionID;

        if (contender != null) {
            contender.grantLeadership(leaderSessionID);
        }

        return confirmationFuture.thenApply(LeaderConnectionInfo::getLeaderSessionId);
    }

    public synchronized void notLeader() {
        hasLeadership = false;

        if (contender != null) {
            contender.revokeLeadership();
        }
    }

    public synchronized String getAddress() {
        if (confirmationFuture.isDone()) {
            return confirmationFuture.join().getAddress();
        } else {
            throw new IllegalStateException("TestingLeaderElectionService has not been started.");
        }
    }

    /**
     * Returns the start future indicating whether this leader election service has been started or
     * not.
     *
     * @return Future which is completed once this service has been started
     */
    public synchronized CompletableFuture<Void> getStartFuture() {
        return startFuture;
    }

    public synchronized boolean hasContenderRegistered() {
        return contender != null;
    }

    public synchronized boolean isStopped() {
        return !startFuture.isDone();
    }
}
