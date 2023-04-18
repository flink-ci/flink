/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Default {@link AllocatedSlotPool} implementation. */
public class DefaultAllocatedSlotPool implements AllocatedSlotPool {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultAllocatedSlotPool.class);

    private final Map<AllocationID, AllocatedSlot> registeredSlots;

    /** All free slots and since when they are free, index by TaskExecutor. */
    private final FreeSlots freeSlots;

    /** Index containing a mapping between TaskExecutors and their slots. */
    private final Map<ResourceID, Set<AllocationID>> slotsPerTaskExecutor;

    public DefaultAllocatedSlotPool() {
        this.registeredSlots = new HashMap<>();
        this.slotsPerTaskExecutor = new HashMap<>();
        this.freeSlots = new FreeSlots();
    }

    @Override
    public void addSlots(Collection<AllocatedSlot> slots, long currentTime) {
        for (AllocatedSlot slot : slots) {
            addSlot(slot, currentTime);
        }
    }

    private void addSlot(AllocatedSlot slot, long currentTime) {
        Preconditions.checkState(
                !registeredSlots.containsKey(slot.getAllocationId()),
                "The slot pool already contains a slot with id %s",
                slot.getAllocationId());
        addSlotInternal(slot, currentTime);

        slotsPerTaskExecutor
                .computeIfAbsent(slot.getTaskManagerId(), resourceID -> new HashSet<>())
                .add(slot.getAllocationId());
    }

    private void addSlotInternal(AllocatedSlot slot, long currentTime) {
        registeredSlots.put(slot.getAllocationId(), slot);
        freeSlots.addFreeSlot(slot, currentTime);
    }

    @Override
    public Optional<AllocatedSlot> removeSlot(AllocationID allocationId) {
        final AllocatedSlot removedSlot = removeSlotInternal(allocationId);

        if (removedSlot != null) {
            final ResourceID owner = removedSlot.getTaskManagerId();

            slotsPerTaskExecutor.computeIfPresent(
                    owner,
                    (resourceID, allocationIds) -> {
                        allocationIds.remove(allocationId);

                        if (allocationIds.isEmpty()) {
                            return null;
                        }

                        return allocationIds;
                    });

            return Optional.of(removedSlot);
        } else {
            return Optional.empty();
        }
    }

    @Nullable
    private AllocatedSlot removeSlotInternal(AllocationID allocationId) {
        final AllocatedSlot removedSlot = registeredSlots.remove(allocationId);
        if (removedSlot != null) {
            freeSlots.removeFreeSlot(removedSlot);
        }
        return removedSlot;
    }

    @Override
    public AllocatedSlotsAndReservationStatus removeSlots(ResourceID owner) {
        final Set<AllocationID> slotsOfTaskExecutor = slotsPerTaskExecutor.remove(owner);

        if (slotsOfTaskExecutor != null) {
            final Collection<AllocatedSlot> removedSlots = new ArrayList<>();
            final Map<AllocationID, ReservationStatus> removedSlotsReservationStatus =
                    new HashMap<>();

            for (AllocationID allocationId : slotsOfTaskExecutor) {
                final ReservationStatus reservationStatus =
                        containsFreeSlot(allocationId)
                                ? ReservationStatus.FREE
                                : ReservationStatus.RESERVED;

                final AllocatedSlot removedSlot =
                        Preconditions.checkNotNull(removeSlotInternal(allocationId));
                removedSlots.add(removedSlot);
                removedSlotsReservationStatus.put(removedSlot.getAllocationId(), reservationStatus);
            }

            return new DefaultAllocatedSlotsAndReservationStatus(
                    removedSlots, removedSlotsReservationStatus);
        } else {
            return new DefaultAllocatedSlotsAndReservationStatus(
                    Collections.emptyList(), Collections.emptyMap());
        }
    }

    @Override
    public boolean containsSlots(ResourceID owner) {
        return slotsPerTaskExecutor.containsKey(owner);
    }

    @Override
    public boolean containsSlot(AllocationID allocationId) {
        return registeredSlots.containsKey(allocationId);
    }

    @Override
    public boolean containsFreeSlot(AllocationID allocationId) {
        return freeSlots.contains(allocationId);
    }

    @Override
    public AllocatedSlot reserveFreeSlot(AllocationID allocationId) {
        LOG.debug("Reserve free slot with allocation id {}.", allocationId);
        AllocatedSlot slot = registeredSlots.get(allocationId);
        Preconditions.checkNotNull(slot, "The slot with id %s was not exists.", allocationId);
        Preconditions.checkState(
                freeSlots.removeFreeSlot(slot) != null,
                "The slot with id %s was not free.",
                allocationId);
        return registeredSlots.get(allocationId);
    }

    @Override
    public Optional<AllocatedSlot> freeReservedSlot(AllocationID allocationId, long currentTime) {
        final AllocatedSlot allocatedSlot = registeredSlots.get(allocationId);

        if (allocatedSlot != null && !freeSlots.contains(allocationId)) {
            freeSlots.addFreeSlot(allocatedSlot, currentTime);
            return Optional.of(allocatedSlot);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<SlotInfo> getSlotInformation(AllocationID allocationID) {
        return Optional.ofNullable(registeredSlots.get(allocationID));
    }

    @Override
    public Collection<FreeSlotInfo> getFreeSlotsInformation() {
        final Collection<FreeSlotInfo> freeSlotInfos = new ArrayList<>();

        for (Map.Entry<AllocationID, Long> freeSlot : freeSlots.getFreeSlotsSince().entrySet()) {
            final AllocatedSlot allocatedSlot =
                    Preconditions.checkNotNull(registeredSlots.get(freeSlot.getKey()));

            final ResourceID owner = allocatedSlot.getTaskManagerId();
            final int numberOfSlotsOnOwner = slotsPerTaskExecutor.get(owner).size();
            final int numberOfFreeSlotsOnOwner = freeSlots.getFreeSlotsNumberOfTaskExecutor(owner);
            final double taskExecutorUtilization =
                    (double) (numberOfSlotsOnOwner - numberOfFreeSlotsOnOwner)
                            / numberOfSlotsOnOwner;

            final SlotInfoWithUtilization slotInfoWithUtilization =
                    SlotInfoWithUtilization.from(allocatedSlot, taskExecutorUtilization);

            freeSlotInfos.add(
                    DefaultFreeSlotInfo.create(slotInfoWithUtilization, freeSlot.getValue()));
        }

        return freeSlotInfos;
    }

    @Override
    public Collection<? extends SlotInfo> getAllSlotsInformation() {
        return registeredSlots.values();
    }

    private static final class FreeSlots {
        /** Map containing all free slots and since when they are free. */
        private final Map<AllocationID, Long> freeSlotsSince = new HashMap<>();

        /** Index containing a mapping between TaskExecutors and their free slots number. */
        private final Map<ResourceID, Integer> freeSlotsNumberPerTaskExecutor = new HashMap<>();

        public void addFreeSlot(AllocatedSlot slot, long currentTime) {
            Preconditions.checkState(
                    !freeSlotsSince.containsKey(slot.getAllocationId()),
                    "Slot with id %s has been freed",
                    slot.getAllocationId());
            freeSlotsSince.put(slot.getAllocationId(), currentTime);
            freeSlotsNumberPerTaskExecutor.put(
                    slot.getTaskManagerId(),
                    freeSlotsNumberPerTaskExecutor.getOrDefault(slot.getTaskManagerId(), 0) + 1);
        }

        public Long removeFreeSlot(AllocatedSlot slot) {
            if (freeSlotsSince.containsKey(slot.getAllocationId())) {
                freeSlotsNumberPerTaskExecutor.put(
                        slot.getTaskManagerId(),
                        freeSlotsNumberPerTaskExecutor.get(slot.getTaskManagerId()) - 1);
            }
            return freeSlotsSince.remove(slot.getAllocationId());
        }

        public boolean contains(AllocationID allocationId) {
            return freeSlotsSince.containsKey(allocationId);
        }

        public int getFreeSlotsNumberOfTaskExecutor(ResourceID resourceId) {
            return freeSlotsNumberPerTaskExecutor.getOrDefault(resourceId, 0);
        }

        public Map<AllocationID, Long> getFreeSlotsSince() {
            return freeSlotsSince;
        }
    }

    private static final class DefaultFreeSlotInfo implements AllocatedSlotPool.FreeSlotInfo {

        private final SlotInfoWithUtilization slotInfoWithUtilization;

        private final long freeSince;

        private DefaultFreeSlotInfo(
                SlotInfoWithUtilization slotInfoWithUtilization, long freeSince) {
            this.slotInfoWithUtilization = slotInfoWithUtilization;
            this.freeSince = freeSince;
        }

        @Override
        public SlotInfoWithUtilization asSlotInfo() {
            return slotInfoWithUtilization;
        }

        @Override
        public long getFreeSince() {
            return freeSince;
        }

        private static DefaultFreeSlotInfo create(
                SlotInfoWithUtilization slotInfoWithUtilization, long idleSince) {
            return new DefaultFreeSlotInfo(
                    Preconditions.checkNotNull(slotInfoWithUtilization), idleSince);
        }
    }

    private static final class DefaultAllocatedSlotsAndReservationStatus
            implements AllocatedSlotsAndReservationStatus {

        private final Collection<AllocatedSlot> slots;
        private final Map<AllocationID, ReservationStatus> reservationStatus;

        private DefaultAllocatedSlotsAndReservationStatus(
                Collection<AllocatedSlot> slots,
                Map<AllocationID, ReservationStatus> reservationStatus) {
            this.slots = slots;
            this.reservationStatus = reservationStatus;
        }

        @Override
        public boolean wasFree(AllocationID allocatedSlot) {
            return reservationStatus.get(allocatedSlot) == ReservationStatus.FREE;
        }

        @Override
        public Collection<AllocatedSlot> getAllocatedSlots() {
            return slots;
        }
    }

    private enum ReservationStatus {
        FREE,
        RESERVED
    }
}
