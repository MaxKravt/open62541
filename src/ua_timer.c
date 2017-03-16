/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#include "ua_util.h"
#include "ua_timer.h"

/* Only one thread may traverse the lists. This is usually the "main" thread
 * with the event loop. All other threads may add and remove repeated jobs by
 * adding entries to the beginning of the addRemoveJobs list (with atomic
 * operations).
 *
 * Adding repeated jobs: Add an entry with the "nextTime" timestamp in the
 * future. This will be picked up in the next traversal and inserted at the
 * correct place. So that the next execution takes place ät "nextTime".
 *
 * Removing a repeated job: Add an entry with the "nextTime" timestamp set to
 * UA_INT64_MAX. The next iteration picks this up and removes the repated job
 * from the linked list. */

struct UA_RepeatedJob;
typedef struct UA_RepeatedJob UA_RepeatedJob;

struct UA_RepeatedJob {
    SLIST_ENTRY(UA_RepeatedJob) next; /* Next element in the list */
    UA_DateTime nextTime;             /* The next time when the jobs are to be executed */
    UA_UInt64 interval;               /* Interval in 100ns resolution */
    UA_Guid id;                       /* Id of the repeated job */
    UA_Job job;                       /* The job description itself */
    UA_Boolean removeAfterExecution;  /* The job should be removed after execution */
};

void
UA_RepeatedJobsList_init(UA_RepeatedJobsList *rjl,
                         UA_RepeatedJobsListProcessCallback processCallback,
                         void *processContext) {
    SLIST_INIT(&rjl->repeatedJobs);
    SLIST_INIT(&rjl->addRemoveJobs);
    rjl->processCallback = processCallback;
    rjl->processContext = processContext;
}

static UA_StatusCode
createRepeatedJob(UA_RepeatedJobsList *rjl, const UA_Job job,
                  const UA_UInt32 interval, UA_Guid *newJobId, const UA_Guid *existingJobId, UA_Boolean removeAfterExecution) {
    /* The interval needs to be at least 5ms */
    if(interval < 5)
        return UA_STATUSCODE_BADINTERNALERROR;

    /* Allocate the repeated job structure */
    UA_RepeatedJob *rj = (UA_RepeatedJob*)UA_malloc(sizeof(UA_RepeatedJob));
    if(!rj)
        return UA_STATUSCODE_BADOUTOFMEMORY;

    /* Set the repeated job */
    rj->interval = (UA_UInt64)interval * (UA_UInt64)UA_MSEC_TO_DATETIME;
    if (existingJobId == NULL) {
        rj->id = UA_Guid_random();
        /* Set the output guid */
        if (newJobId)
            *newJobId = rj->id;
    }
    else
        rj->id = *existingJobId;
    rj->job = job;
    rj->nextTime = UA_DateTime_nowMonotonic() + (UA_DateTime)rj->interval;
    rj->removeAfterExecution = removeAfterExecution;


    /* Insert the element to the linked list */
    UA_RepeatedJob *currentFirst;
    do {
        currentFirst = SLIST_FIRST(&rjl->addRemoveJobs);
        SLIST_NEXT(rj, next) = currentFirst;
    } while(UA_atomic_cmpxchg((void**)&SLIST_FIRST(&rjl->addRemoveJobs), currentFirst, rj) != currentFirst);

    return UA_STATUSCODE_GOOD;
}

UA_StatusCode
UA_RepeatedJobsList_addRepeatedJob(UA_RepeatedJobsList *rjl, const UA_Job job,
                                   const UA_UInt32 interval, UA_Guid *jobId) {
    return createRepeatedJob(rjl, job, interval, jobId, NULL, UA_FALSE);
}

UA_StatusCode
UA_RepeatedJobsList_addDelayedJob(UA_RepeatedJobsList *rjl, const UA_Job job,
                                   const UA_UInt32 delay, UA_Guid *jobId) {
    return createRepeatedJob(rjl, job, delay, jobId, NULL, UA_TRUE);
}

UA_StatusCode
UA_RepeatedJobsList_removeRepeatedJob(UA_RepeatedJobsList *rjl, const UA_Guid jobId) {
    /* Allocate the repeated job structure */
    UA_RepeatedJob *rj = (UA_RepeatedJob*)UA_malloc(sizeof(UA_RepeatedJob));
    if(!rj)
        return UA_STATUSCODE_BADOUTOFMEMORY;

    /* Set the repeated job with the sentinel nextTime */
    rj->id = jobId;
    rj->nextTime = UA_INT64_MAX;

    /* Insert the element to the linked list */
    UA_RepeatedJob *currentFirst;
    do {
        currentFirst = SLIST_FIRST(&rjl->addRemoveJobs);
        SLIST_NEXT(rj, next) = currentFirst;
    } while(UA_atomic_cmpxchg((void**)&SLIST_FIRST(&rjl->addRemoveJobs), currentFirst, rj) != currentFirst);

    return UA_STATUSCODE_GOOD;
}


UA_StatusCode
UA_RepeatedJobsList_updateRepeatedJobInterval(UA_RepeatedJobsList *rjl, const UA_Guid jobId, const UA_UInt32 newInterval) {

    // if the job to be updated is still in the addRemove list, we just update the interval there

    UA_RepeatedJob *rj = NULL;
    SLIST_FOREACH(rj, &rjl->addRemoveJobs, next) {
        if(UA_Guid_equal(&jobId, &rj->id)) {
            rj->interval  = (UA_UInt64)newInterval * (UA_UInt64)UA_MSEC_TO_DATETIME;
            rj->nextTime = UA_DateTime_nowMonotonic() + (UA_DateTime)rj->interval;
            return UA_STATUSCODE_GOOD;
        }
    }

    // if the job is already in the repeatedJobs list, we need to remove the job and readd a new one to make sure
    // the updated job is inserted at the correct position. This has to be done in a thread-safe way, so we need to
    // use the addRemoveJobs list, instead of tampering with the repeatedJobs list.

    // find the job
    SLIST_FOREACH(rj, &rjl->repeatedJobs, next) {
        if(UA_Guid_equal(&jobId, &rj->id)) {
            break;
        }
    }

    if (!rj)
        return UA_STATUSCODE_BADNOTFOUND;

    UA_StatusCode retVal = createRepeatedJob(rjl, rj->job, newInterval, NULL, &jobId, UA_FALSE);
    if (retVal != UA_STATUSCODE_GOOD)
        return retVal;

    return UA_RepeatedJobsList_removeRepeatedJob(rjl, jobId);
}

static void
insertRepeatedJob(UA_RepeatedJobsList *rjl,
                  UA_RepeatedJob * UA_RESTRICT rj,
                  UA_DateTime nowMonotonic) {
    /* The latest time for the first execution */
    rj->nextTime = nowMonotonic + (UA_Int64)rj->interval;

    /* Find the last entry before this job */
    UA_RepeatedJob *tmpRj, *afterRj = NULL;
    SLIST_FOREACH(tmpRj, &rjl->repeatedJobs, next) {
        if(tmpRj->nextTime >= rj->nextTime)
            break;
        afterRj = tmpRj;

        /* The goal is to have many repeated jobs with the same repetition
         * interval in a "block" in order to reduce linear search for re-entry
         * to the sorted list after processing. Allow the first execution to lie
         * between "nextTime - 1s" and "nextTime" if this adjustment groups jobs
         * with the same repetition interval. */
        if(tmpRj->interval == rj->interval &&
           tmpRj->nextTime > (rj->nextTime - UA_SEC_TO_DATETIME))
            rj->nextTime = tmpRj->nextTime;
    }

    /* Add the repeated job */
    if(afterRj)
        SLIST_INSERT_AFTER(afterRj, rj, next);
    else
        SLIST_INSERT_HEAD(&rjl->repeatedJobs, rj, next);
}

static void
removeRepeatedJob(UA_RepeatedJobsList *rjl, const UA_Guid *jobId) {
    UA_RepeatedJob *rj, *prev = NULL;
    SLIST_FOREACH(rj, &rjl->repeatedJobs, next) {
        if(UA_Guid_equal(jobId, &rj->id)) {
            if(prev)
                SLIST_REMOVE_AFTER(prev, next);
            else
                SLIST_REMOVE_HEAD(&rjl->repeatedJobs, next);
            UA_free(rj);
            break;
        }
        prev = rj;
    }
}

static void
processAddRemoveJobs(UA_RepeatedJobsList *rjl, UA_DateTime nowMonotonic) {
    UA_RepeatedJob *current;
    while((current = SLIST_FIRST(&rjl->addRemoveJobs))) {
        SLIST_REMOVE_HEAD(&rjl->addRemoveJobs, next);
        if(current->nextTime < UA_INT64_MAX) {
            insertRepeatedJob(rjl, current, nowMonotonic);
        } else {
            removeRepeatedJob(rjl, &current->id);
            UA_free(current);
        }
    }
}

UA_DateTime
UA_RepeatedJobsList_process(UA_RepeatedJobsList *rjl,
                            UA_DateTime nowMonotonic,
                            UA_Boolean *dispatched) {
    /* Insert and remove jobs */
    processAddRemoveJobs(rjl, nowMonotonic);

    /* Find the last job to be executed now */
    UA_RepeatedJob *firstAfter, *lastNow = NULL;
    SLIST_FOREACH(firstAfter, &rjl->repeatedJobs, next) {
        if(firstAfter->nextTime > nowMonotonic)
            break;
        lastNow = firstAfter;
    }

    /* Nothing to do */
    if(!lastNow) {
        if(firstAfter)
            return firstAfter->nextTime;
        return UA_INT64_MAX;
    }

    /* Put the jobs that are executed now in a separate list */
    struct memberstruct(UA_RepeatedJobsList,RepeatedJobsSList) executedNowList;
    executedNowList.slh_first = SLIST_FIRST(&rjl->repeatedJobs);
    lastNow->next.sle_next = NULL;

    /* Fake entry to represent the first element in the newly-sorted list */
    UA_RepeatedJob tmp_first;
    tmp_first.nextTime = nowMonotonic - 1; /* never matches for last_dispatched */
    tmp_first.next.sle_next = firstAfter;
    UA_RepeatedJob *last_dispatched = &tmp_first;

    /* Iterate over the list of jobs to process now */
    UA_RepeatedJob *rj;
    while((rj = SLIST_FIRST(&executedNowList))) {
        /* Remove from the list */
        SLIST_REMOVE_HEAD(&executedNowList, next);

        /* Dispatch/process job */
        rjl->processCallback(rjl->processContext, &rj->job);
        *dispatched = true;

        if (rj->removeAfterExecution) {
            UA_free(rj);
            continue;
        }

        /* Set the time for the next execution. Prevent an infinite loop by
         * forcing the next processing into the next iteration. */
        rj->nextTime += (UA_Int64)rj->interval;
        if(rj->nextTime < nowMonotonic)
            rj->nextTime = nowMonotonic + 1;

        /* Find the new position for rj to keep the list sorted */
        UA_RepeatedJob *prev_rj;
        if(last_dispatched->nextTime == rj->nextTime) {
            /* We "batch" repeatedJobs with the same interval in
             * addRepeatedJobs. So this might occur quite often. */
            UA_assert(last_dispatched != &tmp_first);
            prev_rj = last_dispatched;
        } else {
            /* Find the position for the next execution by a linear search
             * starting at the first possible job */
            prev_rj = &tmp_first;
            while(true) {
                UA_RepeatedJob *n = SLIST_NEXT(prev_rj, next);
                if(!n || n->nextTime >= rj->nextTime)
                    break;
                prev_rj = n;
            }

            /* Update last_dispatched */
            last_dispatched = rj;
        }

        /* Add entry to the new position in the sorted list */
        SLIST_INSERT_AFTER(prev_rj, rj, next);
    }

    /* Set the entry-point for the newly sorted list */
    rjl->repeatedJobs.slh_first = tmp_first.next.sle_next;

    /* Re-repeat processAddRemoved since one of the jobs might have removed or
     * added a job. So we get the returned timeout right. */
    processAddRemoveJobs(rjl, nowMonotonic);

    /* Return timestamp of next repetition */
    rj = SLIST_FIRST(&rjl->repeatedJobs);
    if (!rj)
        return UA_INT64_MAX;
    return rj->nextTime;
}

void
UA_RepeatedJobsList_deleteMembers(UA_RepeatedJobsList *rjl) {
    UA_RepeatedJob *current;
    while((current = SLIST_FIRST(&rjl->repeatedJobs))) {
        SLIST_REMOVE_HEAD(&rjl->repeatedJobs, next);
        UA_free(current);
    }
    while((current = SLIST_FIRST(&rjl->addRemoveJobs))) {
        SLIST_REMOVE_HEAD(&rjl->addRemoveJobs, next);
        UA_free(current);
    }
}
