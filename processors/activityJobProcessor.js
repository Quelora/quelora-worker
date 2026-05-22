/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

/* filepath: packages/quelora-worker/processors/activityJobProcessor.js */
const JobExecutionLog = require('@quelora/common/models/JobExecutionLog');
const { processActivityQueue } = require('@quelora/common/services/activityProcessorService');

module.exports = async (job) => {
    const { cid } = job.data;
    const start = Date.now();

    const logEntry = await JobExecutionLog.create({
        jobName:   'activity',
        queueName: 'activity-jobs',
        cid:       cid,
        bullJobId: job.id,
        status:    'active',
        startedAt: start,
    });

    try {
        const result = await processActivityQueue(cid);

        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status:      'completed',
            completedAt: Date.now(),
            durationMs:  Date.now() - start,
            metadata:    result, // { inserted, batches }
        });

    } catch (error) {
        console.error(`❌ [Worker] Activity Job Failed for ${cid}: ${error.message}`);

        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status:      'failed',
            completedAt: Date.now(),
            durationMs:  Date.now() - start,
            error:       { message: error.message, stack: error.stack },
        });

        throw error;
    }
};
