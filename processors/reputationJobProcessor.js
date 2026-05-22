/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

/* filepath: packages/quelora-worker/processors/reputationJobProcessor.js */
const JobExecutionLog = require('@quelora/common/models/JobExecutionLog');
// Note: Assuming you moved the service to common/services as requested.
// If not, adjust path to: require('@quelora/dashboard-api/services/reputationProcessorService')
const { processReputationQueue } = require('@quelora/common/services/reputationProcessorService');

/**
 * Reputation Job Processor.
 * Wraps the business logic with auditing and error handling.
 */
module.exports = async (job) => {
    const { cid } = job.data;
    const start = Date.now();

    // 1. Audit: Create "Active" Log
    const logEntry = await JobExecutionLog.create({
        jobName: 'reputation',
        queueName: 'reputation-jobs',
        cid: cid,
        bullJobId: job.id,
        status: 'active',
        startedAt: start
    });

    try {
        console.log(`⚙️ [Worker] Processing Reputation for CID: ${cid || 'Global'}`);

        // 2. Execute Business Logic
        const result = await processReputationQueue();

        // 3. Audit: Update "Completed" Log
        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status:      'completed',
            completedAt: Date.now(),
            durationMs:  Date.now() - start,
            metadata:    result, // { eventsProcessed, profilesUpdated }
        });

    } catch (error) {
        console.error(`❌ [Worker] Reputation Job Failed: ${error.message}`);
        
        // 4. Audit: Update "Failed" Log
        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status: 'failed',
            completedAt: Date.now(),
            durationMs: Date.now() - start,
            error: { 
                message: error.message, 
                stack: error.stack 
            }
        });
        
        // Re-throw so BullMQ handles the retry policy
        throw error; 
    }
};