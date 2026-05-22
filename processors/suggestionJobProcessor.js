/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

/* filepath: packages/quelora-worker/processors/suggestionJobProcessor.js */
const JobExecutionLog = require('@quelora/common/models/JobExecutionLog');
const Client = require('@quelora/common/models/Client');
const { processCommunity } = require('@quelora/common/services/suggestService');

module.exports = async (job) => {
    const { cid } = job.data;
    const start = Date.now();

    const logEntry = await JobExecutionLog.create({
        jobName: 'suggestion',
        queueName: 'suggestion-jobs',
        cid: cid,
        bullJobId: job.id,
        status: 'active',
        startedAt: start
    });

    try {
        // Read per-client configurable params (fall back to defaults if absent)
        const client = await Client.findOne({ cid }).select('jobsConfig').lean();
        const params = client?.jobsConfig?.suggestion?.params || {};

        // Execute the suggestion engine for this tenant
        const result = await processCommunity(cid, null, params);

        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status:      'completed',
            completedAt: Date.now(),
            durationMs:  Date.now() - start,
            metadata:    result, // { usersProcessed } or { usersProcessed, skipped: true }
        });

    } catch (error) {
        console.error(`❌ [Worker] Suggestion Job Failed: ${error.message}`);
        
        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status: 'failed',
            completedAt: Date.now(),
            durationMs: Date.now() - start,
            error: { 
                message: error.message, 
                stack: error.stack 
            }
        });
        
        throw error; 
    }
};