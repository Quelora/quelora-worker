/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

/* filepath: packages/quelora-worker/processors/systemJobProcessor.js */
const JobExecutionLog = require('@quelora/common/models/JobExecutionLog');
const {
    saveStats,
    saveGeoStats,
    saveGeoPostStats,
    savePostViews,
    saveProfileStats
} = require('@quelora/common/services/statsService');

const { runTokenUsageRollup } = require('@quelora/common/services/tokenUsageService');
const { runDailyRollup } = require('@quelora/common/services/statsRollupService');

const geoService = require('@quelora/common/services/geoService');

module.exports = async (job) => {
    const start = Date.now();
    const operation = job.name;

    const logEntry = await JobExecutionLog.create({
        jobName:   operation,
        queueName: 'system-jobs',
        cid:       'SYSTEM',
        bullJobId: job.id,
        status:    'active',
        startedAt: start,
    });

    try {
        let metadata = {};

        switch (operation) {
            case 'stats-rollup': {
                const [statsResult, viewsResult, geoResult, geoPostResult] = await Promise.all([
                    saveStats(),
                    savePostViews(),
                    saveGeoStats(),
                    saveGeoPostStats(),
                ]);
                metadata = {
                    postEntities:  statsResult?.postEntities  ?? 0,
                    systemCids:    statsResult?.systemCids    ?? 0,
                    viewsProcessed: viewsResult?.viewsProcessed ?? 0,
                    geoStats:      geoResult   ?? 0,
                    geoPostStats:  geoPostResult ?? 0,
                };
                break;
            }

            case 'profile-stats': {
                const result = await saveProfileStats();
                metadata = { profilesProcessed: result?.profilesProcessed ?? 0 };
                break;
            }

            case 'geo-update':
                console.log('🗺️ [System] Starting MaxMind DB Update...');
                await geoService.updateAllClients(true);
                metadata = { completed: true };
                break;

            case 'token-usage-rollup': {
                const result = await runTokenUsageRollup();
                metadata = { keysProcessed: result?.keysProcessed ?? 0 };
                break;
            }

            case 'daily-rollup': {
                const endDate = new Date();
                endDate.setUTCHours(0, 0, 0, 0);
                const startDate = new Date(endDate);
                startDate.setUTCDate(startDate.getUTCDate() - 1);
                await runDailyRollup(startDate, endDate);
                metadata = {
                    rangeStart: startDate.toISOString(),
                    rangeEnd:   endDate.toISOString(),
                };
                break;
            }

            default:
                throw new Error(`Unknown system operation: ${operation}`);
        }

        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status:      'completed',
            completedAt: Date.now(),
            durationMs:  Date.now() - start,
            metadata,
        });

    } catch (error) {
        console.error(`❌ [System] ${operation} Failed: ${error.message}`);

        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status:      'failed',
            completedAt: Date.now(),
            durationMs:  Date.now() - start,
            error:       { message: error.message, stack: error.stack },
        });
        throw error;
    }
};
