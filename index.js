/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

// filepath: packages/quelora-worker/index.js
require('dotenv').config();
const connectDB      = require('@quelora/common/db');
const { createWorker } = require('@quelora/common/infrastructure/bullmq');
const { QUEUES }       = require('@quelora/common/constants/queues');

// --- 1. Event-driven processors (triggered by API actions) ---
const emailProcessor       = require('./processors/emailProcessor');
const pushProcessor        = require('./processors/pushProcessor');
const aggregationProcessor = require('./processors/aggregationProcessor');

// --- 2. Scheduled job processors (triggered by quelora-jobs) ---
const activityJobProcessor   = require('./processors/activityJobProcessor');
const reputationJobProcessor = require('./processors/reputationJobProcessor');
const suggestionJobProcessor = require('./processors/suggestionJobProcessor');
const systemJobProcessor     = require('./processors/systemJobProcessor');
const gravityDecayJob        = require('./processors/gravityDecayJob');

// --- 3. Optional enterprise router (absent in community builds) ---
let enterpriseJobRouter = null;
try {
    enterpriseJobRouter = require('./processors/enterpriseJobRouter');
    console.log('💼 [Worker] Enterprise router detected.');
} catch (e) {
    console.log('ℹ️  [Worker] Enterprise router not found. Running in community mode.');
}

const CONCURRENCY = parseInt(process.env.WORKER_CONCURRENCY) || 5;

const startWorker = async () => {
    try {
        await connectDB();
        console.log('🚀 Starting Quelora workers...');

        // ---------------------------------------------------------
        // A. Event-driven workers
        // ---------------------------------------------------------
        const emailWorker = createWorker(QUEUES.EMAILS,        emailProcessor,       { concurrency: CONCURRENCY });
        const pushWorker  = createWorker(QUEUES.NOTIFICATIONS,  pushProcessor,        { concurrency: CONCURRENCY });
        const aggrWorker  = createWorker(QUEUES.AGGREGATION,    aggregationProcessor, { concurrency: CONCURRENCY });

        // ---------------------------------------------------------
        // B. Scheduled job workers
        // ---------------------------------------------------------
        const gravityWorker = createWorker(QUEUES.GRAVITY, gravityDecayJob, { concurrency: CONCURRENCY });
        gravityWorker.on('completed', job => {
            if (job.returnvalue?.updated > 0) {
                console.log(`📉 [Gravity] Updated scores for ${job.returnvalue.updated} comments.`);
            }
        });
        gravityWorker.on('failed', (job, err) => console.error(`🔥 [Gravity] Job ${job.id} failed: ${err.message}`));

        const reputationWorker = createWorker(QUEUES.REPUTATION, reputationJobProcessor, { concurrency: 1 });
        reputationWorker.on('failed', (job, err) => console.error(`🔥 [Reputation] Job ${job.id} failed: ${err.message}`));

        const suggestionWorker = createWorker(QUEUES.SUGGESTION, suggestionJobProcessor, { concurrency: 1 });
        suggestionWorker.on('completed', job => console.log(`✅ [Suggestion] Job ${job.id} completed.`));
        suggestionWorker.on('failed',    (job, err) => console.error(`🔥 [Suggestion] Job ${job.id} failed: ${err.message}`));

        const systemWorker = createWorker(QUEUES.SYSTEM, systemJobProcessor, { concurrency: 1 });
        systemWorker.on('completed', job => console.log(`✅ [System] ${job.name} completed.`));
        systemWorker.on('failed',    (job, err) => console.error(`🔥 [System] ${job.name} failed: ${err.message}`));

        const activityWorker = createWorker(QUEUES.ACTIVITY, activityJobProcessor, { concurrency: 5 });
        activityWorker.on('failed', (job, err) => console.error(`🔥 [Activity] Job ${job.id} failed: ${err.message}`));

        // ---------------------------------------------------------
        // C. Enterprise worker (optional)
        // ---------------------------------------------------------
        let enterpriseWorker = null;
        if (enterpriseJobRouter) {
            enterpriseWorker = createWorker(QUEUES.ENTERPRISE, enterpriseJobRouter, { concurrency: 2 });
            enterpriseWorker.on('failed', (job, err) => console.error(`🔥 [Enterprise] ${job.name} failed: ${err.message}`));
            console.log(`✅ [Worker] Enterprise queue listener attached.`);
        }

        console.log('✅ All workers are listening and ready.');

        // ---------------------------------------------------------
        // D. Graceful shutdown
        // ---------------------------------------------------------
        const gracefulShutdown = async (signal) => {
            console.log(`Received ${signal}, closing workers...`);
            try {
                await Promise.all([
                    emailWorker.close(),
                    pushWorker.close(),
                    aggrWorker.close(),
                    gravityWorker.close(),
                    reputationWorker.close(),
                    suggestionWorker.close(),
                    systemWorker.close(),
                    activityWorker.close(),
                    ...(enterpriseWorker ? [enterpriseWorker.close()] : []),
                ]);
                console.log('✅ Workers closed. Exiting.');
                process.exit(0);
            } catch (err) {
                console.error('❌ Error during shutdown:', err);
                process.exit(1);
            }
        };

        process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
        process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

    } catch (error) {
        console.error('❌ Fatal worker start error:', error);
        process.exit(1);
    }
};

startWorker();