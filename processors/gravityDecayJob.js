/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

/* filepath: packages/quelora-worker/processors/gravityDecayJob.js */
const { mongoose } = require('@quelora/common/db');
const Comment = require('@quelora/common/models/Comment');
const Client = require('@quelora/common/models/Client');
const JobExecutionLog = require('@quelora/common/models/JobExecutionLog');
const { calculateHotScore } = require('@quelora/common/utils/rankingUtils');

const BATCH_SIZE = 1000;
const DEFAULT_MAX_AGE_DAYS = 7;

module.exports = async (job) => {
    const start = Date.now();
    // Extraemos el flag forceAll
    const { forceAll } = job.data || {};
    const cid = job.data?.cid || 'SYSTEM';
    const safeJobId = job.id ? String(job.id) : `manual-${Date.now()}`;

    // Read per-client configurable maxAgeDays
    let maxAgeDays = DEFAULT_MAX_AGE_DAYS;
    if (cid !== 'SYSTEM') {
        const client = await Client.findOne({ cid }).select('jobsConfig').lean();
        maxAgeDays = client?.jobsConfig?.['gravity-decay']?.params?.maxAgeDays ?? DEFAULT_MAX_AGE_DAYS;
    }

    const logData = {
        jobName: 'gravity-decay',
        queueName: 'gravity-decay',
        cid,
        bullJobId: safeJobId,
        status: 'active',
        startedAt: start,
        metadata: { processed: 0, updated: 0, mode: forceAll ? 'FULL_SCAN' : 'INCREMENTAL', maxAgeDays }
    };

    const logEntry = await JobExecutionLog.create(logData);

    try {
        console.log(`📉 [Worker] Starting Gravity Decay (${forceAll ? 'FULL SCAN 🚨' : 'Recent Only'}, maxAgeDays=${maxAgeDays})...`);

        // CONSTRUCCIÓN DINÁMICA DE LA QUERY
        const query = { visible: true };

        // Solo aplicamos el filtro de fecha si NO es forceAll
        if (!forceAll) {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - maxAgeDays);
            query.created_at = { $gte: cutoffDate };
        }

        const cursor = Comment.find(query)
            .select('_id likesCount repliesCount created_at trust_snapshot ranking_score')
            .cursor();

        const bulkOps = [];
        let processedCount = 0;
        const now = new Date();

        for (let comment = await cursor.next(); comment != null; comment = await cursor.next()) {
            
            // USAMOS LA UTILIDAD COMPARTIDA
            const newScore = calculateHotScore(comment, now);
            const currentScore = comment.ranking_score || 0;

            // Lógica de actualización:
            // 1. Si el score cambió significativamente (> 0.001)
            // 2. O SI es un FULL SCAN y el campo no existe o es 0 (para asegurar que todos tengan valor)
            const shouldUpdate = Math.abs(newScore - currentScore) > 0.001 || (forceAll && currentScore === 0 && newScore > 0);

            if (shouldUpdate) {
                bulkOps.push({
                    updateOne: {
                        filter: { _id: comment._id },
                        update: { $set: { ranking_score: newScore } }
                    }
                });
            }

            if (bulkOps.length >= BATCH_SIZE) {
                await Comment.bulkWrite(bulkOps, { ordered: false });
                logData.metadata.updated += bulkOps.length;
                bulkOps.length = 0; 
            }
            
            processedCount++;
        }

        if (bulkOps.length > 0) {
            await Comment.bulkWrite(bulkOps, { ordered: false });
            logData.metadata.updated += bulkOps.length;
        }

        logData.metadata.processed = processedCount;
        
        console.log(`✅ [Worker] Gravity Decay Complete. Scanned: ${processedCount}, Updated: ${logData.metadata.updated}`);
        
        return { processed: processedCount, updated: logData.metadata.updated };

    } catch (error) {
        console.error('❌ [Worker] Gravity Decay Failed:', error);
        
        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status: 'failed',
            completedAt: Date.now(),
            durationMs: Date.now() - start,
            error: { message: error.message, stack: error.stack }
        });
        
        throw error;
    } finally {
        const currentLog = await JobExecutionLog.findById(logEntry._id);
        if (currentLog && currentLog.status === 'active') {
             await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
                status: 'completed',
                completedAt: Date.now(),
                durationMs: Date.now() - start,
                metadata: logData.metadata
            });
        }
    }
};