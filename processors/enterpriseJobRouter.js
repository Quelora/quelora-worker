/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

/* filepath: packages/quelora-worker/processors/enterpriseJobRouter.js */
const JobExecutionLog = require('@quelora/common/models/JobExecutionLog');

// 1. Importamos el paquete Enterprise completo
let enterprise = null;
try {
    enterprise = require('@quelora/enterprise');
} catch (e) {
    console.warn('⚠️ [EnterpriseRouter] Could not load @quelora/enterprise package.');
}

module.exports = async (job) => {
    // Si no hay enterprise, no podemos procesar nada
    if (!enterprise || !enterprise.processors) {
        throw new Error('Enterprise package is missing or has no processors export.');
    }

    const jobName = job.name; // Ej: 'gamification' o 'ad-stats'
    const { cid } = job.data;

    // 2. Buscamos el procesador específico en el objeto que exportaste en index.js
    const specificProcessor = enterprise.processors[jobName];

    if (!specificProcessor) {
        throw new Error(`❌ [Enterprise] No processor found for job type: '${jobName}'`);
    }

    // 3. Log de inicio (Mismo patrón que systemJobProcessor)
    const start = Date.now();
    const logEntry = await JobExecutionLog.create({
        jobName: jobName,
        queueName: 'enterprise-jobs',
        cid: cid || 'system',
        bullJobId: job.id,
        status: 'active',
        startedAt: start
    });

    try {
        // 4. Ejecutamos la función específica (gamificationJobProcessor o adStatsJobProcessor)
        const result = await specificProcessor(job);

        // 5. Log de éxito
        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status:      'completed',
            completedAt: Date.now(),
            durationMs:  Date.now() - start,
            metadata:    result || {},
        });

    } catch (error) {
        console.error(`❌ [Enterprise] ${jobName} Failed: ${error.message}`);
        
        await JobExecutionLog.findByIdAndUpdate(logEntry._id, {
            status: 'failed',
            completedAt: Date.now(),
            durationMs: Date.now() - start,
            error: { message: error.message, stack: error.stack }
        });
        throw error; // Re-lanzar para que BullMQ lo marque como fallido
    }
};