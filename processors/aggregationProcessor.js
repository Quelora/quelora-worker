/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

/* filepath: packages/quelora-worker/processors/aggregationProcessor.js */
const { cacheClient } = require('@quelora/common/services/cacheService'); 
const { addEmailJob } = require('@quelora/common/services/emailService');
const { addPushJob } = require('@quelora/common/services/pushService');
const profileService = require('@quelora/common/services/profileService'); 
const Profile = require('@quelora/common/models/Profile');
const { getLocalizedMessage } = require('@quelora/common/services/i18nService');

/**
 * Enterprise Gamification configuration model.
 * Loaded dynamically to maintain compatibility with core.
 */
let GamificationConfig = null;
try {
    GamificationConfig = require('@quelora/enterprise/models/GamificationConfig');
} catch (e) {}

/**
 * Local cache for client-specific configurations.
 */
const localConfigCache = {}; 
const CONFIG_TTL = 60000;
const idToAuthorMap = new Map();
const MAX_MAP_SIZE = 10000;

/**
 * Resolves a MongoDB Profile ID to a User/Author string.
 * Includes a small LRU-like cache to prevent excessive DB lookups.
 * @param {string|ObjectId} mongoId 
 * @returns {Promise<string|null>}
 */
const resolveAuthorId = async (mongoId) => {
    if (!mongoId) return null;
    const key = mongoId.toString();
    if (idToAuthorMap.has(key)) return idToAuthorMap.get(key);

    try {
        const profile = await Profile.findById(mongoId).select('author').lean();
        if (profile && profile.author) {
            if (idToAuthorMap.size > MAX_MAP_SIZE) idToAuthorMap.clear();
            idToAuthorMap.set(key, profile.author);
            return profile.author;
        }
    } catch (e) {
        console.error('Error resolving author ID:', e);
    }
    return null;
};

/**
 * Retrieves gamification currency names for a specific tenant.
 * @param {string} cid 
 * @returns {Promise<Object>}
 */
const getCurrencyConfigCached = async (cid) => {
    if (!GamificationConfig) return {};
    const now = Date.now();
    if (localConfigCache[cid] && localConfigCache[cid].expiresAt > now) {
        return localConfigCache[cid].currency;
    }
    try {
        const config = await GamificationConfig.findOne({ cid }).select('currency').lean();
        const currency = config?.currency || {};
        localConfigCache[cid] = { currency, expiresAt: now + CONFIG_TTL };
        return currency;
    } catch (e) {
        return {};
    }
};

/**
 * Processor for the 'flush-aggregation' job.
 * Collects buffered events from Redis, localizes messages, and dispatches SSE/Push/Email.
 * @param {Object} job - BullMQ Job object.
 */
module.exports = async (job) => {
    const { bufferKey } = job.data;
    
    // 1. Fetch Pipeline
    const pipeline = cacheClient.pipeline();
    
    pipeline.scard(`${bufferKey}:unique_ids`);
    pipeline.get(`${bufferKey}:count`);
    pipeline.lrange(`${bufferKey}:actors`, 0, -1);
    pipeline.hgetall(`${bufferKey}:meta`);
    pipeline.get(`${bufferKey}:valueSum`); 
    
    const results = await pipeline.exec();

    // 2. Parse Results safely
    const uniqueCount = parseInt(results[0][1] || 0); 
    const simpleCount = parseInt(results[1][1] || 0); 
    const count = Math.max(uniqueCount, simpleCount);
    const actors = results[2][1] || [];
    const meta = results[3][1];
    const valueSum = parseFloat(results[4][1] || 0);

    const cleanup = async () => {
        const delPipe = cacheClient.pipeline();
        delPipe.del(`${bufferKey}:unique_ids`);
        delPipe.del(`${bufferKey}:count`);
        delPipe.del(`${bufferKey}:actors`);
        delPipe.del(`${bufferKey}:meta`);
        delPipe.del(`${bufferKey}:valueSum`);
        await delPipe.exec(); 
    };

    if (!meta || count === 0) {
        await cleanup();
        return;
    }

    await cleanup();

    const { cid, recipientId, actionType, preview, references } = meta;
    
    const authorStr = await resolveAuthorId(recipientId);
    if (!authorStr) return;

    let recipientProfile;
    try {
        recipientProfile = await profileService.getProfile(authorStr, cid, {
            includeSettings: true,
            includeCounts: false,
            includeRelations: false
        });
    } catch (e) {
        return;
    }

    // --- CASE A: GAMIFICATION SUMMARY (REAL-TIME SSE) ---
    if ((actionType === 'XP_EARNED' || actionType === 'COIN_EARNED') && valueSum > 0) {
        try {
            let currencyLabel = 'Coins';
            if (actionType === 'COIN_EARNED') {
                const currencyConfig = await getCurrencyConfigCached(cid);
                if (currencyConfig) {
                    if (valueSum === 1 && currencyConfig.singularName) {
                        currencyLabel = currencyConfig.singularName;
                    } else if (currencyConfig.name) {
                        currencyLabel = currencyConfig.name;
                    }
                }
            }

            const label = actionType === 'XP_EARNED' ? 'XP' : currencyLabel;
            const ssePayload = JSON.stringify({
                targetUserId: authorStr,
                payload: {
                    type: 'gamification_summary',
                    data: {
                        cid,
                        totalValue: valueSum,
                        currency: label, 
                        eventCount: count,
                        summary: `+${valueSum} ${label}`
                    }
                }
            });

            // REFACTORED: Publish to segmented CID channel
            const channel = `notifications:cid:${cid}`;
            await cacheClient.publish(channel, ssePayload);
        } catch (err) {
            console.error(`❌ [Aggregation] Error in gamification path for CID ${cid}:`, err.message);
        }
        return; 
    }

    // --- CASE B: AGGREGATED SOCIAL NOTIFICATIONS (PUSH/EMAIL) ---
    let parsedReferences = {};
    if (references) {
        try { parsedReferences = JSON.parse(references); } catch (e) {}
    }

    const userPrefs = recipientProfile.settings?.notifications || {};
    let settingKey;

    if (actionType === 'like' || actionType === 'share') settingKey = 'postLikes';
    else if (actionType === 'comment' || actionType === 'reply') settingKey = 'comments';

    if (settingKey && userPrefs[settingKey] === false) {
        return;
    }

    const allowPush = userPrefs.push !== false;
    const allowEmail = userPrefs.email !== false;
    
    if (!allowPush && !allowEmail) return;

    const locale = recipientProfile.locale || 'es';
    
    let titleKey = `${actionType}.title`;
    let messageKey = `${actionType}.message`;
    let dataForI18n = { post: preview, count };

    if (count === 1) {
        dataForI18n.name = actors[0];
    } else if (count === 2) {
        const uniqueActors = [...new Set(actors)];
        if (uniqueActors.length >= 2) {
            dataForI18n.name = `${uniqueActors[1]} & ${uniqueActors[0]}`; 
        } else {
            dataForI18n.name = uniqueActors[0];
        }
        messageKey = `${actionType}.message_two`;
    } else {
        const uniqueActors = [...new Set(actors)];
        dataForI18n.name = `${uniqueActors[0]}, ${uniqueActors[1]}`;
        dataForI18n.others = count - 2;
        messageKey = `${actionType}.message_multiple`;
    }

    const [finalTitle, finalBody] = await Promise.all([
        getLocalizedMessage(titleKey, locale, dataForI18n),
        getLocalizedMessage(messageKey, locale, dataForI18n)
    ]);

    const jobs = [];

    if (allowPush) {
        jobs.push(addPushJob(cid, authorStr, finalTitle, finalBody, {
            ...parsedReferences,
            type: 'aggregated'
        }));
    }

    if (allowEmail) {
        jobs.push(addEmailJob(
            cid,
            authorStr,
            finalTitle,
            finalBody,
            null,
            { type: 'notification' }
        ));
    }

    if (jobs.length > 0) {
        await Promise.all(jobs);
    }
};