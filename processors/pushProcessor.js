/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

// packages/quelora-worker/processors/pushProcessor.js
const webpush = require('web-push');
const Profile = require('@quelora/common/models/Profile');
const ProfileFollower = require('@quelora/common/models/ProfileFollower');
const getClientConfig = require('@quelora/common/services/clientConfigService');
const { notificationQueue } = require('@quelora/common/services/pushService');

const BATCH_SIZE = 1000;

module.exports = async (job) => {
    const { cid, author, title, body, data, cursorId } = job.data;
    const type = data?.type || 'default';

    const vapidConfig = await getClientConfig.getClientVapidConfig(cid);
    
    if (!vapidConfig) {
        return { success: false, message: 'No VAPID config' };
    }

    webpush.setVapidDetails(
        `mailto:${vapidConfig.contact_email || 'admin@localhost'}`,
        vapidConfig.publicKey,
        vapidConfig.privateKey
    );

    if (type === 'broadcast_followers') {
        const authorProfile = await Profile.findOne({ author, cid }).select('_id');
        if (!authorProfile) return { success: false, message: 'Author not found' };

        const query = { profile_id: authorProfile._id };
        if (cursorId) {
            query._id = { $lt: cursorId };
        }

        const followersBatch = await ProfileFollower.find(query)
            .select('_id follower_id')
            .sort({ _id: -1 })
            .limit(BATCH_SIZE)
            .lean();

        if (followersBatch.length === 0) {
            return { success: true, message: 'Broadcast complete' };
        }

        const followerIds = followersBatch.map(f => f.follower_id);
        const targetProfiles = await Profile.find({ 
            _id: { $in: followerIds }, 
            pushSubscriptions: { $exists: true, $not: { $size: 0 } } 
        }).select('pushSubscriptions').lean();

        const payload = JSON.stringify({ title, body, ...data });
        const allPromises = [];

        for (const profile of targetProfiles) {
            const subPromises = profile.pushSubscriptions.map(sub => 
                webpush.sendNotification(sub, payload).catch(err => {
                    if (err.statusCode === 410 || err.statusCode === 404) {
                        return Profile.updateOne(
                            { _id: profile._id },
                            { $pull: { pushSubscriptions: { endpoint: sub.endpoint } } }
                        );
                    }
                })
            );
            allPromises.push(...subPromises);
        }

        await Promise.allSettled(allPromises);

        if (followersBatch.length === BATCH_SIZE) {
            const lastCursor = followersBatch[followersBatch.length - 1]._id;
            await notificationQueue.add('send-notification', {
                ...job.data,
                cursorId: lastCursor
            }, {
                jobId: `broadcast_${author}_${Date.now()}_chunk_${lastCursor}`
            });
        }

        return { success: true, processed: followersBatch.length, hasMore: followersBatch.length === BATCH_SIZE };
    } 

    const profile = await Profile.findOne({ author, cid });
    if (profile && profile.pushSubscriptions?.length > 0) {
        const payload = JSON.stringify({ title, body, ...data });
        await Promise.allSettled(profile.pushSubscriptions.map(sub => 
            webpush.sendNotification(sub, payload).catch(() => {})
        ));
    }

    return { success: true, type: 'single' };
};