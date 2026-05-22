/*
 * Quelora — quelora-worker
 * Copyright (C) 2026 Germán Zelaya — https://quelora.org
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * This file is part of Quelora. See the LICENSE file for terms.
 */

// packages/quelora-worker/processors/emailProcessor.js
const nodemailer = require('nodemailer');
const Profile = require('@quelora/common/models/Profile');
const ProfileFollower = require('@quelora/common/models/ProfileFollower');
const getClientConfig = require('@quelora/common/services/clientConfigService');
const { emailQueue } = require('@quelora/common/services/emailService');

const BATCH_SIZE = 100;

const transporterCache = new Map();

const getCachedTransporter = (cid, config) => {
    const cacheKey = `${cid || 'default'}:${config.smtp_host}:${config.smtp_user}`;

    if (!transporterCache.has(cacheKey)) {
        console.log(`🔌 Creating new SMTP Pool for: ${cacheKey}`);
        
        const transporter = nodemailer.createTransport({
            pool: true,
            maxConnections: 5,
            maxMessages: Infinity,
            host: config.smtp_host,
            port: parseInt(config.smtp_port) || 587,
            secure: config.smtp_port === '465',
            auth: config.smtp_user ? {
                user: config.smtp_user,
                pass: config.smtp_pass,
            } : undefined,
            tls: { rejectUnauthorized: false },
        });

        transporter.verify((error) => {
            if (error) {
                console.error(`❌ SMTP Connection Error for ${cacheKey}:`, error);
                transporterCache.delete(cacheKey); 
            }
        });

        transporterCache.set(cacheKey, transporter);
    }

    return transporterCache.get(cacheKey);
};

module.exports = async (job) => {
  const { cid, author, subject, body, to, type, cursorId } = job.data;

  try {
    let emailConfig = null;
    if (cid) {
      emailConfig = await getClientConfig.getClientEmailConfig(cid);
    }

    if (!emailConfig || !emailConfig.smtp_host) {
      emailConfig = {
        smtp_host: process.env.SMTP_HOST,
        smtp_port: process.env.SMTP_PORT,
        smtp_user: process.env.SMTP_USER,
        smtp_pass: process.env.SMTP_PASS,
      };
    }

    if (!emailConfig.smtp_host) {
      throw new Error(`No email configuration found for client ${cid}`);
    }

    const fromAddress = emailConfig.smtp_user || "no-reply@quelora.org";

    const transporter = getCachedTransporter(cid, emailConfig);

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
      
      const recipients = await Profile.find({
        _id: { $in: followerIds },
        email: { $exists: true, $ne: null }
      }).select('email').lean();

      const promises = recipients.map(recipient =>
        transporter.sendMail({
          from: fromAddress,
          to: recipient.email,
          subject,
          html: body,
        }).catch(err => console.error(`Failed to email ${recipient.email}: ${err.message}`))
      );

      await Promise.all(promises);

      if (followersBatch.length === BATCH_SIZE) {
          const lastCursor = followersBatch[followersBatch.length - 1]._id;
          await emailQueue.add('send-email', {
              ...job.data,
              cursorId: lastCursor
          }, { 
              jobId: `email_broadcast_${author}_${Date.now()}_chunk_${lastCursor}` 
          });
      }

      return { success: true, processed: recipients.length, hasMore: followersBatch.length === BATCH_SIZE };
    }

    let recipientEmail = to;
    if (!recipientEmail) {
      const profile = await Profile.findOne({ author, cid });
      if (!profile || !profile.email) return { success: false, message: 'No email found' };
      recipientEmail = profile.email;
    }

    await transporter.sendMail({
      from: fromAddress,
      to: recipientEmail,
      subject,
      html: body,
    });

    return { success: true, sentTo: recipientEmail, type: 'direct' };

  } catch (error) {
    throw error;
  }
};