'use strict';
/*
 * Maata server
 * Customer app (/)      : login, messages, WebRTC voice/video call signaling
 * Admin panel  (/admin) : live dashboard, customers, call/login/message reports,
 *                         abuse reports, announcements, audit log, CSV export
 */
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'change-this-secret-in-production';
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || 'admin';
const ADMIN_HASH = process.env.ADMIN_PASSWORD ? bcrypt.hashSync(process.env.ADMIN_PASSWORD, 10) : null;
const TZ = Number(process.env.TZ_OFFSET_MINUTES ?? 330); // 330 = India (IST)
if (!process.env.JWT_SECRET) console.warn('[warn] JWT_SECRET is not set. Set it before going live.');
if (!ADMIN_HASH) console.warn('[warn] ADMIN_PASSWORD is not set. Admin panel is locked until you set it.');

// ---------- Database ----------
// With MONGODB_URI set, data is kept permanently in MongoDB (survives restarts & redeploys).
// Without it, data goes to data/db.json (fine for testing only).
const { MongoClient, GridFSBucket } = require('mongodb');
const MONGODB_URI = process.env.MONGODB_URI || '';
const COLLS = ['users', 'messages', 'logins', 'calls', 'reports', 'announcements', 'audit', 'statuses', 'groups', 'aichats', 'i18n', 'products', 'ads', 'communities', 'channels', 'channelPosts', 'meta', 'reels', 'reelComments', 'activity', 'tickets', 'mapPlaces', 'mapReports', 'mapRoads', 'invoices', 'stockMoves', 'bizLog', 'orders', 'teams', 'teamTracks', 'teamEvents', 'bmKeys', 'bmTemplates', 'bmMsgs', 'bmPrefs', 'bmJobs', 'adminFiles'];
const SORT_BY = { users: 'createdAt', messages: 'ts', logins: 'ts', calls: 'startedAt', reports: 'ts', announcements: 'ts', audit: 'ts', statuses: 'createdAt', groups: 'createdAt', aichats: 'ts', products: 'createdAt', ads: 'createdAt', communities: 'createdAt', channels: 'createdAt', channelPosts: 'ts', meta: 'createdAt', reels: 'createdAt', reelComments: 'ts', activity: 'ts', tickets: 'createdAt', mapPlaces: 'createdAt', mapReports: 'createdAt', mapRoads: 'createdAt' , invoices: 'createdAt', stockMoves: 'at', bizLog: 'at', orders: 'createdAt', teams: 'createdAt', teamTracks: 'createdAt', teamEvents: 'at', bmKeys: 'createdAt', bmTemplates: 'createdAt', bmMsgs: 'createdAt', bmPrefs: 'createdAt', bmJobs: 'createdAt', adminFiles: 'createdAt' };
const DATA_DIR = path.join(__dirname, 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');
let db = {};
for (const k of COLLS) db[k] = [];
let mongo = null;
let bucket = null;
let chatBucket = null;
const MEDIA_DIR = path.join(DATA_DIR, 'media');
const CHAT_DIR = path.join(DATA_DIR, 'chat');
const snap = {}; // collection -> Map(id -> last saved JSON), so only changed records are written

async function loadDb() {
  if (MONGODB_URI) {
    const client = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 15000 });
    await client.connect();
    mongo = client.db(process.env.MONGODB_DB || 'maata');
    bucket = new GridFSBucket(mongo, { bucketName: 'statusMedia' }); // photos & videos for statuses
    chatBucket = new GridFSBucket(mongo, { bucketName: 'chatMedia' }); // files sent in chats
    for (const c of COLLS) {
      const docs = await mongo.collection(c).find({}).toArray();
      db[c] = docs.map(({ _id, ...rest }) => ({ id: _id, ...rest })).sort((a, b) => (a[SORT_BY[c]] || 0) - (b[SORT_BY[c]] || 0));
      snap[c] = new Map(db[c].map((d) => [d.id, JSON.stringify(d)]));
    }
    await mongo.collection('messages').createIndex({ from: 1, to: 1, ts: 1 }).catch(() => {});
    console.log(`Connected to MongoDB: ${db.users.length} customers, ${db.messages.length} messages loaded`);
  } else {
    console.warn('[warn] MONGODB_URI is not set. Using data/db.json - data is lost when the server restarts.');
    fs.mkdirSync(DATA_DIR, { recursive: true });
    try { const f = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); for (const k of COLLS) db[k] = f[k] || []; } catch { /* first run */ }
  }
  // Calls left open by a restart are closed so reports stay accurate
  let fixed = false;
  for (const c of db.calls) if (!c.endedAt) { c.endedAt = c.answeredAt || c.startedAt; c.status = c.answeredAt ? 'completed' : 'missed'; c.duration = c.duration || 0; fixed = true; }
  if (fixed) save();
}

let saveTimer = null, saving = false, saveAgain = false;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(flush, 400);
}
async function flush() {
  if (!mongo) {
    const tmp = DB_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(db));
    fs.renameSync(tmp, DB_FILE);
    return;
  }
  if (saving) { saveAgain = true; return; }
  saving = true;
  try {
    for (const c of COLLS) {
      const prev = snap[c] || new Map();
      const next = new Map();
      const ops = [];
      for (const d of db[c]) {
        const j = JSON.stringify(d);
        next.set(d.id, j);
        if (prev.get(d.id) !== j) { const { id, ...rest } = d; ops.push({ replaceOne: { filter: { _id: id }, replacement: rest, upsert: true } }); }
      }
      for (const id of prev.keys()) if (!next.has(id)) ops.push({ deleteOne: { filter: { _id: id } } });
      if (ops.length) await mongo.collection(c).bulkWrite(ops, { ordered: false });
      snap[c] = next;
    }
  } catch (e) {
    console.error('[db] save failed, retrying in 5s:', e.message);
    setTimeout(save, 5000);
  } finally {
    saving = false;
    if (saveAgain) { saveAgain = false; save(); }
  }
}
// Render stops the old server on every redeploy: write pending changes first
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, async () => {
    clearTimeout(saveTimer);
    try { await flush(); } catch {}
    process.exit(0);
  });
}

// ---------- Helpers ----------
const newId = () => crypto.randomUUID();
const now = () => Date.now();
const dayKey = (ts) => new Date(ts + TZ * 60000).toISOString().slice(0, 10);
const hourOf = (ts) => new Date(ts + TZ * 60000).getUTCHours();
function startOfToday() {
  const d = new Date(now() + TZ * 60000); d.setUTCHours(0, 0, 0, 0);
  return d.getTime() - TZ * 60000;
}
function rangeStart(range) {
  const s = startOfToday();
  if (range === 'today') return s;
  if (range === '7d') return s - 6 * 86400000;
  if (range === '30d') return s - 29 * 86400000;
  return 0;
}
const publicUser = (u) => ({ id: u.id, name: u.name, phone: u.phone, lang: u.lang || null, about: u.about || '', photo: !!u.photo, photoV: u.photoV || 0 });

// ---------- Translation ----------
// Languages customers can choose. Speech recognition & voice playback happen in the browser;
// the server only translates text.
const LANGS = ['te', 'hi', 'en', 'ta', 'kn', 'ml', 'mr', 'bn', 'gu', 'pa', 'ur', 'or'];
const GOOGLE_TRANSLATE_KEY = process.env.GOOGLE_TRANSLATE_KEY || '';
const trCache = new Map();
let trCount = 0;
const decodeEntities = (t) => t.replace(/&#(\d+);/g, (_, n) => String.fromCharCode(n)).replace(/&quot;/g, '"').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>');
async function translate(text, from, to) {
  text = String(text || '').trim().slice(0, 500);
  if (!text || !LANGS.includes(to) || from === to) return text || null;
  const key = from + '|' + to + '|' + text;
  if (trCache.has(key)) return trCache.get(key);
  let out = null;
  try {
    if (GOOGLE_TRANSLATE_KEY) {
      // Google Cloud Translation (best quality for Indian languages)
      const r = await fetch('https://translation.googleapis.com/language/translate/v2?key=' + encodeURIComponent(GOOGLE_TRANSLATE_KEY), {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ q: text, ...(LANGS.includes(from) ? { source: from } : {}), target: to, format: 'text' }),
        signal: AbortSignal.timeout(8000),
      });
      const d = await r.json();
      out = d?.data?.translations?.[0]?.translatedText || null;
      if (!out && d?.error) console.error('[translate] Google:', d.error.message);
    } else {
      // Free fallback (daily limit). Set MYMEMORY_EMAIL for a higher free limit.
      const q = new URLSearchParams({ q: text, langpair: (LANGS.includes(from) ? from : 'en') + '|' + to });
      if (process.env.MYMEMORY_EMAIL) q.set('de', process.env.MYMEMORY_EMAIL);
      const r = await fetch('https://api.mymemory.translated.net/get?' + q, { signal: AbortSignal.timeout(8000) });
      const d = await r.json();
      const t = d?.responseData?.translatedText;
      if (t && Number(d.responseStatus) === 200 && !/MYMEMORY WARNING|QUERY LENGTH LIMIT/i.test(t)) out = decodeEntities(t);
    }
  } catch (e) { console.error('[translate]', e.message); }
  if (out) {
    trCount++;
    trCache.set(key, out);
    if (trCache.size > 5000) trCache.delete(trCache.keys().next().value);
  }
  return out;
}
const userById = (id) => db.users.find((u) => u.id === id);
const nameOf = (id) => (typeof id === 'string' && id.startsWith('g:') ? '👥 ' + (db.groups.find((g) => g.id === id.slice(2))?.name || 'Group') : userById(id)?.name || 'Deleted user');
const sign = (u) => jwt.sign({ sub: u.id, tv: u.tokenVersion || 0 }, JWT_SECRET, { expiresIn: '30d' });
function findUserByToken(token) {
  const p = jwt.verify(token, JWT_SECRET);
  if (!p.sub) return null;
  const u = userById(p.sub);
  if (!u || u.status === 'blocked' || (u.tokenVersion || 0) !== (p.tv || 0)) return null;
  if (p.did) { // a linked computer: works only while the phone keeps it in "Linked devices"
    const d = (u.devices || []).find((x) => x.id === p.did); if (!d) return null;
    if (now() - (d.lastAt || 0) > 600000) { d.lastAt = now(); save(); }
  }
  return u;
}
const tokenDevice = (token) => { try { return jwt.decode(token)?.did || null; } catch { return null; } };
function csv(rows) {
  const esc = (v) => { const s = v == null ? '' : String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
  return rows.map((r) => r.map(esc).join(',')).join('\n');
}
function sendCsv(res, name, rows) {
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${name}-${dayKey(now())}.csv"`);
  res.send('\ufeff' + csv(rows));
}
const fmtLocal = (ts) => (ts ? new Date(ts + TZ * 60000).toISOString().replace('T', ' ').slice(0, 19) : '');

// ---------- HTTP ----------
const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: '100kb' }));
// HTML files can sit in a "public" folder or right next to server.js (easier to upload from a phone)
const PAGES = fs.existsSync(path.join(__dirname, 'public', 'index.html')) ? path.join(__dirname, 'public') : __dirname;
// 📉 Less internet data (hosting bandwidth): API answers bigger than 1.4 KB go out gzip-compressed (about 4× smaller)
const zlib = require('zlib');
const gzOk = (req) => /\bgzip\b/.test(String(req.headers['accept-encoding'] || ''));
app.use((req, res, next) => {
  const json = res.json.bind(res);
  res.json = (body) => {
    const txt = JSON.stringify(body);
    if (txt === undefined || txt.length < 1400 || !gzOk(req) || res.headersSent) return json(body);
    try { const gz = zlib.gzipSync(txt, { level: 6 }); res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Content-Encoding', 'gzip'); res.setHeader('Vary', 'Accept-Encoding'); res.setHeader('Content-Length', gz.length); return res.end(gz); }
    catch { return json(body); }
  };
  next();
});
// the app page (1.5 MB) is sent compressed (~0.35 MB) and only when it changed; otherwise the phone uses its saved copy (304)
const pageCache = new Map();
function sendPage(req, res, file) {
  let st; try { st = fs.statSync(file); } catch { return res.status(404).end(); }
  let c = pageCache.get(file);
  if (!c || c.mtime !== st.mtimeMs) { const raw = fs.readFileSync(file); c = { mtime: st.mtimeMs, raw, gz: zlib.gzipSync(raw, { level: 9 }), etag: '"' + require('crypto').createHash('sha1').update(raw).digest('hex').slice(0, 20) + '"' }; pageCache.set(file, c); }
  res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.setHeader('Cache-Control', 'no-cache'); res.setHeader('ETag', c.etag); res.setHeader('Vary', 'Accept-Encoding');
  if (req.headers['if-none-match'] === c.etag) return res.status(304).end();
  if (gzOk(req)) { res.setHeader('Content-Encoding', 'gzip'); res.setHeader('Content-Length', c.gz.length); return res.end(c.gz); }
  res.setHeader('Content-Length', c.raw.length); res.end(c.raw);
}
// ---------- Installable app (PWA / Play Store TWA) + push notifications when the app is closed ----------
const webpush = require('web-push');
const APP_ORIGIN = (process.env.APP_ORIGIN || 'https://maataapp.com').replace(/\/$/, '');
let VAPID = null;
async function setupPush() {
  let pub = (process.env.VAPID_PUBLIC_KEY || '').trim(), priv = (process.env.VAPID_PRIVATE_KEY || '').trim();
  if (!pub || !priv) { // keep one key pair forever (stored in the database), or every phone would have to subscribe again
    let m = db.meta.find((x) => x.id === 'vapid');
    if (!m) { const k = webpush.generateVAPIDKeys(); m = { id: 'vapid', publicKey: k.publicKey, privateKey: k.privateKey, createdAt: now() }; db.meta.push(m); save(); }
    pub = m.publicKey; priv = m.privateKey;
  }
  webpush.setVapidDetails('mailto:' + (process.env.PUSH_CONTACT || 'support@maataapp.com'), pub, priv);
  VAPID = { publicKey: pub };
  console.log('[push] Notifications when the app is closed: on');
}
async function pushTo(user, payload, opts = {}) {
  if (user && !opts.webOnly) fcmTo(user, payload, opts.ttl).catch(() => {});
  if (!VAPID || !user || !(user.pushSubs || []).length) return 0;
  let sent = 0; const dead = [];
  await Promise.all(user.pushSubs.map(async (s) => {
    try { await webpush.sendNotification({ endpoint: s.endpoint, keys: s.keys }, JSON.stringify(payload), { TTL: opts.ttl || 3600, urgency: opts.urgency || 'normal', topic: opts.topic }); sent++; }
    catch (e) { if (e.statusCode === 404 || e.statusCode === 410) dead.push(s.endpoint); else console.error('[push]', e.statusCode || '', e.body || e.message); }
  }));
  if (dead.length) { user.pushSubs = user.pushSubs.filter((s) => !dead.includes(s.endpoint)); save(); }
  return sent;
}
const hasPush = (u) => !!(u && ((VAPID && (u.pushSubs || []).length) || (FCM && (u.fcmTokens || []).length)));
app.get('/api/push/key', auth, (req, res) => res.json({ key: VAPID ? VAPID.publicKey : null }));
app.post('/api/push/subscribe', auth, (req, res) => {
  const s = req.body?.subscription;
  if (!s || typeof s.endpoint !== 'string' || !/^https:\/\//.test(s.endpoint) || !s.keys?.p256dh || !s.keys?.auth) return res.status(400).json({ error: 'Bad subscription.' });
  const list = (req.user.pushSubs || []).filter((x) => x.endpoint !== s.endpoint);
  list.push({ endpoint: s.endpoint.slice(0, 1000), keys: { p256dh: String(s.keys.p256dh).slice(0, 200), auth: String(s.keys.auth).slice(0, 100) }, ua: clip(req.headers['user-agent'], 120), at: now() });
  req.user.pushSubs = list.slice(-5); save(); res.json({ ok: true });
});
app.post('/api/push/unsubscribe', auth, (req, res) => { req.user.pushSubs = (req.user.pushSubs || []).filter((x) => x.endpoint !== String(req.body?.endpoint || '')); save(); res.json({ ok: true }); });
// Logging out on a device: stop its notifications and calls (web push and/or Android app)
app.post('/api/logout', auth, (req, res) => {
  const ep = String(req.body?.endpoint || ''), ft = String(req.body?.fcm || '');
  if (ep) req.user.pushSubs = (req.user.pushSubs || []).filter((x) => x.endpoint !== ep);
  if (ft) req.user.fcmTokens = (req.user.fcmTokens || []).filter((x) => x.token !== ft);
  save(); res.json({ ok: true });
});
app.post('/api/push/test', auth, async (req, res) => res.json({ sent: await pushTo(req.user, { type: 'test', title: 'Maata 🦜', body: 'Notifications are working!', url: '/' }) }));

// ---------- Native Android app: Firebase Cloud Messaging (full-screen incoming calls, like WhatsApp) ----------
// Reads the service-account key from Render's secret file (or FIREBASE_SERVICE_ACCOUNT_JSON). No extra package needed.
let FCM = null;
(function loadFirebase() {
  try {
    let raw = (process.env.FIREBASE_SERVICE_ACCOUNT_JSON || '').trim();
    if (!raw) for (const f of ['/etc/secrets/firebase-service-account.json', path.join(__dirname, 'firebase-service-account.json')]) if (fs.existsSync(f)) { raw = fs.readFileSync(f, 'utf8'); break; }
    if (!raw) return console.log('[fcm] Android app push: off (no firebase-service-account.json)');
    const sa = JSON.parse(raw);
    if (!sa.client_email || !sa.private_key || !sa.project_id) throw new Error('the key file is missing fields');
    FCM = { sa, token: null, exp: 0 };
    console.log('[fcm] Android app push: on (project ' + sa.project_id + ')');
  } catch (e) { console.error('[fcm] could not read the Firebase key:', e.message); }
})();
async function fcmAccessToken() {
  if (FCM.token && FCM.exp > now() + 60000) return FCM.token;
  const assertion = jwt.sign({ scope: 'https://www.googleapis.com/auth/firebase.messaging' }, FCM.sa.private_key,
    { algorithm: 'RS256', issuer: FCM.sa.client_email, audience: 'https://oauth2.googleapis.com/token', expiresIn: 3600 });
  const r = await fetch('https://oauth2.googleapis.com/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion }), signal: AbortSignal.timeout(10000) });
  const d = await r.json(); if (!d.access_token) throw new Error('token: ' + (d.error_description || d.error || r.status));
  FCM.token = d.access_token; FCM.exp = now() + (d.expires_in || 3600) * 1000; return FCM.token;
}
// data-only, high priority: wakes the phone even when Maata is closed; the app decides how to show it
async function fcmSend(token, data, ttlSec) {
  const body = { message: { token, data: Object.fromEntries(Object.entries(data).map(([k, v]) => [k, String(v ?? '')])), android: { priority: 'HIGH', ttl: (ttlSec || 3600) + 's' } } };
  const r = await fetch('https://fcm.googleapis.com/v1/projects/' + FCM.sa.project_id + '/messages:send', { method: 'POST',
    headers: { Authorization: 'Bearer ' + (await fcmAccessToken()), 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(10000) });
  if (r.ok) return 'ok';
  const t = await r.text();
  if (r.status === 404 || /UNREGISTERED|registration-token-not-registered|INVALID_ARGUMENT.*token/i.test(t)) return 'dead';
  console.error('[fcm]', r.status, t.slice(0, 200)); return 'error';
}
async function fcmTo(user, data, ttlSec) {
  if (!FCM || !user || !(user.fcmTokens || []).length) return 0;
  let sent = 0; const dead = [];
  await Promise.all(user.fcmTokens.map(async (t) => { try { const r = await fcmSend(t.token, data, ttlSec); if (r === 'ok') sent++; if (r === 'dead') dead.push(t.token); } catch (e) { console.error('[fcm]', e.message); } }));
  if (dead.length) { user.fcmTokens = user.fcmTokens.filter((t) => !dead.includes(t.token)); save(); }
  return sent;
}
app.post('/api/push/fcm', auth, (req, res) => {
  const token = String(req.body?.token || '').trim();
  if (token.length < 20 || token.length > 4096) return res.status(400).json({ error: 'Bad token.' });
  for (const u of db.users) if (u.id !== req.user.id && (u.fcmTokens || []).some((t) => t.token === token)) u.fcmTokens = u.fcmTokens.filter((t) => t.token !== token); // phone changed account
  req.user.fcmTokens = [...(req.user.fcmTokens || []).filter((t) => t.token !== token), { token, at: now(), model: clip(req.body?.model, 80) }].slice(-5);
  save(); res.json({ ok: true, fcm: !!FCM });
});
// "Decline" pressed on the full-screen call screen (the app is closed, so it proves itself with its push token)
app.post('/api/push/decline', (req, res) => {
  const token = String(req.body?.fcm || ''), callId = String(req.body?.callId || '');
  const u = token && db.users.find((x) => (x.fcmTokens || []).some((t) => t.token === token));
  const pr = u && pendingRings.get(u.id);
  if (!pr || pr.callId !== callId) return res.json({ ok: false });
  clearInterval(pr.timer); pendingRings.delete(u.id);
  const rec = activeCalls.get(callId); if (rec) finishCall(rec, 'declined');
  io.to('user:' + pr.from).emit('call:rejected', { callId, from: u.id });
  res.json({ ok: true });
});

// Message notifications for people who are not online right now
function previewOf(m) {
  if (m.type === 'text') return m.text.slice(0, 120);
  return ({ image: '📷 Photo', video: '🎥 Video', audio: m.file && m.file.voice ? '🎤 Voice message' : '🎵 Audio', file: '📄 Document', contact: '👤 Contact', poll: '📊 Poll', event: '📅 Event', location: '📍 Location', gif: '🎞️ GIF', sticker: '💟 Sticker', product: '🛍️ Product', reel: '🎬 Reel' })[m.type] || 'New message';
}
function notifyOffline(fromUser, m) {
  if (!VAPID || m.type === 'system' || m.viewOnce) return;
  const body = m.viewOnce ? '① View once' : previewOf(m);
  if (m.group) {
    const g = groupById(m.group); if (!g) return;
    for (const x of g.members) {
      if (x.id === fromUser.id || isOnline(x.id)) continue;
      const u = userById(x.id); if (!u || (u.muted || []).includes('g:' + g.id)) continue;
      const saved = contactsOf(u).find((c) => c.id === fromUser.id);
      pushTo(u, { type: 'msg', title: g.name, body: (saved ? saved.name : fromUser.name) + ': ' + body, tag: 'g:' + g.id, url: '/#chat=g:' + g.id });
    }
  } else {
    const u = userById(m.to); if (!u || isOnline(u.id) || (u.muted || []).includes(fromUser.id)) return;
    const saved = contactsOf(u).find((c) => c.id === fromUser.id);
    pushTo(u, { type: 'msg', title: saved ? saved.name : fromUser.name + ' (' + fromUser.phone + ')', body, tag: fromUser.id, url: '/#chat=' + fromUser.id });
  }
}
// Calls to someone whose app is closed: wake the phone with a push, keep the call waiting ~40 s
const pendingRings = new Map(); // userId -> { offer, ice: [], until, callId, from }
app.get('/manifest.webmanifest', (req, res) => {
  res.type('application/manifest+json').json({
    id: '/', name: 'Maata — free calls & messages', short_name: 'Maata', description: 'Free voice calls, video calls and chat with live translation.',
    start_url: '/?source=app', scope: '/', display: 'standalone', orientation: 'portrait', background_color: '#0A3A47', theme_color: '#0F4C5C', lang: 'en-IN', categories: ['communication', 'social'],
    icons: [{ src: '/icons/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' }, { src: '/icons/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
      { src: '/icons/icon-maskable-192.png', sizes: '192x192', type: 'image/png', purpose: 'maskable' }, { src: '/icons/icon-maskable-512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' }],
    shortcuts: [{ name: 'Maata AI', url: '/#chat=ai', icons: [{ src: '/icons/icon-192.png', sizes: '192x192' }] }, { name: 'Status', url: '/#tab=status', icons: [{ src: '/icons/icon-192.png', sizes: '192x192' }] }],
  });
});
const ICON_FILES = ['icon-192.png', 'icon-512.png', 'icon-maskable-192.png', 'icon-maskable-512.png', 'apple-touch-icon.png', 'badge-96.png'];
app.get('/icons/:f', (req, res) => {
  if (!ICON_FILES.includes(req.params.f)) return res.status(404).end();
  const p1 = path.join(PAGES, 'icons', req.params.f), p2 = path.join(PAGES, req.params.f);
  const f = fs.existsSync(p1) ? p1 : fs.existsSync(p2) ? p2 : null;
  if (!f) return res.status(404).end();
  res.setHeader('Cache-Control', 'public, max-age=604800'); res.sendFile(f);
});
app.get('/favicon.ico', (req, res) => res.redirect(301, '/icons/icon-192.png'));
// Play Store app (TWA): proves maataapp.com and the Android app belong together, so the app opens full screen
app.get('/.well-known/assetlinks.json', (req, res) => {
  const pkg = (process.env.TWA_PACKAGE || '').trim(), fps = (process.env.TWA_SHA256 || '').split(',').map((x) => x.trim()).filter(Boolean);
  res.json(pkg && fps.length ? [{ relation: ['delegate_permission/common.handle_all_urls'], target: { namespace: 'android_app', package_name: pkg, sha256_cert_fingerprints: fps } }] : []);
});
app.get('/sw.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript'); res.setHeader('Cache-Control', 'no-cache'); res.setHeader('Service-Worker-Allowed', '/');
  res.send(SW_SOURCE);
});
const SW_SOURCE = `
const CACHE = 'maata-shell-v3';
self.addEventListener('install', (e) => { e.waitUntil(caches.open(CACHE).then((c) => c.addAll(['/', '/icons/icon-192.png', '/icons/badge-96.png']).catch(() => {}))); self.skipWaiting(); });
self.addEventListener('activate', (e) => { e.waitUntil(caches.keys().then((ks) => Promise.all(ks.filter((k) => k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim())); });
// Pages: always try the network first (so updates show at once), fall back to the saved copy when offline
// Chat photos, videos, voice notes and files are kept on the phone. The server deletes them after some days,
// so the phone's copy is what people see later (like WhatsApp).
const MEDIA = 'maata-media-v1';
const mediaKey = (u) => { const x = new URL(u); return x.origin + x.pathname; };
function sliceResp(resp, range) {
  return resp.blob().then((b) => {
    const m = /bytes=(\\d*)-(\\d*)/.exec(range || ''); let a = 0, z = b.size - 1;
    if (m) { if (m[1]) { a = parseInt(m[1], 10); if (m[2]) z = Math.min(parseInt(m[2], 10), b.size - 1); } else if (m[2]) a = Math.max(0, b.size - parseInt(m[2], 10)); }
    if (a > z) return new Response(null, { status: 416, headers: { 'Content-Range': 'bytes */' + b.size } });
    return new Response(b.slice(a, z + 1), { status: 206, headers: { 'Content-Type': resp.headers.get('Content-Type') || '', 'Content-Range': 'bytes ' + a + '-' + z + '/' + b.size, 'Content-Length': String(z - a + 1), 'Accept-Ranges': 'bytes' } });
  });
}
function expiredResp(req) {
  if (req.destination === 'image') return new Response('<svg xmlns="http://www.w3.org/2000/svg" width="320" height="200" viewBox="0 0 320 200"><rect width="320" height="200" rx="14" fill="#E4E7EC"/><text x="160" y="92" text-anchor="middle" font-family="sans-serif" font-size="34">⌛</text><text x="160" y="128" text-anchor="middle" font-family="sans-serif" font-size="15" fill="#475467">Photo expired</text><text x="160" y="150" text-anchor="middle" font-family="sans-serif" font-size="12" fill="#667085">No copy on this phone</text></svg>', { status: 200, headers: { 'Content-Type': 'image/svg+xml', 'X-Maata-Expired': '1' } });
  return new Response(null, { status: 410, headers: { 'X-Maata-Expired': '1' } });
}
function saveFull(url, key) {
  return fetch(url, { credentials: 'same-origin' }).then((net) => {
    if (!net.ok || net.status !== 200) return net;
    const len = Number(net.headers.get('content-length') || 0);
    if (len && len > 80 * 1048576) return net; // very big files are not kept
    return net.blob().then((b) => caches.open(MEDIA).then((c) => c.put(key, new Response(b, { headers: { 'Content-Type': net.headers.get('content-type') || b.type || '', 'Content-Length': String(b.size), 'X-Saved': String(Date.now()) } }))).then(() => caches.open(MEDIA)).then((c) => c.match(key)));
  });
}
async function mediaFetch(e) {
  const req = e.request, key = mediaKey(req.url), range = req.headers.get('range');
  const hit = await (await caches.open(MEDIA)).match(key);
  if (hit) return range ? sliceResp(hit, range) : hit;
  if (range) { // a video or voice note starting to play: stream it now, keep a full copy in the background
    let net; try { net = await fetch(req); } catch { return expiredResp(req); }
    if (net.status === 410) return expiredResp(req);
    if (net.ok) e.waitUntil(saveFull(req.url, key).catch(() => {}));
    return net;
  }
  try { const r = await saveFull(req.url, key); return r && r.status === 410 ? expiredResp(req) : r; } catch { return expiredResp(req); }
}
self.addEventListener('fetch', (e) => {
  const u = new URL(e.request.url);
  if (e.request.method === 'GET' && u.origin === location.origin && u.pathname.startsWith('/api/chat/file/') && !u.searchParams.has('dl')) e.respondWith(mediaFetch(e));
});
self.addEventListener('message', (e) => {
  const d = e.data || {};
  if (d.type === 'media-stats') e.waitUntil(caches.open(MEDIA).then(async (c) => { const ks = await c.keys(); let bytes = 0, videos = 0, vbytes = 0; for (const k of ks) { const r = await c.match(k); const n = Number(r.headers.get('Content-Length') || 0); bytes += n; if (/^video\\//.test(r.headers.get('Content-Type') || '')) { videos++; vbytes += n; } } e.source.postMessage({ type: 'media-stats', files: ks.length, bytes, videos, vbytes }); }));
  if (d.type === 'media-clear') e.waitUntil(caches.open(MEDIA).then(async (c) => { let n = 0; for (const k of await c.keys()) { const r = await c.match(k); if (d.videosOnly && !/^video\\//.test(r.headers.get('Content-Type') || '')) continue; await c.delete(k); n++; } e.source.postMessage({ type: 'media-cleared', n }); }));
});
self.addEventListener('fetch', (e) => {
  const r = e.request;
  if (r.method !== 'GET' || new URL(r.url).origin !== location.origin) return;
  if (r.url.includes('/api/chat/file/')) return;
  if (r.mode === 'navigate') { e.respondWith(fetch(r).then((res) => { const cp = res.clone(); caches.open(CACHE).then((c) => c.put('/', cp)); return res; }).catch(() => caches.match('/'))); return; }
  if (r.url.includes('/icons/')) e.respondWith(caches.match(r).then((m) => m || fetch(r)));
});
self.addEventListener('push', (e) => {
  let d = {}; try { d = e.data.json(); } catch { d = { title: 'Maata', body: e.data ? e.data.text() : '' }; }
  const call = d.type === 'call';
  const opts = { body: d.body || '', icon: '/icons/icon-192.png', badge: '/icons/badge-96.png', tag: d.tag || d.type || 'maata', renotify: true, data: { url: d.url || '/', type: d.type, callId: d.callId },
    vibrate: call ? [800, 400, 800, 400, 800, 400, 800] : [150, 80, 150], requireInteraction: call, silent: false, timestamp: Date.now(), image: undefined,
    actions: call ? [{ action: 'answer', title: '📞 Answer' }, { action: 'decline', title: '✖ Decline' }] : [{ action: 'open', title: 'Open' }] };
  e.waitUntil(self.registration.showNotification(d.title || 'Maata', opts));
});
self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const d = e.notification.data || {};
  if (e.action === 'decline') return;
  const url = d.type === 'call' ? '/#answer=' + d.callId : d.url || '/';
  e.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((list) => {
    for (const c of list) if (new URL(c.url).origin === location.origin) { c.postMessage({ maata: 'open', url }); return c.focus(); }
    return self.clients.openWindow(url);
  }));
});`;

app.get(['/', '/index.html'], (req, res) => sendPage(req, res, path.join(PAGES, 'index.html')));
// Newest Android app version (written by the GitHub build next to maata.apk) — installed apps compare with it
app.get('/app-version.json', (req, res) => {
  res.setHeader('Cache-Control', 'no-cache');
  for (const f of [path.join(PAGES, 'app-version.json'), path.join(__dirname, 'app-version.json')]) { try { return res.json(JSON.parse(fs.readFileSync(f, 'utf8'))); } catch { /* try next */ } }
  res.json({ version: 0 });
});
// Android app download: put the newest APK in public/ as maata.apk (from GitHub Actions → Artifacts)
let apkMirror = (process.env.APK_URL || '').trim(); // e.g. the GitHub copy of maata.apk — downloads then do not use the hosting bandwidth
(async () => { if (apkMirror) return; const repo = (process.env.GITHUB_REPO || 'balajiapollo2025-gif/Maata').trim();
  for (const u of ['https://raw.githubusercontent.com/' + repo + '/main/public/maata.apk', 'https://raw.githubusercontent.com/' + repo + '/main/maata.apk']) {
    try { const r = await fetch(u, { method: 'HEAD', signal: AbortSignal.timeout(8000) }); if (r.ok && Number(r.headers.get('content-length') || 0) > 1e6) { apkMirror = u; console.log('[apk] downloads served from', u); return; } } catch { /* try next */ } } })();
app.get(['/download', '/app', '/maata.apk'], (req, res, next) => {
  if (apkMirror && req.query.local !== '1') { res.setHeader('Cache-Control', 'no-cache'); return res.redirect(302, apkMirror + '?v=' + Date.now()); }
  next();
});
app.get(['/download', '/app', '/maata.apk'], (req, res) => {
  const f = [path.join(PAGES, 'maata.apk'), path.join(__dirname, 'maata.apk')].find((x) => fs.existsSync(x));
  if (f) { res.setHeader('Content-Type', 'application/vnd.android.package-archive'); res.setHeader('Cache-Control', 'no-cache'); return res.download(f, 'Maata.apk'); }
  res.status(404).type('html').send('<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><body style="font-family:system-ui;padding:24px;line-height:1.5"><h2>🦜 Maata Android app</h2><p>The download is being prepared. Please try again soon, or use Maata in your browser at <a href="/">maataapp.com</a>.</p></body>');
});
app.get(['/admin', '/admin.html'], (req, res) => sendPage(req, res, path.join(PAGES, 'admin.html')));
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 3e6 }); // allows short speech clips for live translation

const online = new Map();      // userId -> connected device count
const activeCalls = new Map(); // callId -> call record (ringing / answered)
const isOnline = (id) => (online.get(id) || 0) > 0;

const hits = new Map();
function rateLimit(req, res, next) {
  const key = req.ip + req.path;
  const list = (hits.get(key) || []).filter((t) => now() - t < 60_000);
  list.push(now()); hits.set(key, list);
  if (list.length > 20) return res.status(429).json({ error: 'Too many attempts. Wait a minute and try again.' });
  next();
}
function auth(req, res, next) {
  const h = req.headers.authorization || '';
  try {
    const u = findUserByToken(h.startsWith('Bearer ') ? h.slice(7) : '');
    if (!u) throw new Error('no user');
    req.user = u; next();
  } catch { res.status(401).json({ error: 'Session expired. Log in again.' }); }
}

// ---------- App features: the admin can switch any customer feature on or off (Admin → ⚙️ App features) ----------
const FEATURE_LIST = [
  ['Chat', 'translate', '🌐 Maata Translate (translate any message)'],
  ['Chat', 'ai', '🦜 Maata AI (AI chat, ✨ write with AI, suggest replies, AI auto-reply)'],
  ['Chat', 'documents', '📄 Send documents'],
  ['Chat', 'contactCard', '👤 Share a contact'],
  ['Chat', 'polls', '📊 Polls & events'],
  ['Chat', 'location', '📍 Share location'],
  ['Chat', 'gps', '🗺️ GPS photo stamp'],
  ['Chat', 'broadcast', '📣 Broadcast lists'],
  ['Chat', 'quickReplies', '⚡ Quick replies'],
  ['Updates & social', 'status', '⭕ Status / stories'],
  ['Updates & social', 'channels', '📢 Channels'],
  ['Updates & social', 'communities', '👥 Communities'],
  ['Reels', 'reels', '🎬 Reels (watch & post short videos)'],
  ['Reels', 'studio', '🎨 Maata Studio (reel editor: music, text, AI subtitles, AI voice)'],
  ['Reels', 'remix', '🎭 Remix & Duet'],
  ['Reels', 'nearby', '📍 Nearby reels'],
  ['Calls', 'voiceCalls', '📞 Voice calls'],
  ['Calls', 'videoCalls', '🎥 Video calls'],
  ['Calls', 'groupCalls', '👥 Group calls'],
  ['Calls', 'conference', '➕ Add a person to a call (conference)'],
  ['Calls', 'callTranslate', '🌐 Live call translation'],
  ['Calls', 'screenShare', '🖥️ Screen share in video calls'],
  ['Calls', 'callQuality', '🎧 Call quality setting'],
  ['Business', 'business', '🏪 Business tools (profile, catalogue, free ads)'],
  ['Account & app', 'qrLogin', '💻 Log in on a computer with QR code'],
  ['Account & app', 'backup', '☁️ Chat backup (Google Drive)'],
  ['Account & app', 'themes', '🎨 Theme & colours'],
  ['Account & app', 'appLock', '🔒 App lock'],
  ['Account & app', 'support', '🆘 Help & Support (complaints)'],
  ['Updates & social', 'maps', '🗺️ Maata Maps (navigation)'],
  ['Account & app', 'invite', '🎁 Invite a friend'],
  ['Account & app', 'coins', '🪙 Referral coins (1 coin when an invited friend uses Maata 3 days)'],
];
function featureFlags() {
  const m = (db.meta || []).find((x) => x.id === 'features'), f = {};
  for (const [, k] of FEATURE_LIST) f[k] = !(m && m.flags && m.flags[k] === false);
  return f;
}
const featOn = (k) => featureFlags()[k] !== false;
const OFF_MSG = 'This feature is turned off by Maata right now.';
app.get('/api/features', (req, res) => { res.setHeader('Cache-Control', 'no-store'); res.json(featureFlags()); });
// the server enforces it too (older app versions cannot use a switched-off feature)
const FEAT_PATHS = [['/api/reels', 'reels'], ['/api/reels-creators', 'reels'], ['/api/insights', 'reels'], ['/api/ai', 'ai'], ['/api/translate', 'translate'], ['/api/qr', 'qrLogin'], ['/api/backup', 'backup'],
  ['/api/support', 'support'], ['/api/maps', 'maps'], ['/api/channels', 'channels'], ['/api/communities', 'communities'], ['/api/business', 'business'], ['/api/catalogue', 'business'], ['/api/ads', 'business']];
for (const [path, k] of FEAT_PATHS) app.use(path, (req, res, next) => (featOn(k) ? next() : res.status(403).json({ error: OFF_MSG, featureOff: k })));
app.use('/api/status', (req, res, next) => (req.method === 'GET' || featOn('status') ? next() : res.status(403).json({ error: OFF_MSG, featureOff: 'status' })));

// ---------- Live feed for admins ----------
let liveTimer = null;
function liveNow() {
  let inCall = 0, ringing = 0;
  for (const c of activeCalls.values()) c.status === 'answered' ? inCall++ : ringing++;
  for (const c of gcalls.values()) inCall += c.parts.size;
  return { online: online.size, inCall, ringing, ts: now() };
}
function pushLive() {
  clearTimeout(liveTimer);
  liveTimer = setTimeout(() => io.to('admins').emit('live', liveNow()), 300);
}
const feed = (type, text) => io.to('admins').emit('feed', { type, text, ts: now() });

function recordLogin(u, req, kind) {
  u.lastLoginAt = now();
  db.logins.push({ id: newId(), userId: u.id, ts: now(), kind, ip: req.ip, ua: String(req.headers['user-agent'] || '').slice(0, 200) });
  feed(kind === 'signup' ? 'signup' : 'login', `${u.name} ${kind === 'signup' ? 'joined Maata' : 'logged in'}`);
}

// ---------- Customer API ----------
// Indian mobile numbers are stored as exactly 10 digits (no +91, no 0), so one number = one account
function cleanMobile(raw) {
  let d = String(raw || '').replace(/\D/g, '');
  if (d.length === 12 && d.startsWith('91')) d = d.slice(2);
  else if (d.length === 11 && d.startsWith('0')) d = d.slice(1);
  else if (d.length === 13 && d.startsWith('091')) d = d.slice(3);
  return /^[6-9]\d{9}$/.test(d) ? d : null;
}
const findByMobile = (m) => db.users.find((x) => cleanMobile(x.phone) === m);
// ---------- Email (for forgot password) ----------
// Works with any SMTP: Gmail (smtp.gmail.com, port 465, an "App password"), Brevo, Zoho, etc.
const nodemailer = require('nodemailer');
// ---------- Email (password reset codes) ----------
// Render's free plan blocks normal email ports (SMTP), so the best choice is an email API over HTTPS:
//   BREVO_API_KEY  (brevo.com, free 300 emails/day)  or  RESEND_API_KEY (resend.com)
// SMTP (e.g. Gmail app password) still works on paid servers.
const SMTP = { host: (process.env.SMTP_HOST || '').trim(), port: Number(process.env.SMTP_PORT || 465), user: (process.env.SMTP_USER || '').trim(), pass: (process.env.SMTP_PASS || '').replace(/\s+/g, '') };
const BREVO_KEY = (process.env.BREVO_API_KEY || '').trim(), RESEND_KEY = (process.env.RESEND_API_KEY || '').trim();
const MAIL_FROM_RAW = (process.env.MAIL_FROM || '').trim();
const mailer = SMTP.host && SMTP.user && SMTP.pass ? nodemailer.createTransport({ host: SMTP.host, port: SMTP.port, secure: SMTP.port === 465, auth: { user: SMTP.user, pass: SMTP.pass }, connectionTimeout: 10000, greetingTimeout: 10000 }) : null;
const MAIL_VIA = BREVO_KEY ? 'Brevo' : RESEND_KEY ? 'Resend' : mailer ? 'SMTP ' + SMTP.host : '';
const MAIL_ON = !!MAIL_VIA;
console.log(MAIL_ON ? '[mail] Email on via ' + MAIL_VIA : '[mail] Email off: add BREVO_API_KEY + MAIL_FROM (recommended) to send password reset codes');
function mailFrom() { // "Maata <no-reply@maataapp.com>" -> { name, email }
  const m = /^(.*?)\s*<([^>]+)>$/.exec(MAIL_FROM_RAW);
  if (m) return { name: m[1].replace(/"/g, '').trim() || 'Maata', email: m[2].trim() };
  return { name: 'Maata', email: MAIL_FROM_RAW || SMTP.user };
}
async function sendMail(to, subject, html, text) {
  const from = mailFrom();
  if (BREVO_KEY) {
    if (!from.email) throw new Error('Set MAIL_FROM on Render (the sender email you verified in Brevo).');
    const r = await fetch('https://api.brevo.com/v3/smtp/email', { method: 'POST', headers: { 'api-key': BREVO_KEY, 'Content-Type': 'application/json', accept: 'application/json' }, signal: AbortSignal.timeout(15000),
      body: JSON.stringify({ sender: from, to: [{ email: to }], subject, htmlContent: html, textContent: text }) });
    if (!r.ok) throw new Error('Brevo ' + r.status + ': ' + (await r.text()).slice(0, 200));
    return;
  }
  if (RESEND_KEY) {
    const r = await fetch('https://api.resend.com/emails', { method: 'POST', headers: { Authorization: 'Bearer ' + RESEND_KEY, 'Content-Type': 'application/json' }, signal: AbortSignal.timeout(15000),
      body: JSON.stringify({ from: from.name + ' <' + (from.email || 'onboarding@resend.dev') + '>', to: [to], subject, html, text }) });
    if (!r.ok) throw new Error('Resend ' + r.status + ': ' + (await r.text()).slice(0, 200));
    return;
  }
  if (!mailer) throw new Error('mail-off');
  await mailer.sendMail({ from: MAIL_FROM_RAW || ('Maata <' + SMTP.user + '>'), to, subject, html, text });
}
const cleanEmail = (e) => { const v = String(e || '').trim().toLowerCase(); return /^[^\s@]{1,64}@[^\s@]+\.[a-z]{2,}$/.test(v) && v.length <= 120 ? v : null; };
const maskEmail = (e) => { const [a, d] = String(e).split('@'); return a.slice(0, 2) + '•'.repeat(Math.max(2, a.length - 2)) + '@' + d; };
const resetCodes = new Map(); // userId -> { hash, until, tries, sentAt }
app.post('/api/password/forgot', rateLimit, async (req, res) => {
  const id = String(req.body?.id || '').trim();
  const u = id.includes('@') ? db.users.find((x) => x.email === cleanEmail(id)) : (cleanMobile(id) && findByMobile(cleanMobile(id)));
  if (!u) return res.status(404).json({ error: 'No Maata account found with this mobile number or email.' });
  if (!u.email) return res.status(400).json({ error: 'This account has no email address. Please contact Maata support to reset your password.' });
  if (!MAIL_ON) return res.status(503).json({ error: 'Password reset by email is not switched on yet. Please contact Maata support.' });
  const prev = resetCodes.get(u.id);
  if (prev && now() - prev.sentAt < 60000) return res.status(429).json({ error: 'Please wait a minute before asking for another code.' });
  const code = String(crypto.randomInt(100000, 1000000));
  resetCodes.set(u.id, { hash: crypto.createHash('sha256').update(code + u.id).digest('hex'), until: now() + 15 * 60000, tries: 0, sentAt: now() });
  try {
    await sendMail(u.email, 'Your Maata password reset code: ' + code,
      `<div style="font-family:Arial,sans-serif;max-width:480px;margin:auto;padding:24px;border:1px solid #e5e7eb;border-radius:16px"><h2 style="color:#0F4C5C;margin:0 0 8px">🦜 Maata</h2><p>Hi ${String(u.name).replace(/[<>&]/g, '')},</p><p>Use this code to reset your Maata password:</p><p style="font-size:34px;font-weight:700;letter-spacing:8px;color:#0F4C5C;margin:18px 0">${code}</p><p style="color:#555">The code works for 15 minutes. If you did not ask for this, ignore this email — your password stays the same.</p></div>`,
      'Your Maata password reset code is ' + code + '. It works for 15 minutes. If you did not ask for this, ignore this email.');
  } catch (e) { console.error('[mail]', e.message); resetCodes.delete(u.id); return res.status(502).json({ error: 'Could not send the email right now. Try again later.' }); }
  audit('customer:' + u.phone, 'password-reset-requested', u.name);
  res.json({ ok: true, sentTo: maskEmail(u.email) });
});
app.post('/api/password/reset', rateLimit, async (req, res) => {
  const id = String(req.body?.id || '').trim(), code = String(req.body?.code || '').replace(/\D/g, ''), pw = String(req.body?.password || '');
  const u = id.includes('@') ? db.users.find((x) => x.email === cleanEmail(id)) : (cleanMobile(id) && findByMobile(cleanMobile(id)));
  const r = u && resetCodes.get(u.id);
  if (!r || r.until < now()) return res.status(400).json({ error: 'The code expired. Ask for a new code.' });
  if (r.tries >= 5) { resetCodes.delete(u.id); return res.status(429).json({ error: 'Too many wrong codes. Ask for a new code.' }); }
  if (crypto.createHash('sha256').update(code + u.id).digest('hex') !== r.hash) { r.tries++; return res.status(400).json({ error: 'Wrong code. ' + (5 - r.tries) + ' tries left.' }); }
  if (pw.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters.' });
  resetCodes.delete(u.id);
  u.passHash = await bcrypt.hash(pw, 10); u.tokenVersion = (u.tokenVersion || 0) + 1; // logs out every other device
  kick(u.id, 'Your password was changed. Please log in again.');
  recordLogin(u, req, 'password-reset'); save();
  audit('customer:' + u.phone, 'password-reset', u.name);
  res.json({ token: sign(u), user: publicUser(u) });
});
app.put('/api/me/email', auth, async (req, res) => {
  if (!(await bcrypt.compare(String(req.body?.password || ''), req.user.passHash || ''))) return res.status(400).json({ error: 'Wrong password.' });
  const e = String(req.body?.email || '').trim() ? cleanEmail(req.body.email) : '';
  if (e === null) return res.status(400).json({ error: 'Enter a valid email address.' });
  if (e && db.users.some((x) => x !== req.user && x.email === e)) return res.status(409).json({ error: 'This email is already used by another Maata account.' });
  if (e) req.user.email = e; else delete req.user.email;
  save(); res.json({ ...publicUser(req.user), email: req.user.email || '' });
});
app.post('/api/me/password', auth, rateLimit, async (req, res) => {
  if (!(await bcrypt.compare(String(req.body?.current || ''), req.user.passHash || ''))) return res.status(400).json({ error: 'Your current password is wrong.' });
  const pw = String(req.body?.password || ''); if (pw.length < 6) return res.status(400).json({ error: 'New password must be at least 6 characters.' });
  req.user.passHash = await bcrypt.hash(pw, 10); req.user.tokenVersion = (req.user.tokenVersion || 0) + 1;
  kick(req.user.id, 'Your password was changed. Please log in again.'); save();
  res.json({ token: sign(req.user), user: publicUser(req.user) });
});

app.post('/api/register', rateLimit, async (req, res) => {
  const name = String(req.body?.name || '').trim().slice(0, 40);
  const phone = cleanMobile(req.body?.phone);
  const password = String(req.body?.password || '');
  if (name.length < 2) return res.status(400).json({ error: 'Name must be at least 2 characters.' });
  if (!phone) return res.status(400).json({ error: 'Enter your 10-digit mobile number (without +91 or 0).' });
  const email = cleanEmail(req.body?.email);
  if (!email) return res.status(400).json({ error: 'Enter a valid email address. It is used if you forget your password.' });
  if (db.users.some((x) => x.email === email)) return res.status(409).json({ error: 'This email is already used by another Maata account.' });
  if (password.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters.' });
  if (findByMobile(phone)) return res.status(409).json({ error: 'This number is already registered. Log in instead.' });
  const u = { id: newId(), name, phone, email, passHash: await bcrypt.hash(password, 10), createdAt: now(), status: 'active', tokenVersion: 0 };
  // joined from a friend's invite link: remember who invited, and save each other as contacts
  const inviter = req.body?.ref ? db.users.find((x) => x.inviteCode && x.inviteCode === String(req.body.ref).trim().toUpperCase() && x.status !== 'blocked') : null;
  if (inviter) {
    u.referredBy = inviter.id;
    contactsOf(u).push({ id: inviter.id, name: inviter.name, addedAt: now() });
    if (!contactsOf(inviter).some((c) => c.id === u.id)) contactsOf(inviter).push({ id: u.id, name: u.name, addedAt: now() });
  }
  db.users.push(u);
  recordLogin(u, req, 'signup'); u.activeDays = [istDay()];
  save();
  if (inviter) { io.to('user:' + inviter.id).emit('invite:joined', { name: u.name }); try { pushTo(inviter, { type: 'msg', title: '🎉 ' + u.name + ' joined Maata', body: 'Your friend joined from your invite. Say hi!', tag: 'joined-' + u.id }); } catch {} }
  res.json({ token: sign(u), user: publicUser(u) });
});

app.post('/api/login', rateLimit, async (req, res) => {
  const phone = cleanMobile(req.body?.phone);
  const password = String(req.body?.password || '');
  if (!phone) return res.status(400).json({ error: 'Enter your 10-digit mobile number.' });
  const u = db.users.find((x) => x.phone === phone) || findByMobile(phone);
  if (!u || !(await bcrypt.compare(password, u.passHash))) {
    if (u && u.secure && u.secure.on) { secEvent(u, 'login-fail', 'Wrong password used to log in to your account.'); const bad = u.secure.events.filter((e) => e.type === 'login-fail' && now() - e.t < 3600000).length; save(); if (bad === 3) pushTo(u, { type: 'msg', title: '🛡️ Maata Secure', body: 'Someone tried to log in to your Maata account with a wrong password 3 times.', tag: 'maata-secure', url: '/?secure=1' }).catch(() => {}); }
    return res.status(401).json({ error: 'Wrong mobile number or password.' });
  }
  if (u.status === 'blocked') return res.status(403).json({ error: 'This account is blocked. Contact Maata support.' });
  if (u.twoStep) { const ticket = crypto.randomBytes(18).toString('base64url'); pinTickets.set(ticket, { uid: u.id, t: now(), tries: 0 }); return res.json({ needPin: true, ticket, hint: u.twoStep.hint || '' }); }
  recordLogin(u, req, 'login');
  if (u.secure && u.secure.on) { secEvent(u, 'login', 'New login on ' + deviceName(req.headers['user-agent']) + '.'); pushTo(u, { type: 'msg', title: '🛡️ New login to Maata', body: 'Your account was just opened on ' + deviceName(req.headers['user-agent']) + '. Not you? Open Maata Secure and log out other devices.', tag: 'maata-secure', url: '/?secure=1' }).catch(() => {}); }
  save();
  res.json({ token: sign(u), user: publicUser(u) });
});

app.get('/api/me', auth, (req, res) => res.json({ ...publicUser(req.user), email: req.user.email || '' }));

// ---------- My profile: name, about, photo ----------
app.put('/api/me/profile', auth, (req, res) => {
  const b = req.body || {};
  if (b.name !== undefined) {
    const n = String(b.name).trim().replace(/\s+/g, ' ').slice(0, 40);
    if (n.length < 2) return res.status(400).json({ error: 'Name must be at least 2 letters.' });
    req.user.name = n;
  }
  if (b.about !== undefined) req.user.about = String(b.about).trim().slice(0, 140);
  save();
  for (const w of watchersOf(req.user.id)) io.to('user:' + w).emit('user:profile', { id: req.user.id });
  res.json(publicUser(req.user));
});
app.post('/api/me/photo', auth, express.raw({ type: ['image/*'], limit: '3mb' }), async (req, res) => {
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Choose a photo.' });
  const id = newId(), mime = String(req.headers['content-type'] || 'image/jpeg').split(';')[0];
  try { await saveChatFile(id, req.body, mime); } catch { return res.status(500).json({ error: 'Could not save the photo.' }); }
  const old = req.user.photo;
  Object.assign(req.user, { photo: id, photoMime: mime, photoSize: req.body.length, photoV: now() });
  if (old) deleteChatFile(old);
  save();
  for (const w of watchersOf(req.user.id)) io.to('user:' + w).emit('user:profile', { id: req.user.id });
  res.json(publicUser(req.user));
});
app.delete('/api/me/photo', auth, (req, res) => {
  if (req.user.photo) deleteChatFile(req.user.photo);
  delete req.user.photo; req.user.photoV = now(); save();
  for (const w of watchersOf(req.user.id)) io.to('user:' + w).emit('user:profile', { id: req.user.id });
  res.json(publicUser(req.user));
});
// Profile photos open for any logged-in Maata user (like WhatsApp's default "Everyone"), never for people they blocked
app.get('/api/users/:id/photo', (req, res) => {
  let viewer = null; try { viewer = findUserByToken(String(req.query.t || '')); } catch { /* bad token */ }
  const u = userById(req.params.id);
  if (!viewer || !u || !u.photo || blockedOf(u).includes(viewer.id)) return res.status(404).end();
  res.setHeader('Content-Type', u.photoMime); res.setHeader('Cache-Control', 'private, max-age=86400');
  const st = chatFileStream(u.photo, 0, u.photoSize - 1); st.on('error', () => res.destroy()); st.pipe(res);
});

app.post('/api/me/lang', auth, (req, res) => {
  const lang = String(req.body?.lang || '');
  if (!LANGS.includes(lang)) return res.status(400).json({ error: 'Choose a language from the list.' });
  req.user.lang = lang; save();
  toWatchers(req.user.id, 'user:update', { id: req.user.id, lang });
  res.json(publicUser(req.user));
});

// Text-to-speech fallback: when a phone has no voice for a language (common for Telugu/Hindi on
// computers), the server makes the audio with Google Cloud Text-to-Speech.
const TTS_KEY = process.env.GOOGLE_TTS_KEY || process.env.GOOGLE_TRANSLATE_KEY || '';
// Speech-to-text on the server (Google Cloud Speech-to-Text) works in every browser (Chrome, Firefox,
// Safari/iPhone) and uses the call's own microphone stream, so there is no microphone conflict on phones.
const SPEECH_KEY = process.env.GOOGLE_SPEECH_KEY || process.env.GOOGLE_TRANSLATE_KEY || '';
const STT_LANG = { te: 'te-IN', hi: 'hi-IN', en: 'en-IN', ta: 'ta-IN', kn: 'kn-IN', ml: 'ml-IN', mr: 'mr-IN', bn: 'bn-IN', gu: 'gu-IN', pa: 'pa-Guru-IN', ur: 'ur-IN', or: 'or-IN' };
// which language a sentence is really in, from its letters (people often speak English while their setting is Telugu)
const SCRIPT_LANG = [[/[\u0C00-\u0C7F]/g, 'te'], [/[\u0900-\u097F]/g, 'hi'], [/[\u0B80-\u0BFF]/g, 'ta'], [/[\u0C80-\u0CFF]/g, 'kn'], [/[\u0D00-\u0D7F]/g, 'ml'], [/[\u0980-\u09FF]/g, 'bn'], [/[\u0A80-\u0AFF]/g, 'gu'], [/[\u0A00-\u0A7F]/g, 'pa'], [/[\u0B00-\u0B7F]/g, 'or'], [/[\u0600-\u06FF]/g, 'ur'], [/[A-Za-z]/g, 'en']];
function scriptLang(text, fallback) {
  let best = null, n = 0; for (const [re, l] of SCRIPT_LANG) { const c = (String(text).match(re) || []).length; if (c > n) { n = c; best = l; } }
  if (!best) return fallback; if (best === 'hi' && fallback === 'mr') return 'mr'; return best;
}
// 🎧 Maata AI interpreter: one short voice clip → what was said (any language, auto-detected) + the listener's language
function pcm16Wav(b64) {
  const pcm = Buffer.from(b64, 'base64'), h = Buffer.alloc(44);
  h.write('RIFF', 0); h.writeUInt32LE(36 + pcm.length, 4); h.write('WAVE', 8); h.write('fmt ', 12); h.writeUInt32LE(16, 16); h.writeUInt16LE(1, 20); h.writeUInt16LE(1, 22);
  h.writeUInt32LE(16000, 24); h.writeUInt32LE(32000, 28); h.writeUInt16LE(2, 32); h.writeUInt16LE(16, 34); h.write('data', 36); h.writeUInt32LE(pcm.length, 40);
  return Buffer.concat([h, pcm]).toString('base64');
}
async function aiHearTranslate(b64, speakerLang, toLang) {
  const L = (c) => AI_LANG_NAME[c] || 'English';
  const sys = 'You are a live phone-call interpreter in India. You get one short voice clip from a phone call. ' +
    '1) Write exactly what was said, in the script of the language spoken (detect it yourself — people often speak English, or mix English with Telugu/Hindi). ' +
    '2) Translate it naturally into ' + L(toLang) + ', short and spoken style, keeping names and numbers. ' +
    'Reply ONLY JSON: {"heard":"...","lang":"<2-letter code of the spoken language>","translated":"..."}. If there is no clear speech, reply {"heard":""}.';
  const r = await gemini(sys, [{ role: 'user', parts: [{ inline_data: { mime_type: 'audio/wav', data: pcm16Wav(b64) } }, { text: 'The speaker usually speaks ' + L(speakerLang) + '. The listener understands ' + L(toLang) + '.' }] }], { json: true, maxTokens: 1500, temperature: 0.1, fast: true });
  if (r.error) return { error: r.error === 'busy' || r.error === 'limit' ? 'busy' : 'service' };
  let d; try { d = JSON.parse(String(r.text).replace(/^```(json)?|```$/g, '').trim()); } catch { return { error: 'service' }; }
  const heard = String(d.heard || '').trim().slice(0, 500); if (!heard) return { text: '' };
  const lang = LANGS.includes(String(d.lang || '').slice(0, 2)) ? String(d.lang).slice(0, 2) : scriptLang(heard, speakerLang);
  return { text: heard, lang, translated: String(d.translated || '').trim().slice(0, 600) };
}
async function speechToText(base64, lang) {
  if (!SPEECH_KEY || !STT_LANG[lang]) return { error: 'not-configured' };
  try {
    const r = await fetch('https://speech.googleapis.com/v1/speech:recognize?key=' + encodeURIComponent(SPEECH_KEY), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        config: { encoding: 'LINEAR16', sampleRateHertz: 16000, languageCode: STT_LANG[lang], enableAutomaticPunctuation: true, ...(lang !== 'en' ? { alternativeLanguageCodes: ['en-IN'] } : {}) },
        audio: { content: base64 },
      }),
      signal: AbortSignal.timeout(12000),
    });
    const d = await r.json();
    if (d.error) { console.error('[stt]', d.error.message); return { error: 'service' }; }
    const text = (d.results || []).map((x) => x.alternatives?.[0]?.transcript || '').join(' ').trim();
    return { text };
  } catch (e) { console.error('[stt]', e.message); return { error: 'service' }; }
}

app.get('/api/config', auth, (req, res) => res.json({
  maxFile: R2_ON ? MAX_BIG_FILE : MAX_FILE,
  groupCallMax: GCALL_MAX,
  serverSpeech: !!SPEECH_KEY || !!GEMINI_KEY, // Google speech, or the Maata AI interpreter (hears every language, also in the Android app)
  serverVoice: !!(process.env.GOOGLE_TTS_KEY || process.env.GOOGLE_TRANSLATE_KEY),
  aiVoice: !!GEMINI_KEY,
  translate: GOOGLE_TRANSLATE_KEY ? 'google' : 'free',
}));

const TTS_LANG = { te: 'te-IN', hi: 'hi-IN', en: 'en-IN', ta: 'ta-IN', kn: 'kn-IN', ml: 'ml-IN', mr: 'mr-IN', bn: 'bn-IN', gu: 'gu-IN', pa: 'pa-IN', ur: 'ur-IN', or: 'or-IN' };
const ttsCache = new Map();
const ttsHits = new Map();
// ---------- Linked devices: log in on a computer by scanning a QR code with the phone (like WhatsApp Web) ----------
const QRCode = require('qrcode');
const qrSessions = new Map(); // id -> { secret, ua, until, status, token, user }
function deviceName(ua) {
  ua = String(ua || '');
  const b = /Edg\//.test(ua) ? 'Edge' : /OPR\/|Opera/.test(ua) ? 'Opera' : /Firefox\//.test(ua) ? 'Firefox' : /Chrome\//.test(ua) ? 'Chrome' : /Safari\//.test(ua) ? 'Safari' : 'Browser';
  const o = /Windows/.test(ua) ? 'Windows' : /Mac OS X|Macintosh/.test(ua) ? 'Mac' : /Android/.test(ua) ? 'Android' : /iPhone|iPad/.test(ua) ? 'iPhone' : /Linux/.test(ua) ? 'Linux' : 'computer';
  return b + ' on ' + o;
}
setInterval(() => { for (const [k, q] of qrSessions) if (q.until < now() - 60000) qrSessions.delete(k); }, 60000);
app.post('/api/qr/new', rateLimit, async (req, res) => {
  const id = newId(), secret = crypto.randomBytes(18).toString('hex');
  qrSessions.set(id, { secret, ua: String(req.headers['user-agent'] || ''), until: now() + 120000, status: 'waiting' });
  // a picture (PNG), so no page styling can change the squares; dark on white scans best
  const png = await QRCode.toDataURL('maata-link:' + id + ':' + secret, { margin: 2, width: 480, errorCorrectionLevel: 'M', color: { dark: '#0A2E36', light: '#FFFFFF' } });
  res.json({ id, secret, png, expiresIn: 120 });
});
app.get('/api/qr/:id', (req, res) => {
  const q = qrSessions.get(req.params.id);
  if (!q || q.secret !== String(req.query.s || '')) return res.status(404).json({ status: 'expired' });
  if (q.status === 'approved') { qrSessions.delete(req.params.id); return res.json({ status: 'approved', token: q.token, user: q.user }); }
  if (q.until < now()) return res.json({ status: 'expired' });
  res.json({ status: q.status });
});
function qrFromCode(code) {
  const m = /^maata-link:([\w-]+):([0-9a-f]{36})$/.exec(String(code || '').trim()); if (!m) return null;
  const q = qrSessions.get(m[1]); return q && q.secret === m[2] && q.until > now() && q.status === 'waiting' ? [m[1], q] : null;
}
app.post('/api/qr/peek', auth, (req, res) => { const f = qrFromCode(req.body?.code); if (!f) return res.status(400).json({ error: 'This QR code expired. Reload it on the computer and scan again.' }); res.json({ device: deviceName(f[1].ua) }); });
app.post('/api/qr/approve', auth, (req, res) => {
  const f = qrFromCode(req.body?.code); if (!f) return res.status(400).json({ error: 'This QR code expired. Reload it on the computer and scan again.' });
  const [id, q] = f, u = req.user;
  u.devices = (u.devices || []).slice(-4); // up to 5 linked devices
  const d = { id: newId(), name: deviceName(q.ua), linkedAt: now(), lastAt: now() };
  u.devices.push(d); save();
  Object.assign(q, { status: 'approved', token: jwt.sign({ sub: u.id, tv: u.tokenVersion || 0, did: d.id }, JWT_SECRET, { expiresIn: '30d' }), user: publicUser(u) });
  audit('customer:' + u.phone, 'device-linked', d.name);
  res.json({ ok: true, device: d });
});
app.get('/api/devices', auth, (req, res) => res.json((req.user.devices || []).map((d) => ({ ...d, online: [...io.sockets.sockets.values()].some((s) => s.did === d.id) }))));
app.delete('/api/devices/:id', auth, async (req, res) => {
  const u = req.user; u.devices = (u.devices || []).filter((d) => d.id !== req.params.id); save();
  for (const s of await io.in('user:' + u.id).fetchSockets()) if (s.did === req.params.id) { s.emit('force-logout', { reason: 'This device was logged out from your phone.' }); s.disconnect(true); }
  res.json({ ok: true });
});


// ---------- 🛡️ Maata Secure: find my phone, loud alarm, lost mode, trusted contacts, AI safety check ----------
// Only the account owner sees and controls their own phones. Strong actions need the Maata password again.
// Every action is written to the owner's security timeline, and the phone being found is told about it.
const SEC_MAX_PHONES = 5, SEC_HIST = 40, SEC_HIST_LOST = 300;
const secDevOk = (s) => /^[A-Za-z0-9_-]{10,48}$/.test(String(s || ''));
function secOf(u) {
  if (!u.secure) u.secure = { on: false, phones: [], trusted: [], events: [], lostMsg: '', lostCall: '', imei: '' };
  const s = u.secure; s.phones = s.phones || []; s.trusted = s.trusted || []; s.events = s.events || [];
  return s;
}
function secEvent(u, type, text, extra = {}) {
  const s = secOf(u); s.events.push({ id: newId(), t: now(), type, text: clip(text, 220), ...extra }); s.events = s.events.slice(-150);
}
const secPhone = (u, id) => secOf(u).phones.find((p) => p.id === id);
const secOnline = (u, id) => [...io.sockets.sockets.values()].some((s) => s.user && s.user.id === u.id && s.secDev === id);
const secAgo = (t) => { const m = Math.round((now() - t) / 60000); return m < 1 ? 'just now' : m < 60 ? m + ' min ago' : m < 1440 ? Math.round(m / 60) + ' h ago' : Math.round(m / 1440) + ' days ago'; };
const secTime = (t) => new Date(t + 5.5 * 3600000).toISOString().replace('T', ' ').slice(0, 16) + ' IST';
const secMapLink = (l) => 'https://www.google.com/maps?q=' + l.lat + ',' + l.lng;
// the place this phone usually is (most visited ~1 km square, outside lost mode)
function secUsual(p) {
  const cnt = new Map();
  for (const h of p.hist || []) { if (h.lost) continue; const k = (h.lat * 100 | 0) + ':' + (h.lng * 100 | 0), c = cnt.get(k); if (c) c.n++; else cnt.set(k, { n: 1, h }); }
  let best = null; for (const v of cnt.values()) if (!best || v.n > best.n) best = v;
  return best && best.n >= 3 ? best.h : null;
}
// 🤖 risk engine: simple, explainable rules on the owner's own security signals
function secRisk(u) {
  const s = secOf(u); if (!s.on) return { score: 0, level: 'off', reasons: [], tips: ['Turn on Maata Secure to protect this phone.'] };
  let score = 0; const reasons = [], tips = [], day = now() - 86400000;
  for (const p of s.phones) {
    if (p.lost) { score += 60; reasons.push('🚨 ' + p.name + ' is in lost mode since ' + secAgo(p.lostAt || now()) + '.'); }
    if (p.lastSeen && now() - p.lastSeen > 2 * 86400000) { score += 12; reasons.push('📴 ' + p.name + ' has not been online for ' + secAgo(p.lastSeen).replace(' ago', '') + '.'); }
    const usual = secUsual(p);
    if (usual && p.loc && geoDist([usual.lng, usual.lat], [p.loc.lng, p.loc.lat]) > 80000 && now() - p.loc.at < day) { score += 15; reasons.push('🧭 ' + p.name + ' is ' + Math.round(geoDist([usual.lng, usual.lat], [p.loc.lng, p.loc.lat]) / 1000) + ' km away from where it usually is.'); }
    if (p.lost && p.battery != null && p.battery < 15) { score += 5; reasons.push('🪫 ' + p.name + ' battery is low (' + p.battery + '%). Act soon.'); }
  }
  const ev = s.events.filter((e) => e.t > day);
  const unl = ev.filter((e) => e.type === 'unlock-fail').length; if (unl) { score += Math.min(25, 10 * unl); reasons.push('🔐 Wrong app-lock tries on your phone ' + unl + ' time' + (unl > 1 ? 's' : '') + ' today.'); }
  const bad = ev.filter((e) => e.type === 'login-fail').length; if (bad >= 3) { score += 15; reasons.push('🔑 ' + bad + ' wrong-password logins to your account today.'); }
  const nl = ev.filter((e) => e.type === 'login').length; if (nl) { score += 8; reasons.push('📱 New login to your account today. Check it was you.'); }
  if (!s.trusted.length) tips.push('Add a trusted family member. They get an alert if your phone is lost.');
  if (!s.lostCall) tips.push('Add a phone number to show on the lost-phone screen, so a finder can call you.');
  if (!s.imei) tips.push('Save your IMEI number (dial *#06#). Police and CEIR need it to block a stolen phone.');
  if (!(u.twoStep)) tips.push('Turn on two-step verification, so a thief cannot log in with only your password.');
  score = Math.min(100, score);
  return { score, level: score >= 50 ? 'high' : score >= 20 ? 'watch' : 'safe', reasons, tips };
}
function secView(u, devId) {
  const s = secOf(u);
  return {
    on: !!s.on, lostMsg: s.lostMsg || '', lostCall: s.lostCall || '', imei: s.imei || '',
    trusted: s.trusted.map((id) => { const t = userById(id); return t ? { id, name: t.name } : null; }).filter(Boolean),
    phones: s.phones.map((p) => ({ id: p.id, name: p.name, addedAt: p.addedAt, lastSeen: p.lastSeen || null, loc: p.loc || null, battery: p.battery ?? null, charging: !!p.charging, lost: !!p.lost, lostAt: p.lostAt || null, alarm: !!p.alarm, online: secOnline(u, p.id), me: p.id === devId, trail: (p.hist || []).slice(-12) })),
    events: s.events.slice(-60).reverse(), risk: secRisk(u),
  };
}
// send a command to one phone: live (socket) and as a high-priority push (wakes the Android app even when closed)
async function secSend(u, p, cmd) {
  p.cmds = [...(p.cmds || []).filter((c) => now() - c.t < 86400000 && c.cmd !== cmd), { cmd, t: now() }].slice(-6);
  for (const s of await io.in('user:' + u.id).fetchSockets()) if (s.secDev === p.id) s.emit('secure:cmd', { cmd });
  const title = { alarm: '🔔 Maata Secure alarm', lost: '🚨 This phone is reported lost', locate: '📍 Maata Secure', found: '✅ Lost mode is off', stop: '🔕 Alarm stopped' }[cmd];
  const body = { alarm: 'The owner is looking for this phone.', lost: (u.secure.lostMsg || 'Please return this phone to its owner.') + (u.secure.lostCall ? ' Call ' + u.secure.lostCall : ''), locate: 'Your Maata account asked for this phone\'s location. Open Maata to share it.', found: 'This phone is back with its owner.', stop: '' }[cmd];
  if (FCM && p.fcm) { try { await fcmSend(p.fcm, { type: 'secure_' + cmd, title: title || 'Maata Secure', body: body || '', tag: 'maata-secure', url: '/?secure=1' }, 3600); } catch (e) { console.error('[secure] push', e.message); } }
}
async function secTellTrusted(u, p, lost) {
  const s = secOf(u); let n = 0;
  for (const id of s.trusted) {
    const c = canMessage(u, id); if (c.error) continue;
    const text = lost
      ? '⚠️ Maata Secure: ' + u.name + '\'s phone (' + p.name + ') was marked LOST at ' + secTime(now()) + '. ' + (p.loc ? 'Last seen ' + secAgo(p.loc.at) + ' at the location below.' : 'No location yet.') + ' If someone calls you about this phone, please help. ' + (s.lostCall ? 'Contact: ' + s.lostCall : '')
      : '✅ Maata Secure: ' + u.name + '\'s phone (' + p.name + ') is found. Thank you!';
    deliverMessage(u, c.target, { type: 'text', text: clip(text, 900) });
    if (lost && p.loc) deliverMessage(u, c.target, { type: 'location', location: { lat: p.loc.lat, lng: p.loc.lng, acc: p.loc.acc || 0, live: false } });
    n++;
  }
  return n;
}
const secHits = new Map();
function secLimit(req, res, max) {
  const k = req.user.id, list = (secHits.get(k) || []).filter((t) => now() - t < 3600000); list.push(now()); secHits.set(k, list);
  if (list.length > max) { res.status(429).json({ error: 'Too many Maata Secure actions. Wait a few minutes.' }); return false; }
  return true;
}
app.get('/api/secure', auth, (req, res) => res.json(secView(req.user, String(req.query.dev || ''))));
app.post('/api/secure/settings', auth, async (req, res) => {
  const u = req.user, s = secOf(u), b = req.body || {};
  if (b.on !== undefined) {
    if (!b.on) { // off: forget phones and locations (privacy)
      if (!(await bcrypt.compare(String(b.password || ''), u.passHash || ''))) return res.status(403).json({ error: 'Enter your Maata password to turn off protection.' });
      s.on = false; s.phones = []; secEvent(u, 'off', 'Maata Secure turned off. Saved locations were deleted.');
    } else if (!s.on) { s.on = true; secEvent(u, 'on', 'Maata Secure turned on.'); }
  }
  if (b.lostMsg !== undefined) s.lostMsg = clip(String(b.lostMsg).trim(), 160);
  if (b.lostCall !== undefined) s.lostCall = String(b.lostCall).replace(/[^\d+ ]/g, '').slice(0, 16);
  if (b.imei !== undefined) s.imei = String(b.imei).replace(/[^\d, ]/g, '').slice(0, 40);
  if (Array.isArray(b.trusted)) s.trusted = [...new Set(b.trusted.map(String))].filter((id) => canAddTo(u, id)).slice(0, 5);
  save(); res.json(secView(u, String(b.dev || '')));
});
app.post('/api/secure/enroll', auth, (req, res) => {
  const u = req.user, s = secOf(u), b = req.body || {};
  if (!s.on) return res.status(400).json({ error: 'Turn on Maata Secure first.' });
  if (!secDevOk(b.dev)) return res.status(400).json({ error: 'Bad device.' });
  let p = secPhone(u, b.dev);
  if (!p) {
    if (s.phones.length >= SEC_MAX_PHONES) s.phones.sort((a, c) => (a.lastSeen || 0) - (c.lastSeen || 0)).shift();
    p = { id: b.dev, name: clip(String(b.name || 'My phone').trim(), 50) || 'My phone', addedAt: now(), hist: [], cmds: [] }; s.phones.push(p);
    secEvent(u, 'enroll', p.name + ' is now protected.', { dev: p.id });
  }
  if (b.fcm && String(b.fcm).length > 20 && String(b.fcm).length < 4096) p.fcm = String(b.fcm);
  p.lastSeen = now(); save(); res.json(secView(u, p.id));
});
app.post('/api/secure/rename', auth, (req, res) => {
  const p = secPhone(req.user, String(req.body?.dev || '')); if (!p) return res.status(404).json({ error: 'Phone not found.' });
  p.name = clip(String(req.body?.name || '').trim(), 50) || p.name; save(); res.json(secView(req.user, String(req.body?.me || '')));
});
app.post('/api/secure/remove', auth, async (req, res) => {
  const u = req.user, s = secOf(u), id = String(req.body?.dev || ''), p = secPhone(u, id); if (!p) return res.status(404).json({ error: 'Phone not found.' });
  if (p.lost && !(await bcrypt.compare(String(req.body?.password || ''), u.passHash || ''))) return res.status(403).json({ error: 'This phone is in lost mode. Enter your Maata password to remove it.' });
  s.phones = s.phones.filter((x) => x !== p); secEvent(u, 'remove', p.name + ' was removed from Maata Secure.'); save(); res.json(secView(u, String(req.body?.me || '')));
});
// the phone reports in (only while Maata Secure is on and this phone is enrolled)
app.post('/api/secure/ping', auth, (req, res) => {
  const u = req.user, s = secOf(u), b = req.body || {}, p = s.on && secPhone(u, String(b.dev || ''));
  if (!p) return res.json({ enrolled: false, on: !!s.on });
  p.lastSeen = now();
  if (b.battery != null && isFinite(b.battery)) { const bat = Math.max(0, Math.min(100, Math.round(Number(b.battery)))); if (p.lost && bat < 15 && (p.battery == null || p.battery >= 15)) secEvent(u, 'battery', p.name + ' battery is low (' + bat + '%) while lost.', { dev: p.id }); p.battery = bat; p.charging = !!b.charging; }
  const lat = Number(b.lat), lng = Number(b.lng);
  if (Math.abs(lat) <= 90 && Math.abs(lng) <= 180 && (lat || lng)) {
    const pt = { lat: +lat.toFixed(6), lng: +lng.toFixed(6), acc: Math.round(Number(b.acc) || 0), at: now(), ...(p.lost ? { lost: 1 } : {}) };
    const last = (p.hist || [])[p.hist.length - 1];
    const moved = !last || geoDist([last.lng, last.lat], [pt.lng, pt.lat]) > (p.lost ? 30 : 150) || now() - last.at > (p.lost ? 120000 : 1800000);
    p.loc = pt; if (moved) { p.hist = [...(p.hist || []), pt].slice(-(p.lost ? SEC_HIST_LOST : SEC_HIST)); if (p.lost) secEvent(u, 'seen', p.name + ' was seen (lost mode).', { dev: p.id, lat: pt.lat, lng: pt.lng }); }
  }
  const cmds = (p.cmds || []).filter((c) => now() - c.t < 86400000).map((c) => c.cmd); p.cmds = [];
  save();
  res.json({ enrolled: true, on: true, lost: !!p.lost, alarm: !!p.alarm, cmds, owner: u.name, lostMsg: s.lostMsg || '', lostCall: s.lostCall || '' });
});
// owner's remote actions
app.post('/api/secure/cmd', auth, async (req, res) => {
  if (!secLimit(req, res, 40)) return;
  const u = req.user, s = secOf(u), b = req.body || {}, cmd = String(b.cmd || ''), p = secPhone(u, String(b.dev || ''));
  if (!s.on) return res.status(400).json({ error: 'Maata Secure is off.' });
  if (!['alarm', 'stop', 'locate', 'lost', 'found'].includes(cmd)) return res.status(400).json({ error: 'Unknown action.' });
  if (!p) return res.status(404).json({ error: 'Phone not found.' });
  if (['lost', 'found'].includes(cmd) && !(await bcrypt.compare(String(b.password || ''), u.passHash || ''))) return res.status(403).json({ error: 'Wrong Maata password.' });
  const from = clip(String(b.from || 'another device'), 50);
  let told = 0;
  if (cmd === 'alarm') { p.alarm = true; secEvent(u, 'alarm', 'Alarm played on ' + p.name + ' (from ' + from + ').', { dev: p.id }); }
  if (cmd === 'stop') { p.alarm = false; secEvent(u, 'alarm-stop', 'Alarm stopped on ' + p.name + '.', { dev: p.id }); }
  if (cmd === 'locate') secEvent(u, 'locate', 'Location requested for ' + p.name + ' (from ' + from + ').', { dev: p.id });
  if (cmd === 'lost') { p.lost = true; p.lostAt = now(); told = await secTellTrusted(u, p, true); secEvent(u, 'lost', p.name + ' marked LOST' + (told ? '. ' + told + ' trusted contact' + (told > 1 ? 's were' : ' was') + ' told.' : '.'), { dev: p.id }); }
  if (cmd === 'found') { p.lost = false; p.alarm = false; told = await secTellTrusted(u, p, false); secEvent(u, 'found', p.name + ' marked FOUND. Lost mode is off.', { dev: p.id }); }
  save(); secSend(u, p, cmd);
  audit('customer:' + u.phone, 'secure-' + cmd, p.name);
  res.json({ ...secView(u, String(b.me || '')), told });
});
// log out every other device (works even when the lost phone is offline: its login stops working at once)
app.post('/api/secure/logout-others', auth, async (req, res) => {
  if (!secLimit(req, res, 40)) return;
  const u = req.user;
  if (!(await bcrypt.compare(String(req.body?.password || ''), u.passHash || ''))) return res.status(403).json({ error: 'Wrong Maata password.' });
  u.tokenVersion = (u.tokenVersion || 0) + 1; u.devices = [];
  secEvent(u, 'logout', 'All other devices were logged out.'); save();
  const keep = String(req.body?.socket || '');
  for (const s of await io.in('user:' + u.id).fetchSockets()) if (s.id !== keep) { s.emit('force-logout', { reason: 'You were logged out by the account owner (Maata Secure).' }); s.disconnect(true); }
  audit('customer:' + u.phone, 'secure-logout-others', '');
  res.json({ token: sign(u), user: publicUser(u) });
});
// events the phone reports: wrong app-lock tries
app.post('/api/secure/event', auth, async (req, res) => {
  if (!secLimit(req, res, 60)) return;
  const u = req.user, s = secOf(u), b = req.body || {}; if (!s.on) return res.json({ ok: true });
  const p = secPhone(u, String(b.dev || '')), name = p ? p.name : 'your phone';
  if (b.type === 'unlock-fail') {
    const lat = Number(b.lat), lng = Number(b.lng), loc = Math.abs(lat) <= 90 && Math.abs(lng) <= 180 && (lat || lng) ? { lat: +lat.toFixed(6), lng: +lng.toFixed(6) } : {};
    secEvent(u, 'unlock-fail', 'Wrong app-lock tries on ' + name + '.', { dev: b.dev, ...loc });
    save(); pushTo(u, { type: 'msg', title: '🛡️ Maata Secure', body: 'Someone tried to unlock Maata on ' + name + ' with the wrong PIN or voice. Was it you?', tag: 'maata-secure', url: '/?secure=1' }).catch(() => {});
  }
  res.json({ ok: true });
});
// 🤖 AI advice on the owner's own security signals (places and numbers only, never messages)
app.post('/api/secure/ai', auth, async (req, res) => {
  if (!secLimit(req, res, 30)) return;
  const u = req.user, r = secRisk(u), lang = AI_LANG_NAME[u.lang] || 'English';
  const facts = { level: r.level, score: r.score, reasons: r.reasons, tips: r.tips, phones: secOf(u).phones.map((p) => ({ name: p.name, lost: !!p.lost, lastSeen: p.lastSeen ? secAgo(p.lastSeen) : 'never', battery: p.battery })) };
  let text = '';
  try {
    if (GEMINI_KEY) { const g = await gemini('You are Maata Secure, a calm phone-safety helper in India. Give 3 to 5 short, practical steps for this person, in ' + lang + ', based only on these facts. If a phone is lost: first call the phone, then lost mode, then block it on CEIR (ceir.sancharsaathi.gov.in, Sanchar Saathi) with the IMEI and file a police complaint. Never invent facts. Plain text, one step per line, no markdown.', [{ role: 'user', parts: [{ text: JSON.stringify(facts) }] }], { maxTokens: 400, temperature: 0.3 }); text = String((g && g.text) || '').trim(); }
  } catch (e) { console.error('[secure] ai', e.message); }
  if (!text) text = [...r.reasons, ...r.tips].slice(0, 5).join('\n') || 'Everything looks fine. Keep Maata Secure on.';
  res.json({ ...r, advice: clip(text, 1500) });
});
// 🧾 theft report: everything the police and CEIR ask for, ready to copy
app.get('/api/secure/report', auth, (req, res) => {
  const u = req.user, s = secOf(u), p = secPhone(u, String(req.query.dev || '')); if (!p) return res.status(404).json({ error: 'Phone not found.' });
  const trail = (p.hist || []).slice(-8).reverse();
  const lines = [
    'LOST / STOLEN MOBILE PHONE – DETAILS', '',
    'Owner name: ' + u.name, 'Owner mobile number: ' + u.phone, 'Phone: ' + p.name, 'IMEI: ' + (s.imei || '[dial *#06# on a phone with the same box, or see the phone box / bill]'),
    'Lost / noticed missing at: ' + (p.lostAt ? secTime(p.lostAt) : '[DATE AND TIME]'), '',
    'Last known location: ' + (p.loc ? p.loc.lat + ', ' + p.loc.lng + ' (' + secTime(p.loc.at) + ', accuracy about ' + (p.loc.acc || '?') + ' m)\n' + secMapLink(p.loc) : 'Not available'),
    'Battery at last report: ' + (p.battery != null ? p.battery + '%' : 'not known'),
    ...(trail.length > 1 ? ['', 'Recent locations (newest first):', ...trail.map((h) => '• ' + secTime(h.at) + ' – ' + secMapLink(h))] : []),
    '', 'Next steps:',
    '1. File a complaint at the nearest police station (or your state police website) and keep the complaint number.',
    '2. Block the phone on CEIR (Sanchar Saathi): ceir.sancharsaathi.gov.in – needs the IMEI, the complaint number and your mobile number.',
    '3. Get a duplicate SIM for your number from your mobile operator.',
    '', 'Report made by Maata Secure on ' + secTime(now()) + '.',
  ];
  secEvent(u, 'report', 'Theft report made for ' + p.name + '.', { dev: p.id }); save();
  res.json({ text: lines.join('\n') });
});

// Free backup voice: Gemini speech (uses the same GEMINI_API_KEY). Returns a WAV sound.
const pcmToWav = (pcm, rate) => { const h = Buffer.alloc(44); h.write('RIFF', 0); h.writeUInt32LE(36 + pcm.length, 4); h.write('WAVE', 8); h.write('fmt ', 12); h.writeUInt32LE(16, 16); h.writeUInt16LE(1, 20); h.writeUInt16LE(1, 22); h.writeUInt32LE(rate, 24); h.writeUInt32LE(rate * 2, 28); h.writeUInt16LE(2, 32); h.writeUInt16LE(16, 34); h.write('data', 36); h.writeUInt32LE(pcm.length, 40); return Buffer.concat([h, pcm]); };
async function geminiTts(text, gender) {
  if (!GEMINI_KEY) return null;
  for (const model of ['gemini-2.5-flash-preview-tts', 'gemini-2.5-flash-tts', 'gemini-2.5-pro-preview-tts']) {
    if (busyUntil.get(model) > now()) continue;
    try {
      const r = await geminiFetch('models/' + model + ':generateContent', geminiAuthOk || 'header', { method: 'POST', body: JSON.stringify({ contents: [{ parts: [{ text }] }], generationConfig: { responseModalities: ['AUDIO'], speechConfig: { voiceConfig: { prebuiltVoiceConfig: { voiceName: gender === 'MALE' ? 'Charon' : 'Kore' } } } } }) }); // Kore = woman, Charon = man
      const d = await r.json().catch(() => ({}));
      if (r.status === 429 || r.status >= 500) { busyUntil.set(model, now() + 60000); console.error('[tts] ' + model + ' busy ' + r.status); continue; }
      const part = (d.candidates?.[0]?.content?.parts || []).find((x) => x.inlineData?.data);
      if (!part) { console.error('[tts] ' + model + ': ' + r.status + ' ' + String(d.error?.message || 'no audio').slice(0, 120)); continue; }
      const rate = Number((/rate=(\d+)/.exec(part.inlineData.mimeType || '') || [])[1]) || 24000;
      return pcmToWav(Buffer.from(part.inlineData.data, 'base64'), rate).toString('base64');
    } catch (e) { console.error('[tts] ' + model + ': ' + e.message); }
  }
  return null;
}
app.post('/api/tr/test', auth, async (req, res) => {
  const a = String(req.body?.audio || ''); if (!a || a.length > 1_200_000) return res.status(400).json({ error: 'No sound.' });
  if (!SPEECH_KEY && !GEMINI_KEY) return res.json({ error: 'not-configured' });
  const to = String(req.body?.to || req.user.lang || 'te').slice(0, 2), t0 = now();
  const r = SPEECH_KEY ? await speechToText(a, req.user.lang || 'en') : await aiHearTranslate(a, req.user.lang || 'en', to);
  if (SPEECH_KEY && r.text) { r.lang = scriptLang(r.text, req.user.lang || 'en'); r.translated = r.lang === to ? r.text : await translate(r.text, r.lang, to); }
  res.json({ ...r, ms: now() - t0, engine: SPEECH_KEY ? 'google' : 'ai', aiError: r.error ? aiLastError : null });
});
app.post('/api/tts', auth, async (req, res) => {
  if (!TTS_KEY) { // no Google Cloud voice key: use Gemini speech
    if (!GEMINI_KEY) return res.status(501).json({ error: 'Server voice is not set up.' });
    const text = String(req.body?.text || '').trim().slice(0, 400); if (!text) return res.status(400).json({ error: 'Nothing to speak.' });
    const list = (ttsHits.get(req.user.id) || []).filter((t) => now() - t < 60_000); list.push(now()); ttsHits.set(req.user.id, list);
    if (list.length > 25) return res.status(429).json({ error: 'Too many voice requests. Wait a minute.' });
    const gender = req.body?.gender === 'MALE' ? 'MALE' : 'FEMALE';
    const key = 'g|' + gender + '|' + text; if (ttsCache.has(key)) return res.json({ audio: ttsCache.get(key), mime: 'audio/wav' });
    const audio = await geminiTts(text, gender); if (!audio) return res.status(502).json({ error: 'Voice not available right now.' });
    ttsCache.set(key, audio); if (ttsCache.size > 300) ttsCache.delete(ttsCache.keys().next().value);
    return res.json({ audio, mime: 'audio/wav' });
  }
  const list = (ttsHits.get(req.user.id) || []).filter((t) => now() - t < 60_000);
  list.push(now()); ttsHits.set(req.user.id, list);
  if (list.length > 40) return res.status(429).json({ error: 'Too many voice requests. Wait a minute.' });
  const text = String(req.body?.text || '').trim().slice(0, 400);
  const lang = TTS_LANG[req.body?.lang];
  const gender = req.body?.gender === 'MALE' ? 'MALE' : 'FEMALE';
  if (!text || !lang) return res.status(400).json({ error: 'Nothing to speak.' });
  const key = lang + '|' + gender + '|' + text;
  if (ttsCache.has(key)) return res.json({ audio: ttsCache.get(key) });
  try {
    const r = await fetch('https://texttospeech.googleapis.com/v1/text:synthesize?key=' + encodeURIComponent(TTS_KEY), {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ input: { text }, voice: { languageCode: lang, ssmlGender: gender }, audioConfig: { audioEncoding: 'MP3', speakingRate: 1.0 } }),
      signal: AbortSignal.timeout(8000),
    });
    const d = await r.json();
    if (!d.audioContent) { console.error('[tts]', d?.error?.message || 'no audio'); return res.status(502).json({ error: 'Voice not available for this language.' }); }
    ttsCache.set(key, d.audioContent);
    if (ttsCache.size > 300) ttsCache.delete(ttsCache.keys().next().value);
    res.json({ audio: d.audioContent });
  } catch (e) { console.error('[tts]', e.message); res.status(502).json({ error: 'Voice not available right now.' }); }
});

const trHits = new Map();
const TR_NAMES = { te: 'Telugu (Telugu script)', en: 'English', hi: 'Hindi (Devanagari script)', ta: 'Tamil (Tamil script)', kn: 'Kannada (Kannada script)', ml: 'Malayalam (Malayalam script)', bn: 'Bengali (Bengali script)', mr: 'Marathi (Devanagari script)', gu: 'Gujarati (Gujarati script)', pa: 'Punjabi (Gurmukhi script)', ur: 'Urdu (Urdu script)', or: 'Odia (Odia script)' };
const aiTrCache = new Map();
async function aiTranslate(text, to) {
  const name = TR_NAMES[to]; if (!name) return null;
  const k = to + '|' + text; if (aiTrCache.has(k)) return aiTrCache.get(k);
  const r = await gemini('You are a translator for a chat app in India. Translate the user message into ' + name + '. The message may be in any language, including Indian languages typed in English letters (for example Telugu like "eppudu vastharu" or Hindi like "kab aaoge"). Keep names, numbers, phone numbers and addresses exactly. Keep the tone. Return ONLY the translation, nothing else.',
    [{ role: 'user', parts: [{ text }] }], { maxTokens: 800, temperature: 0.2 });
  if (r.error || !r.text) return null;
  const out = r.text.trim().replace(/^["“]|["”]$/g, '');
  aiTrCache.set(k, out); if (aiTrCache.size > 3000) aiTrCache.delete(aiTrCache.keys().next().value);
  return out;
}
app.post('/api/translate', auth, async (req, res) => {
  const list = (trHits.get(req.user.id) || []).filter((t) => now() - t < 60_000);
  list.push(now()); trHits.set(req.user.id, list);
  if (list.length > 60) return res.status(429).json({ error: 'Too many translations. Wait a minute.' });
  const to = String(req.body?.to || req.user.lang || 'en');
  const from = String(req.body?.from || '');
  const text = String(req.body?.text || '').trim().slice(0, 1500);
  if (!text) return res.status(400).json({ error: 'Nothing to translate.' });
  let out = LANGS.includes(to) ? await translate(text, from, to) : null;
  // The free/Google translators fail on Indian languages typed in English letters ("eppudu vastharu") or when their
  // daily limit is over — then Maata AI (Gemini) translates. It understands mixed and romanised Telugu, Hindi, Tamil…
  const unchanged = out && out.trim().toLowerCase() === text.toLowerCase();
  if ((!out || unchanged) && GEMINI_KEY) out = await aiTranslate(text, to) || out;
  if (!out) return res.status(502).json({ error: 'Translation is not available right now. Try again in a minute.' });
  res.json({ translated: out });
});

// ---------- Contacts (privacy) ----------
// Customers only see people they saved by phone number, plus anyone who already chatted with them.
const last10 = (p) => String(p || '').replace(/\D/g, '').slice(-10);
const contactsOf = (u) => (u.contacts = u.contacts || []);
const blockedOf = (u) => (u.blocked = u.blocked || []);
const hasContact = (u, id) => contactsOf(u).some((c) => c.id === id);
const watchersOf = (id) => db.users.filter((u) => u.contacts && u.contacts.some((c) => c.id === id)).map((u) => u.id);
function toWatchers(id, ev, data) { for (const w of watchersOf(id)) io.to('user:' + w).emit(ev, data); }
function hasConversation(a, b) { return db.messages.some((m) => (m.from === a && m.to === b) || (m.from === b && m.to === a)); }
function contactView(meUser, u, extra = {}) {
  const c = contactsOf(meUser).find((x) => x.id === u.id);
  const inContacts = !!c;
  return {
    ...publicUser(u),
    displayName: (c && c.name) || u.name,
    inContacts,
    blocked: blockedOf(meUser).includes(u.id),
    online: inContacts && isOnline(u.id),
    biz: u.business ? { name: u.business.name, category: u.business.category } : null,
    ...extra,
  };
}

app.get('/api/users', auth, (req, res) => {
  const me = req.user.id;
  const convo = {}; // partnerId -> { lastMessage, unread }
  const cleared = req.user.clearedChats || {}, hiddenL = new Set(req.user.hiddenMsgs || []);
  for (const m of db.messages) {
    if (m.from !== me && m.to !== me) continue;
    const other = m.from === me ? m.to : m.from;
    if (hiddenL.has(m.id) || m.ts <= (cleared[other] || 0)) continue;
    const c = (convo[other] = convo[other] || { lastMessage: null, unread: 0 });
    c.lastMessage = m;
    if (m.from === other && m.status !== 'read') c.unread++;
  }
  const ids = new Set([...contactsOf(req.user).map((c) => c.id), ...Object.keys(convo)]);
  const list = [];
  for (const id of ids) {
    const u = userById(id);
    if (!u || u.id === me || u.status === 'blocked') continue;
    list.push(contactView(req.user, u, convo[id] || { lastMessage: null, unread: 0 }));
  }
  res.json([...list, ...groupsFor(req.user)]);
});

// ---------- Call history (each customer sees only their own calls) ----------
function hasCall(a, b) { return db.calls.some((c) => (c.from === a && c.to === b) || (c.from === b && c.to === a)); }
function callView(meUser, c) {
  if (c.group) {
    const g = groupById(c.group), joined = (c.participants || []).includes(meUser.id);
    return { id: c.id, kind: c.kind, status: joined ? c.status : 'missed', direction: c.from === meUser.id ? 'out' : 'in', startedAt: c.startedAt, duration: joined ? c.duration || 0 : 0, group: true, people: (c.participants || []).length,
      other: g ? { id: 'g:' + g.id, isGroup: true, name: g.name, displayName: g.name, phone: '', photo: !!g.photo, photoV: g.photoV || 0 } : { id: 'g:' + c.group, name: c.groupName || 'Group', displayName: c.groupName || 'Group', phone: '', deleted: true, isGroup: true } };
  }
  const outgoing = c.from === meUser.id;
  const otherId = outgoing ? c.to : c.from;
  const u = userById(otherId);
  return {
    id: c.id, kind: c.kind, status: c.status, direction: outgoing ? 'out' : 'in',
    startedAt: c.startedAt, duration: c.duration || 0, translated: !!c.translated,
    other: u ? contactView(meUser, u, { lastMessage: null, unread: 0 }) : { id: otherId, name: 'Deleted user', displayName: 'Deleted user', phone: '', deleted: true },
  };
}
app.get('/api/calls', auth, (req, res) => {
  const me = req.user.id, since = req.user.callsClearedAt || 0;
  const out = [];
  for (let i = db.calls.length - 1; i >= 0 && out.length < 200; i--) {
    const c = db.calls[i];
    if ((c.from === me || c.to === me || (c.group && gMember(groupById(c.group), me))) && c.endedAt && c.startedAt > since) out.push(callView(req.user, c));
  }
  res.json(out);
});
app.delete('/api/calls', auth, (req, res) => {
  req.user.callsClearedAt = now(); save();
  res.json({ ok: true });
});

app.get('/api/users/:id', auth, (req, res) => {
  const u = userById(req.params.id);
  if (!u || u.status === 'blocked' || !(hasContact(req.user, u.id) || hasConversation(req.user.id, u.id) || hasCall(req.user.id, u.id))) return res.status(404).json({ error: 'Not found.' });
  res.json(contactView(req.user, u, { lastMessage: null, unread: 0 }));
});

// Add one contact by mobile number
app.post('/api/contacts', auth, rateLimit, (req, res) => {
  const want = last10(req.body?.phone);
  const nick = String(req.body?.name || '').trim().slice(0, 40);
  if (want.length < 10) return res.status(400).json({ error: 'Enter a valid 10-digit mobile number.' });
  const u = db.users.find((x) => last10(x.phone) === want && x.status !== 'blocked');
  if (!u) return res.status(404).json({ error: 'This number is not on Maata yet. Invite them!', notOnMaata: true });
  if (u.id === req.user.id) return res.status(400).json({ error: 'That is your own number.' });
  const list = contactsOf(req.user);
  const existing = list.find((c) => c.id === u.id);
  if (existing) { if (nick) existing.name = nick; } else list.push({ id: u.id, name: nick || '', addedAt: now() });
  save();
  res.json(contactView(req.user, u, { lastMessage: null, unread: 0 }));
});

// Match many numbers from the phone's contact list (limited, so nobody can scan all numbers)
const matchUse = new Map();
app.post('/api/contacts/match', auth, (req, res) => {
  const day = dayKey(now());
  const use = matchUse.get(req.user.id);
  const used = use && use.day === day ? use.n : 0;
  const input = Array.isArray(req.body?.contacts) ? req.body.contacts.slice(0, 500) : [];
  const pairs = [];
  for (const c of input) for (const ph of (Array.isArray(c?.phones) ? c.phones.slice(0, 5) : [])) {
    const n = last10(ph); if (n.length === 10) pairs.push({ n, name: String(c?.name || '').trim().slice(0, 40) });
  }
  if (used + pairs.length > 2000) return res.status(429).json({ error: 'Daily contact sync limit reached. Try again tomorrow.' });
  matchUse.set(req.user.id, { day, n: used + pairs.length });
  const byPhone = new Map(db.users.filter((u) => u.status !== 'blocked' && u.id !== req.user.id).map((u) => [last10(u.phone), u]));
  const list = contactsOf(req.user);
  const added = [];
  const seen = new Set();
  for (const { n, name } of pairs) {
    const u = byPhone.get(n);
    if (!u || seen.has(u.id)) continue;
    seen.add(u.id);
    const ex = list.find((c) => c.id === u.id);
    if (ex) { if (name && !ex.name) ex.name = name; } else list.push({ id: u.id, name, addedAt: now() });
    added.push(contactView(req.user, u, { lastMessage: null, unread: 0 }));
  }
  save();
  res.json({ checked: pairs.length, found: added.length, users: added });
});

// Look up one mobile number: is this person on Maata? (limited per user, so nobody can scan numbers)
const lookupUse = new Map();
app.get('/api/lookup', auth, (req, res) => {
  const want = last10(req.query.phone);
  if (want.length < 10) return res.status(400).json({ error: 'Enter a full 10-digit mobile number.' });
  const t = now(), l = (lookupUse.get(req.user.id) || []).filter((x) => t - x < 24 * 3600_000);
  if (l.filter((x) => t - x < 3600_000).length >= 40 || l.length >= 200) return res.status(429).json({ error: 'Too many number searches. Try again later.' });
  l.push(t); lookupUse.set(req.user.id, l);
  const u = db.users.find((x) => last10(x.phone) === want && x.status !== 'blocked');
  if (!u || blockedOf(u).includes(req.user.id)) return res.json({ found: false });
  if (u.id === req.user.id) return res.json({ found: false, self: true });
  res.json({ found: true, user: contactView(req.user, u, { lastMessage: null, unread: 0 }) });
});

app.delete('/api/contacts/:id', auth, (req, res) => {
  req.user.contacts = contactsOf(req.user).filter((c) => c.id !== req.params.id);
  save(); res.json({ ok: true });
});

app.post('/api/block/:id', auth, (req, res) => {
  const u = userById(req.params.id);
  if (!u || u.id === req.user.id) return res.status(404).json({ error: 'Not found.' });
  const list = blockedOf(req.user);
  const block = req.body?.blocked !== false;
  req.user.blocked = block ? [...new Set([...list, u.id])] : list.filter((x) => x !== u.id);
  save(); res.json({ ok: true, blocked: block });
});

app.get('/api/messages/:userId', auth, (req, res) => {
  const me = req.user.id, other = req.params.userId;
  const gid = gidOf(other);
  if (gid) {
    if (!gMember(groupById(gid), me)) return res.status(404).json({ error: 'Group not found.' });
    const hidden = new Set(req.user.hiddenMsgs || []), since = (req.user.clearedChats || {})[other] || 0;
    return res.json(db.messages.filter((m) => m.group === gid && !hidden.has(m.id) && m.ts > since && !(m.expiresAt && m.expiresAt <= now())).slice(-300));
  }
  const hidden = new Set(req.user.hiddenMsgs || []), since = (req.user.clearedChats || {})[other] || 0;
  res.json(db.messages.filter((m) => ((m.from === me && m.to === other) || (m.from === other && m.to === me)) && !hidden.has(m.id) && m.ts > since && !(m.expiresAt && m.expiresAt <= now())).slice(-300));
});

// ---------- Chat media is kept on the server for MEDIA_KEEP_DAYS (default 30), then only on phones ----------
// Existing media gets the same grace period counted from the day this feature was first switched on.
const MEDIA_KEEP_DAYS = process.env.MEDIA_KEEP_DAYS === undefined ? 30 : Math.max(0, Number(process.env.MEDIA_KEEP_DAYS) || 0);
let purging = false;
async function purgeOldMedia() {
  if (!MEDIA_KEEP_DAYS || purging || !db.meta) return;
  purging = true;
  try {
    let meta = db.meta.find((x) => x.id === 'mediaPurge');
    if (!meta) { meta = { id: 'mediaPurge', since: now(), freed: 0, files: 0, createdAt: now() }; db.meta.push(meta); save(); }
    const cutoff = now() - MEDIA_KEEP_DAYS * 86400000;
    if (meta.since > cutoff) return; // still inside the first grace period
    let n = 0;
    for (const m of db.messages) {
      if (!m.file || m.file.purged || m.ts > cutoff || !m.file.id) continue;
      try { await deleteChatFile(m.file.id); } catch { /* already gone */ }
      m.file.purged = true; m.file.purgedAt = now(); meta.freed += m.file.size || 0; meta.files++; n++;
      if (n >= 400) break; // a little at a time
    }
    if (n) { meta.lastRun = now(); save(); console.log('[media] removed ' + n + ' old chat files from the server (phones keep their copies)'); }
  } finally { purging = false; }
}
setTimeout(() => purgeOldMedia().catch(() => {}), 90_000); setInterval(() => purgeOldMedia().catch(() => {}), 3600_000);
app.get('/admin/api/storage', adminAuth, (req, res) => {
  const meta = (db.meta || []).find((x) => x.id === 'mediaPurge') || null;
  let files = 0, bytes = 0; for (const m of db.messages) if (m.file && !m.file.purged) { files++; bytes += m.file.size || 0; }
  const reels = db.reels.reduce((n, r) => n + (r.file ? r.file.size || 0 : 0), 0);
  res.json({ keepDays: MEDIA_KEEP_DAYS, chatFiles: files, chatBytes: bytes, reelBytes: reels, reels: db.reels.length, freedFiles: meta ? meta.files : 0, freedBytes: meta ? meta.freed : 0,
    graceUntil: meta ? meta.since + MEDIA_KEEP_DAYS * 86400000 : null, lastRun: meta ? meta.lastRun || null : null, r2: R2_ON });
});
app.get('/api/media-policy', auth, (req, res) => { const meta = (db.meta || []).find((x) => x.id === 'mediaPurge'); res.json({ keepDays: MEDIA_KEEP_DAYS, graceUntil: meta ? meta.since + MEDIA_KEEP_DAYS * 86400000 : null }); });

// ---------- 🪙 Referral coins: a friend joins with your invite and uses Maata on 3 different days → you get 1 coin ----------
const REF_DAYS = 3, REF_DAY_CAP = 10; // at most 10 coins a day per person (stops fake-account farming)
const istDay = (t = now()) => new Date(t + 5.5 * 3600000).toISOString().slice(0, 10);
function addCoins(u, amount, entry) {
  u.coins = Math.max(0, (u.coins || 0) + amount);
  (u.coinLog = u.coinLog || []).unshift({ amount, ts: now(), ...entry }); if (u.coinLog.length > 200) u.coinLog.length = 200;
}
function markActive(u) {
  if (!u) return;
  const d = istDay(); u.activeDays = u.activeDays || [];
  if (u.activeDays.includes(d)) return;
  u.activeDays.push(d); if (u.activeDays.length > 30) u.activeDays = u.activeDays.slice(-30);
  checkReferral(u); save();
}
function checkReferral(u) {
  if (!u.referredBy || u.referralRewarded || (u.activeDays || []).length < REF_DAYS || !featOn('coins')) return;
  const inv = userById(u.referredBy); if (!inv || inv.status === 'blocked') return;
  const today = istDay(); const given = (inv.coinLog || []).filter((x) => x.type === 'referral' && istDay(x.ts) === today).length;
  if (given >= REF_DAY_CAP) return; // tried again on their next active day
  u.referralRewarded = now();
  addCoins(inv, 1, { type: 'referral', friend: u.name, friendId: u.id });
  io.to('user:' + inv.id).emit('coins:update', { coins: inv.coins, text: '🪙 +1 coin! ' + u.name + ' used Maata for ' + REF_DAYS + ' days.' });
  try { pushTo(inv, { type: 'msg', title: '🪙 +1 Maata coin', body: u.name + ' used Maata for ' + REF_DAYS + ' days. You now have ' + inv.coins + ' coins.', tag: 'coins' }); } catch {}
}
app.get('/api/coins', auth, (req, res) => {
  const me2 = req.user;
  const friends = db.users.filter((x) => x.referredBy === me2.id).sort((a, b) => b.createdAt - a.createdAt).slice(0, 50)
    .map((x) => ({ name: x.name, joinedAt: x.createdAt, days: Math.min(REF_DAYS, (x.activeDays || []).length), done: !!x.referralRewarded }));
  res.json({ coins: me2.coins || 0, need: REF_DAYS, friends, log: (me2.coinLog || []).slice(0, 50) });
});
// Admin: every customer's coins, the friends they invited, and whether those friends are still active
app.get('/admin/api/referrals', adminAuth, (req, res) => {
  const t = now(), DAY = 86400000;
  const friendOut = (x) => {
    const last = isOnline(x.id) ? t : (x.lastSeenAt || x.lastLoginAt || x.createdAt);
    const ago = Math.floor((t - last) / DAY);
    return { id: x.id, name: x.name, phone: x.phone, joinedAt: x.createdAt, activeDays: (x.activeDays || []).length, lastSeen: last, online: isOnline(x.id),
      state: x.status === 'blocked' ? 'blocked' : isOnline(x.id) ? 'online' : ago <= 7 ? 'active' : 'inactive', daysAgo: ago, rewarded: !!x.referralRewarded };
  };
  const byInviter = new Map();
  for (const x of db.users) if (x.referredBy) { if (!byInviter.has(x.referredBy)) byInviter.set(x.referredBy, []); byInviter.get(x.referredBy).push(x); }
  const rows = db.users.filter((u) => (u.coins || 0) > 0 || byInviter.has(u.id) || (u.coinLog || []).length).map((u) => {
    const fr = (byInviter.get(u.id) || []).sort((a, b) => b.createdAt - a.createdAt).map(friendOut);
    return { id: u.id, name: u.name, phone: u.phone, coins: u.coins || 0, invited: fr.length, earned: fr.filter((f) => f.rewarded).length, pending: fr.filter((f) => !f.rewarded).length,
      activeFriends: fr.filter((f) => f.state === 'online' || f.state === 'active').length, bonus: (u.coinLog || []).filter((l) => l.type === 'admin').reduce((n, l) => n + l.amount, 0), friends: fr };
  }).sort((a, b) => b.coins - a.coins || b.invited - a.invited);
  const all = rows.flatMap((r) => r.friends);
  res.json({ totals: { coins: rows.reduce((n, r) => n + r.coins, 0), inviters: rows.filter((r) => r.invited).length, joined: all.length, earned: all.filter((f) => f.rewarded).length, active: all.filter((f) => f.state === 'online' || f.state === 'active').length }, rows });
});
app.post('/admin/api/users/:id/coins', adminAuth, (req, res) => {
  const u = userById(req.params.id); if (!u) return res.status(404).json({ error: 'Not found.' });
  const delta = Math.trunc(Number(req.body?.delta) || 0); if (!delta || Math.abs(delta) > 1000) return res.status(400).json({ error: 'Enter how many coins to add or remove.' });
  addCoins(u, delta, { type: 'admin', note: clip(req.body?.reason, 100) || (delta > 0 ? 'Bonus from Maata' : 'Adjusted by Maata') }); save();
  io.to('user:' + u.id).emit('coins:update', { coins: u.coins, text: delta > 0 ? '🪙 +' + delta + ' coins from Maata' : '🪙 Coins updated' });
  audit(req.admin, 'coins', u.name + ' ' + (delta > 0 ? '+' : '') + delta, clip(req.body?.reason, 100));
  res.json({ coins: u.coins });
});

// ---------- Invite a friend ----------
function inviteCodeOf(u) {
  if (!u.inviteCode) { let c; do { c = crypto.randomBytes(4).toString('base64').replace(/[^A-Z0-9]/gi, '').toUpperCase().slice(0, 6); } while (c.length < 6 || db.users.some((x) => x.inviteCode === c)); u.inviteCode = c; save(); }
  return u.inviteCode;
}
app.get('/api/invite', auth, async (req, res) => {
  const code = inviteCodeOf(req.user), base = (process.env.APP_ORIGIN || ('https://' + req.headers.host)).replace(/\/$/, '');
  const joined = db.users.filter((x) => x.referredBy === req.user.id).sort((a, b) => b.createdAt - a.createdAt);
  let qr = ''; try { qr = await QRCode.toDataURL(base + '/?ref=' + code, { margin: 2, width: 360, color: { dark: '#0A2E36', light: '#FFFFFF' } }); } catch {}
  res.json({ code, link: base + '/?ref=' + code, site: base, qr, joined: joined.length, recent: joined.slice(0, 10).map((x) => ({ name: x.name, at: x.createdAt })) });
});
app.get('/api/invite/:code', (req, res) => { const u = db.users.find((x) => x.inviteCode === String(req.params.code).toUpperCase()); res.json(u ? { name: u.name } : {}); });

// ---------- Chat backup (saved by the customer to Google Drive / phone) ----------
app.get('/api/backup/export', auth, (req, res) => {
  const me = req.user.id, hidden = new Set(req.user.hiddenMsgs || []), cleared = req.user.clearedChats || {}, t = now();
  const myGroups = new Set(db.groups.filter((g) => gMember(g, me)).map((g) => g.id));
  const chats = new Map();
  const nameFor = (id) => { const c = contactsOf(req.user).find((x) => x.id === id); const u = userById(id); return (c && c.name) || (u && u.name) || 'Maata user'; };
  for (const m of db.messages) {
    let key;
    if (m.group) { if (!myGroups.has(m.group)) continue; key = 'g:' + m.group; }
    else if (m.from === me) key = m.to; else if (m.to === me) key = m.from; else continue;
    if (hidden.has(m.id) || m.ts <= (cleared[key] || 0) || (m.expiresAt && m.expiresAt <= t) || m.viewOnce || m.deleted) continue;
    if (!chats.has(key)) { const g = m.group && groupById(m.group), u = !m.group && userById(key); chats.set(key, { id: key, name: g ? g.name : nameFor(key), phone: u ? u.phone : '', isGroup: !!g, messages: [] }); }
    chats.get(key).messages.push({ id: m.id, from: m.from === me ? 'me' : m.from, fromName: m.from === me ? req.user.name : nameFor(m.from), ts: m.ts, type: m.type || 'text', text: m.text || '',
      ...(m.file ? { file: { id: m.file.id, name: m.file.name, mime: m.file.mime, size: m.file.size } } : {}), ...(m.product ? { product: m.product } : {}), ...(m.location ? { location: m.location } : {}) });
  }
  const list = [...chats.values()].sort((a, b) => (b.messages.at(-1)?.ts || 0) - (a.messages.at(-1)?.ts || 0));
  let total = 0; for (const c of list) { if (c.messages.length > 5000) c.messages = c.messages.slice(-5000); total += c.messages.length; }
  res.json({ app: 'Maata', version: 1, exportedAt: t, me: { id: me, name: req.user.name, phone: req.user.phone }, chats: list, totalMessages: total });
});
app.post('/api/backup/done', auth, (req, res) => { req.user.lastBackup = { at: now(), size: Math.max(0, Number(req.body?.size) || 0), messages: Math.max(0, Number(req.body?.messages) || 0), media: !!req.body?.media, where: clip(req.body?.where, 30) }; save(); res.json(req.user.lastBackup); });
app.get('/api/backup/info', auth, (req, res) => res.json(req.user.lastBackup || null));

app.get('/api/ice', auth, (req, res) => {
  const ice = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
  if (process.env.TURN_URL) ice.push({ urls: process.env.TURN_URL.split(','), username: process.env.TURN_USERNAME || '', credential: process.env.TURN_PASSWORD || '' });
  res.json(ice);
});

app.get('/api/announcement', auth, (req, res) => {
  const a = db.announcements.find((x) => x.active);
  res.json(a ? { id: a.id, text: a.text, ts: a.ts } : null);
});

app.post('/api/report', auth, rateLimit, (req, res) => {
  const target = userById(String(req.body?.userId || ''));
  const reason = String(req.body?.reason || '').trim().slice(0, 500);
  if (!target || target.id === req.user.id) return res.status(400).json({ error: 'Choose a valid user to report.' });
  if (reason.length < 3) return res.status(400).json({ error: 'Tell us briefly what happened.' });
  const r = { id: newId(), reporter: req.user.id, target: target.id, reason, ts: now(), status: 'open' };
  db.reports.push(r); save();
  feed('report', `${req.user.name} reported ${target.name}`);
  io.to('admins').emit('report:new', r);
  res.json({ ok: true });
});

// ---------- Status (text, photo, video - disappears after 24 hours) ----------
// Only mutual contacts (you saved them AND they saved you) can see each other's status.
const STATUS_TTL = 24 * 3600 * 1000;
async function saveMedia(id, buf, mime) {
  if (bucket) {
    await new Promise((res, rej) => { const up = bucket.openUploadStreamWithId(id, id, { contentType: mime }); up.on('error', rej).on('finish', res); up.end(buf); });
  } else { fs.mkdirSync(MEDIA_DIR, { recursive: true }); fs.writeFileSync(path.join(MEDIA_DIR, id), buf); }
}
async function deleteMedia(id) { try { if (bucket) await bucket.delete(id); else fs.unlinkSync(path.join(MEDIA_DIR, id)); } catch { /* already gone */ } }
function mediaStream(id, start, end) {
  return bucket ? bucket.openDownloadStream(id, { start, end: end + 1 }) : fs.createReadStream(path.join(MEDIA_DIR, id), { start, end });
}
const isMutual = (a, b) => hasContact(a, b.id) && hasContact(b, a.id);
function statusVisible(viewer, st) {
  if (st.userId === viewer.id) return true;
  const owner = userById(st.userId);
  if (!owner || owner.status === 'blocked' || blockedOf(owner).includes(viewer.id) || blockedOf(viewer).includes(owner.id)) return false;
  const pv = owner.statusPrivacy || { mode: 'contacts', list: [] };
  if (pv.mode === 'only') return pv.list.includes(viewer.id) && hasContact(owner, viewer.id);
  if (pv.mode === 'except' && pv.list.includes(viewer.id)) return false;
  return isMutual(viewer, owner);
}
function statusOut(viewer, st) {
  const mine = st.userId === viewer.id;
  return {
    id: st.id, userId: st.userId, type: st.type, text: st.text || '', bg: st.bg || null, caption: st.caption || '', mime: st.mime || null,
    createdAt: st.createdAt, expiresAt: st.expiresAt,
    viewed: mine || (st.views || []).some((v) => v.userId === viewer.id),
    ...(mine ? { viewCount: (st.views || []).length } : {}),
    mediaUrl: st.type === 'text' ? null : '/api/status/media/' + st.id,
  };
}
function notifyStatus(u) { for (const w of watchersOf(u.id)) if (hasContact(u, w)) io.to('user:' + w).emit('status:new', { userId: u.id }); }
const tooManyStatuses = (u) => db.statuses.filter((x) => x.userId === u.id && x.createdAt > now() - STATUS_TTL).length >= 30;

app.get('/api/status', auth, (req, res) => {
  const mine = [], others = {};
  for (const st of db.statuses) {
    if (st.expiresAt <= now()) continue;
    if (st.userId === req.user.id) mine.push(statusOut(req.user, st));
    else if (statusVisible(req.user, st)) (others[st.userId] = others[st.userId] || []).push(statusOut(req.user, st));
  }
  const users = Object.entries(others).map(([uid, list]) => ({
    user: contactView(req.user, userById(uid), { lastMessage: null, unread: 0 }),
    statuses: list, allViewed: list.every((x) => x.viewed), latest: list[list.length - 1].createdAt,
  })).sort((a, b) => (a.allViewed - b.allViewed) || (b.latest - a.latest));
  res.json({ mine, users });
});

app.post('/api/status/text', auth, (req, res) => {
  const text = String(req.body?.text || '').trim().slice(0, 700);
  if (!text) return res.status(400).json({ error: 'Write something first.' });
  if (tooManyStatuses(req.user)) return res.status(429).json({ error: 'You can post up to 30 status updates a day.' });
  const bg = /^#[0-9a-fA-F]{6}$/.test(req.body?.bg || '') ? req.body.bg : '#0F4C5C';
  const st = { id: newId(), userId: req.user.id, type: 'text', text, bg, createdAt: now(), expiresAt: now() + STATUS_TTL, views: [] };
  db.statuses.push(st); save(); notifyStatus(req.user);
  res.json(statusOut(req.user, st));
});

app.post('/api/status/media', auth, express.raw({ type: ['image/*', 'video/*'], limit: '16mb' }), async (req, res) => {
  const mime = String(req.headers['content-type'] || '').split(';')[0].trim();
  const type = mime.startsWith('image/') ? 'image' : mime.startsWith('video/') ? 'video' : null;
  if (!type || !Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Choose a photo or video.' });
  if (tooManyStatuses(req.user)) return res.status(429).json({ error: 'You can post up to 30 status updates a day.' });
  const id = newId();
  try { await saveMedia(id, req.body, mime); } catch (e) { console.error('[media]', e.message); return res.status(500).json({ error: 'Could not save the file. Try a smaller one.' }); }
  const st = { id, userId: req.user.id, type, caption: String(req.query.caption || '').trim().slice(0, 300), mime, size: req.body.length, createdAt: now(), expiresAt: now() + STATUS_TTL, views: [] };
  db.statuses.push(st); save(); notifyStatus(req.user);
  res.json(statusOut(req.user, st));
});

// <img>/<video> cannot send headers, so the login token comes in ?t=
app.get('/api/status/media/:id', (req, res) => {
  let viewer = null;
  try { const h = req.headers.authorization || ''; viewer = findUserByToken(h.startsWith('Bearer ') ? h.slice(7) : String(req.query.t || '')); } catch { /* invalid */ }
  if (!viewer) return res.status(401).end();
  const st = db.statuses.find((x) => x.id === req.params.id && x.type !== 'text');
  if (!st || st.expiresAt <= now() || !statusVisible(viewer, st)) return res.status(404).end();
  const size = st.size;
  let start = 0, end = size - 1, code = 200;
  const m = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
  if (m) {
    if (m[1]) { start = parseInt(m[1], 10); end = m[2] ? Math.min(parseInt(m[2], 10), size - 1) : size - 1; }
    else if (m[2]) { start = Math.max(0, size - parseInt(m[2], 10)); }
    if (start > end || start >= size) { res.setHeader('Content-Range', `bytes */${size}`); return res.status(416).end(); }
    code = 206; res.setHeader('Content-Range', `bytes ${start}-${end}/${size}`);
  }
  res.status(code);
  res.setHeader('Content-Type', st.mime); res.setHeader('Accept-Ranges', 'bytes');
  res.setHeader('Content-Length', end - start + 1); res.setHeader('Cache-Control', 'private, max-age=86400');
  const stream = mediaStream(st.id, start, end);
  stream.on('error', () => res.destroy());
  stream.pipe(res);
});

app.post('/api/status/:id/view', auth, (req, res) => {
  const st = db.statuses.find((x) => x.id === req.params.id);
  if (!st || st.expiresAt <= now() || !statusVisible(req.user, st)) return res.status(404).json({ error: 'Not found.' });
  st.views = st.views || [];
  if (st.userId !== req.user.id && !st.views.some((v) => v.userId === req.user.id)) {
    st.views.push({ userId: req.user.id, at: now() }); save();
    io.to('user:' + st.userId).emit('status:view', { id: st.id, count: st.views.length });
  }
  res.json({ ok: true });
});
app.get('/api/status/:id/views', auth, (req, res) => {
  const st = db.statuses.find((x) => x.id === req.params.id && x.userId === req.user.id);
  if (!st) return res.status(404).json({ error: 'Not found.' });
  res.json((st.views || []).slice().reverse().map((v) => { const u = userById(v.userId); return { at: v.at, user: u ? contactView(req.user, u) : { id: v.userId, name: 'Deleted user', displayName: 'Deleted user' } }; }));
});
app.delete('/api/status/:id', auth, async (req, res) => {
  const st = db.statuses.find((x) => x.id === req.params.id && x.userId === req.user.id);
  if (!st) return res.status(404).json({ error: 'Not found.' });
  db.statuses = db.statuses.filter((x) => x.id !== st.id); save();
  if (st.type !== 'text') await deleteMedia(st.id);
  res.json({ ok: true });
});
function cleanStatuses() {
  const t = now(), old = db.statuses.filter((x) => x.expiresAt <= t);
  if (!old.length) return;
  db.statuses = db.statuses.filter((x) => x.expiresAt > t);
  for (const x of old) if (x.type !== 'text') deleteMedia(x.id);
  save();
}
setInterval(cleanStatuses, 10 * 60 * 1000);

// ---------- Chat attachments & rich messages ----------
// Message types: text, image, video, audio, file (documents), contact, poll, event, location (current or live).
const MAX_FILE = 16 * 1024 * 1024;
async function saveChatFile(id, buf, mime) {
  if (chatBucket) await new Promise((res, rej) => { const up = chatBucket.openUploadStreamWithId(id, id, { contentType: mime }); up.on('error', rej).on('finish', res); up.end(buf); });
  else { fs.mkdirSync(CHAT_DIR, { recursive: true }); fs.writeFileSync(path.join(CHAT_DIR, id), buf); }
}
async function deleteChatFile(id) { if (typeof memFiles !== 'undefined' && memFiles.has(id)) { memBytes -= memFiles.get(id).length; memFiles.delete(id); } if (isR2(id)) return r2Delete(id); try { if (chatBucket) await chatBucket.delete(id); else fs.unlinkSync(path.join(CHAT_DIR, id)); } catch { /* gone */ } }
const chatFileStream = (id, start, end) => (chatBucket ? chatBucket.openDownloadStream(id, { start, end: end + 1 }) : fs.createReadStream(path.join(CHAT_DIR, id), { start, end }));

// Checks both people and the blocks; returns an error text or null
function canMessage(fromUser, to) {
  const gid = gidOf(to);
  if (gid) {
    const g = groupById(gid);
    if (!gMember(g, fromUser.id)) return { error: 'You are not a member of this group.' };
    if (g.settings.onlyAdminsMessage && !gAdmin(g, fromUser.id)) return { error: 'Only admins can send messages in this group.' };
    return { target: { id: 'g:' + gid, name: g.name, isGroup: true }, group: g };
  }
  const target = userById(to);
  if (!target || to === fromUser.id || target.status === 'blocked') return { error: 'Message not sent.' };
  if (blockedOf(fromUser).includes(to)) return { error: 'You blocked this person. Unblock them to send messages.' };
  if (blockedOf(target).includes(fromUser.id)) return { error: 'Message not delivered.' };
  return { target };
}
function deliverMessage(fromUser, target, fields, exceptSocketId) {
  if (target.isGroup) {
    const g = groupById(target.id.slice(2));
    const anyOn = g.members.some((x) => x.id !== fromUser.id && isOnline(x.id));
    const ttl = fields.type === 'system' ? 0 : g.disappearing || 0;
    const m = { id: newId(), from: fromUser.id, to: target.id, group: g.id, ts: now(), status: anyOn ? 'delivered' : 'sent', readBy: {}, type: 'text', text: '', ...(ttl ? { expiresAt: now() + ttl * 1000 } : {}), ...fields };
    db.messages.push(m); save();
    (exceptSocketId ? io.to('grp:' + g.id).except(exceptSocketId) : io.to('grp:' + g.id)).emit('msg:new', m);
    notifyOffline(fromUser, m);
    if (m.type !== 'system') feed('message', `${fromUser.name} → group ${g.name}: ${m.type === 'text' ? 'message' : m.type}`);
    return m;
  }
  const online2 = isOnline(target.id);
  const ttl = fields.type === 'system' ? 0 : (fromUser.disappearing || {})[target.id] || 0;
  const m = { id: newId(), from: fromUser.id, to: target.id, ts: now(), status: online2 ? 'delivered' : 'sent', ...(online2 ? { deliveredAt: now() } : {}), ...(ttl ? { expiresAt: now() + ttl * 1000 } : {}), type: 'text', text: '', ...fields };
  db.messages.push(m); save();
  io.to('user:' + target.id).emit('msg:new', m);
  if (!online2) notifyOffline(fromUser, m);
  (exceptSocketId ? io.to('user:' + fromUser.id).except(exceptSocketId) : io.to('user:' + fromUser.id)).emit('msg:new', m);
  feed('message', `${fromUser.name} → ${target.name}: ${m.type === 'text' ? 'message' : m.type}`);
  return m;
}
// A file can be shared by several messages (forwards); delete it only when none is left
function releaseFile(fileId) { if (!db.messages.some((x) => x.file && x.file.id === fileId)) deleteChatFile(fileId); }
app.get('/api/starred', auth, (req, res) => res.json(req.user.starred || []));

// ---------- Chat list tools: starred, mark read, delete chats, quick replies, lists, broadcast ----------
app.get('/api/starred/messages', auth, (req, res) => {
  const me = req.user.id, ids = new Set(req.user.starred || []), hidden = new Set(req.user.hiddenMsgs || []);
  const out = [];
  for (let i = db.messages.length - 1; i >= 0 && out.length < 300; i--) {
    const m = db.messages[i];
    if (!ids.has(m.id) || hidden.has(m.id) || m.deleted || (m.from !== me && m.to !== me)) continue;
    const other = userById(m.from === me ? m.to : m.from);
    out.push({ message: m, other: other ? contactView(req.user, other) : { id: '', name: 'Deleted user', displayName: 'Deleted user', phone: '' } });
  }
  res.json(out);
});
function markRead(meUser, fromIds) {
  const set = new Set(fromIds), bySender = {};
  for (const m of db.messages) {
    if (m.to === meUser.id && set.has(m.from) && m.status !== 'read') {
      m.status = 'read'; m.readAt = now(); if (!m.deliveredAt) m.deliveredAt = m.readAt;
      (bySender[m.from] = bySender[m.from] || []).push(m.id);
    }
  }
  for (const [from, ids] of Object.entries(bySender)) io.to('user:' + from).emit('msg:status', { ids, status: 'read' });
  if (Object.keys(bySender).length) save();
}
app.post('/api/chats/read', auth, (req, res) => {
  let ids = Array.isArray(req.body?.ids) ? req.body.ids.map(String) : [];
  if (req.body?.all) ids = [...new Set(db.messages.filter((m) => m.to === req.user.id && m.status !== 'read').map((m) => m.from))];
  markRead(req.user, ids.slice(0, 5000));
  res.json({ ok: true });
});
app.post('/api/chats/delete', auth, (req, res) => {
  const ids = (Array.isArray(req.body?.ids) ? req.body.ids : []).map(String).slice(0, 500);
  req.user.clearedChats = req.user.clearedChats || {};
  for (const id of ids) req.user.clearedChats[id] = now();
  if (req.body?.removeContacts) req.user.contacts = contactsOf(req.user).filter((c) => !ids.includes(c.id));
  save(); res.json({ ok: true });
});
app.get('/api/prefs', auth, (req, res) => res.json({ quickReplies: req.user.quickReplies || [], lists: req.user.lists || [], favs: req.user.favs || [], stickers: req.user.stickers || [], muted: req.user.muted || [] }));
app.put('/api/prefs', auth, (req, res) => {
  const b = req.body || {};
  if (Array.isArray(b.quickReplies)) {
    req.user.quickReplies = b.quickReplies.slice(0, 50).map((q) => ({ shortcut: String(q?.shortcut || '').replace(/[^\p{L}\p{N}_-]/gu, '').slice(0, 25), text: String(q?.text || '').slice(0, 1000) })).filter((q) => q.shortcut && q.text);
  }
  if (Array.isArray(b.lists)) {
    req.user.lists = b.lists.slice(0, 20).map((l) => ({ id: String(l?.id || newId()).slice(0, 40), name: String(l?.name || '').trim().slice(0, 30), members: (Array.isArray(l?.members) ? l.members : []).map(String).slice(0, 500) })).filter((l) => l.name);
  }
  if (Array.isArray(b.favs)) req.user.favs = b.favs.map(String).slice(0, 500);
  if (Array.isArray(b.muted)) req.user.muted = b.muted.map(String).slice(0, 500);
  if (Array.isArray(b.stickers)) req.user.stickers = b.stickers.map(String).filter((x) => /^[\w-]{8,64}$/.test(x)).slice(-60);
  save();
  res.json({ quickReplies: req.user.quickReplies || [], lists: req.user.lists || [], favs: req.user.favs || [], stickers: req.user.stickers || [], muted: req.user.muted || [] });
});
// GIF search through Tenor (set TENOR_KEY, or enable "Tenor API" on the same Google key)
const TENOR_KEY = process.env.TENOR_KEY || process.env.GOOGLE_TRANSLATE_KEY || '';
const gifCache = new Map();
app.get('/api/gifs', auth, async (req, res) => {
  if (!TENOR_KEY) return res.status(501).json({ error: 'GIFs are not set up yet.' });
  const q = String(req.query.q || '').trim().slice(0, 50);
  const key = q.toLowerCase() || '__featured';
  const c = gifCache.get(key); if (c && now() - c.t < 600000) return res.json(c.list);
  try {
    const base = 'https://tenor.googleapis.com/v2/' + (q ? 'search' : 'featured');
    const u = base + '?' + new URLSearchParams({ key: TENOR_KEY, client_key: 'maata', limit: '30', media_filter: 'gif,tinygif', contentfilter: 'medium', country: 'IN', ...(q ? { q } : {}) });
    const d = await (await fetch(u, { signal: AbortSignal.timeout(8000) })).json();
    if (d.error) { console.error('[gif]', d.error.message); return res.status(502).json({ error: 'GIF search is not available right now.' }); }
    const list = (d.results || []).map((r) => ({ id: r.id, url: r.media_formats?.gif?.url, preview: r.media_formats?.tinygif?.url, w: r.media_formats?.gif?.dims?.[0], h: r.media_formats?.gif?.dims?.[1], title: r.content_description || '' })).filter((x) => x.url && x.preview);
    gifCache.set(key, { t: now(), list }); if (gifCache.size > 300) gifCache.delete(gifCache.keys().next().value);
    res.json(list);
  } catch (e) { console.error('[gif]', e.message); res.status(502).json({ error: 'GIF search is not available right now.' }); }
});

// ---------- GPS photo stamp: address + map tile (OpenStreetMap, cached, polite rate) ----------
const OSM_UA = { 'User-Agent': 'MaataApp/1.0 (GPS photo stamp)', 'Accept-Language': 'en' };
const geoCache = new Map(), tileCache = new Map(), geoHits = new Map();
let lastNominatim = 0;
function geoLimit(req, res) {
  const l = (geoHits.get(req.user.id) || []).filter((t) => now() - t < 60_000); l.push(now()); geoHits.set(req.user.id, l);
  if (l.length > 40) { res.status(429).json({ error: 'Too many location requests. Wait a minute.' }); return false; }
  return true;
}
// ---------- Version: the same number is written in index.html, so the app can tell when both files are updated ----------
const MAATA_VERSION = 173;
// remembers which Maata Android app version each person last opened, so the app can say "Update successful" after an update
app.post('/api/app-seen', auth, (req, res) => {
  const ver = String(req.body?.ver || '').slice(0, 20); if (!/^\d+\.\d+\.\d+$/.test(ver)) return res.json({ prev: '' });
  const prev = req.user.androidVer || ''; if (prev !== ver) { req.user.androidVer = ver; req.user.androidVerAt = now(); save(); }
  res.json({ prev });
});
app.get('/api/version', (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  let android = 0; for (const f of [path.join(PAGES, 'app-version.json'), path.join(__dirname, 'app-version.json')]) { try { android = JSON.parse(fs.readFileSync(f, 'utf8')).version || 0; break; } catch { /* next */ } }
  res.json({ server: MAATA_VERSION, android });
});
// ---------- 🗺️ Maata Maps: places added by Maata people (villages often miss shops on public maps) + Maata shops ----------
const PLACE_CATS = ['shop', 'food', 'fuel', 'hospital', 'pharmacy', 'temple', 'school', 'atm', 'mechanic', 'hotel', 'office', 'other'];
const placeOut = (x, me) => { const o = userById(x.by) || {}; const biz = x.biz && o.business ? { ownerId: o.id, name: o.business.name, category: o.business.category, phone: o.phone } : null;
  return { id: x.id, name: x.name, cat: x.cat, lat: x.lat, lng: x.lng, phone: x.phone || '', note: x.note || '', byName: o.name || 'Maata user', mine: x.by === me, biz, createdAt: x.createdAt }; };
app.get('/api/maps/places', auth, (req, res) => {
  const lat = Number(req.query.lat), lng = Number(req.query.lng); if (!(Math.abs(lat) <= 90 && Math.abs(lng) <= 180)) return res.json([]);
  const dLat = 0.3, dLng = 0.3 / Math.max(0.2, Math.cos(lat * Math.PI / 180));
  res.json(db.mapPlaces.filter((x) => !x.hidden && Math.abs(x.lat - lat) < dLat && Math.abs(x.lng - lng) < dLng).slice(-300).map((x) => placeOut(x, req.user.id)));
});
app.get('/api/maps/places/mine', auth, (req, res) => res.json(db.mapPlaces.filter((x) => x.by === req.user.id && !x.hidden).map((x) => placeOut(x, req.user.id))));
app.post('/api/maps/places', auth, (req, res) => {
  const today = db.mapPlaces.filter((x) => x.by === req.user.id && now() - x.createdAt < 86400000).length;
  if (today >= 20) return res.status(429).json({ error: 'You can add up to 20 places a day.' });
  const name = clip(req.body?.name, 80), lat = Number(req.body?.lat), lng = Number(req.body?.lng);
  if (name.length < 2) return res.status(400).json({ error: 'Enter the place name.' });
  if (!(Math.abs(lat) <= 90 && Math.abs(lng) <= 180)) return res.status(400).json({ error: 'Choose the place on the map.' });
  const biz = !!req.body?.mine && !!req.user.business;
  if (biz && db.mapPlaces.some((x) => x.by === req.user.id && x.biz && !x.hidden)) return res.status(400).json({ error: 'Your shop is already on the map. Delete the old pin first to move it.' });
  const x = { id: newId(), by: req.user.id, name, cat: PLACE_CATS.includes(req.body?.cat) ? req.body.cat : 'other', lat: +lat.toFixed(6), lng: +lng.toFixed(6), phone: clip(req.body?.phone, 20).replace(/[^\d+ ]/g, ''), note: clip(req.body?.note, 200), biz, createdAt: now() };
  db.mapPlaces.push(x); save(); res.json(placeOut(x, req.user.id));
});
app.delete('/api/maps/places/:id', auth, (req, res) => {
  const x = db.mapPlaces.find((y) => y.id === req.params.id && y.by === req.user.id); if (!x) return res.status(404).json({ error: 'Not found.' });
  db.mapPlaces = db.mapPlaces.filter((y) => y !== x); save(); res.json({ ok: true });
});
app.post('/api/maps/places/:id/report', auth, (req, res) => {
  const x = db.mapPlaces.find((y) => y.id === req.params.id); if (!x) return res.status(404).json({ error: 'Not found.' });
  x.reports = [...new Set([...(x.reports || []), req.user.id])]; if (x.reports.length >= 3) x.hidden = true; save(); res.json({ ok: true });
});

// ---------- 🗺️ Maata Maps: routes, nearby places and AI search (free OpenStreetMap services, called from the server) ----------
const MAP_UA = { 'User-Agent': 'MaataApp/1.0 (maataapp.com maps)', 'Accept-Language': 'en' };
const mapCache = new Map(); // small cache so repeated requests are instant
const mapCacheGet = (k) => { const c = mapCache.get(k); if (c && now() - c.t < c.ttl) return c.v; mapCache.delete(k); return null; };
const mapCacheSet = (k, v, ttl) => { mapCache.set(k, { v, t: now(), ttl }); if (mapCache.size > 800) mapCache.delete(mapCache.keys().next().value); };
const mapHits = new Map();
function mapLimit(req, res, max = 40) {
  const h = (mapHits.get(req.user.id) || []).filter((t) => now() - t < 60000); h.push(now()); mapHits.set(req.user.id, h);
  if (h.length > max) { res.status(429).json({ error: 'Too many map requests. Wait a minute.' }); return false; }
  return true;
}
// two-wheelers drive like cars on Indian roads (the "bike" OSRM profile is a bicycle, too slow), so OSRM uses the car roads for them
const ROUTERS = { car: ['https://routing.openstreetmap.de/routed-car/route/v1/driving/', 'https://router.project-osrm.org/route/v1/driving/'], bike: ['https://routing.openstreetmap.de/routed-car/route/v1/driving/', 'https://router.project-osrm.org/route/v1/driving/'], foot: ['https://routing.openstreetmap.de/routed-foot/route/v1/driving/'] };
const VALHALLA = ['https://valhalla1.openstreetmap.de/route'];
function decodePoly6(str) { // Valhalla shape → [[lng, lat], …]
  const out = []; let i = 0, lat = 0, lng = 0;
  while (i < str.length) { for (const k of [0, 1]) { let b, shift = 0, r = 0; do { b = str.charCodeAt(i++) - 63; r |= (b & 31) << shift; shift += 5; } while (b >= 32); const d = r & 1 ? ~(r >> 1) : r >> 1; if (k === 0) lat += d; else lng += d; } out.push([lng / 1e6, lat / 1e6]); }
  return out;
}
const VAL_TURN = { 1: ['depart', ''], 2: ['depart', ''], 3: ['depart', ''], 4: ['arrive', ''], 5: ['arrive', ''], 6: ['arrive', ''], 7: ['continue', 'straight'], 8: ['continue', 'straight'], 22: ['continue', 'straight'], 17: ['continue', 'straight'], 25: ['continue', 'straight'],
  9: ['turn', 'slight right'], 18: ['turn', 'slight right'], 20: ['turn', 'slight right'], 23: ['turn', 'slight right'], 37: ['turn', 'slight right'], 10: ['turn', 'right'], 11: ['turn', 'sharp right'], 12: ['turn', 'uturn'], 13: ['turn', 'uturn'],
  14: ['turn', 'sharp left'], 15: ['turn', 'left'], 16: ['turn', 'slight left'], 19: ['turn', 'slight left'], 21: ['turn', 'slight left'], 24: ['turn', 'slight left'], 38: ['turn', 'slight left'], 26: ['roundabout', ''] };
function valhallaTrip(trip) {
  const coords = [], steps = [];
  for (const leg of trip.legs || []) {
    const shape = decodePoly6(leg.shape || ''), base = coords.length; coords.push(...shape);
    for (const m of leg.maneuvers || []) {
      if (m.type === 27) continue; const [type, mod] = VAL_TURN[m.type] || ['continue', 'straight'];
      const names = m.street_names || []; const ref = names.find((n) => /^[A-Z]{1,4}[ -]?\d/.test(n)) || '';
      steps.push({ type, mod, exit: m.roundabout_exit_count || 0, loc: shape[m.begin_shape_index] || shape[0], name: names.find((n) => n !== ref) || '', ref, distance: Math.round((m.length || 0) * 1000), duration: Math.round(m.time || 0) });
    }
    void base;
  }
  return { distance: Math.round((trip.summary?.length || 0) * 1000), duration: Math.round(trip.summary?.time || 0), geometry: { type: 'LineString', coordinates: coords },
    via: [...new Set(steps.filter((s) => s.distance > 1500).sort((a, b) => b.distance - a.distance).map((s) => s.ref || s.name).filter(Boolean))].slice(0, 2), steps, engine: 'valhalla' };
}
async function valhallaRoute(mode, pts) {
  const costing = mode === 'foot' ? 'pedestrian' : mode === 'bike' ? 'motorcycle' : 'auto';
  const costing_options = { auto: { use_tracks: 0.6, use_living_streets: 0.8 }, motorcycle: { use_trails: 0.6, use_highways: 0.8 } };
  const body = { locations: pts.map(([lo, la], i) => ({ lat: la, lon: lo, type: i === 0 || i === pts.length - 1 ? 'break' : 'through', radius: 300 })), costing, costing_options, directions_options: { units: 'kilometers' }, alternates: pts.length === 2 ? 2 : 0 };
  for (const base of VALHALLA) {
    try {
      const r = await fetch(base, { method: 'POST', headers: { ...MAP_UA, 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(12000) });
      const d = await r.json(); if (!d.trip) continue;
      return [d.trip, ...(d.alternates || []).map((a) => a.trip)].filter(Boolean).map(valhallaTrip).filter((x) => x.geometry.coordinates.length > 1);
    } catch { /* next */ }
  }
  return [];
}
async function osrmRoute(mode, pts) {
  const path = pts.map((p) => p[0].toFixed(6) + ',' + p[1].toFixed(6)).join(';') + '?overview=full&geometries=geojson&steps=true&alternatives=' + (pts.length === 2 ? 'true' : 'false');
  for (const base of ROUTERS[mode]) {
    try {
      const d = await (await fetch(base + path, { headers: MAP_UA, signal: AbortSignal.timeout(12000) })).json();
      if (d.code === 'NoRoute') return []; if (d.code !== 'Ok' || !d.routes?.length) continue;
      const k = mode === 'bike' ? 0.9 : 1; // bikes squeeze through traffic a little faster than cars
      return d.routes.slice(0, 3).map((r) => ({ distance: Math.round(r.distance), duration: Math.round(r.duration * k), geometry: r.geometry, engine: 'osrm',
        via: [...new Set(r.legs.flatMap((l) => l.steps.filter((s) => s.distance > 1500).sort((a, b) => b.distance - a.distance).map((s) => s.ref || s.name).filter(Boolean)))].slice(0, 2),
        steps: r.legs.flatMap((l) => l.steps).map((s) => ({ type: s.maneuver.type, mod: s.maneuver.modifier || '', exit: s.maneuver.exit || 0, loc: s.maneuver.location, name: s.name || '', ref: s.ref || '', distance: Math.round(s.distance), duration: Math.round(s.duration * k) })) }));
    } catch { /* next server */ }
  }
  return [];
}
// Two route engines at once; the one that really reaches small village roads (ends closest to the place) wins
app.get('/api/maps/route', auth, async (req, res) => {
  if (!mapLimit(req, res)) return;
  const mode = ROUTERS[req.query.mode] ? req.query.mode : 'car';
  const pts = String(req.query.pts || '').split(';').map((p) => p.split(',').map(Number)).filter((p) => p.length === 2 && Math.abs(p[0]) <= 180 && Math.abs(p[1]) <= 90).slice(0, 6);
  if (pts.length < 2) return res.status(400).json({ error: 'Choose where to go.' });
  const key = 'r2:' + mode + ':' + pts.map((p) => p.map((x) => x.toFixed(5)).join(',')).join(';');
  const hit = mapCacheGet(key); if (hit) return res.json(hit);
  const [v, o] = await Promise.all([valhallaRoute(mode, pts).catch(() => []), osrmRoute(mode, pts).catch(() => [])]);
  const end = pts[pts.length - 1], start = pts[0];
  const gap = (r) => { const c = r.geometry.coordinates; return geoDist(c[c.length - 1], end) + geoDist(c[0], start); };
  const all = [...v, ...o].map((r) => ({ ...r, gap: Math.round(gap(r)) }));
  if (!all.length) return res.status(502).json({ error: 'Route service is busy. Try again in a moment.' });
  // best = reaches the place (small gap), then fastest
  all.sort((a, b) => (Math.abs(a.gap - b.gap) > 120 ? a.gap - b.gap : a.duration - b.duration));
  const routes = [], best = all[0].gap;
  for (const r of all) { if (r.gap > best + 300) continue; if (routes.some((x) => Math.abs(x.distance - r.distance) < Math.max(150, r.distance * 0.02) && Math.abs(x.duration - r.duration) < 60)) continue; routes.push(r); if (routes.length === 3) break; }
  const out = { mode, routes };
  mapCacheSet(key, out, 10 * 60000); res.json(out);
});
const NEAR_CATS = { fuel: 'amenity=fuel', ev: 'amenity=charging_station', hospital: 'amenity~"hospital|clinic"', atm: 'amenity~"atm|bank"', food: 'amenity~"restaurant|fast_food|cafe"', parking: 'amenity=parking', hotel: 'tourism~"hotel|guest_house"', temple: 'amenity=place_of_worship', pharmacy: 'amenity=pharmacy', police: 'amenity=police', toilets: 'amenity=toilets', mechanic: 'shop~"car_repair|motorcycle_repair|tyres"' };
app.get('/api/maps/nearby', auth, async (req, res) => {
  if (!mapLimit(req, res, 20)) return;
  const lat = Number(req.query.lat), lng = Number(req.query.lng), cat = NEAR_CATS[req.query.cat] ? req.query.cat : 'fuel';
  if (!(Math.abs(lat) <= 90 && Math.abs(lng) <= 180)) return res.status(400).json({ error: 'Location needed.' });
  const key = 'near:' + cat + ':' + lat.toFixed(2) + ',' + lng.toFixed(2); const hit = mapCacheGet(key); if (hit) return res.json(hit);
  let answered = false;
  const R = 6371000, rad = Math.PI / 180;
  const distOf = (la, lo) => Math.round(2 * R * Math.asin(Math.sqrt(Math.sin((la - lat) * rad / 2) ** 2 + Math.cos(lat * rad) * Math.cos(la * rad) * Math.sin((lo - lng) * rad / 2) ** 2)));
  const finish = (list) => { const out = list.filter((x) => isFinite(x.lat) && isFinite(x.lng)).map((x) => ({ ...x, dist: distOf(x.lat, x.lng) })).sort((a, b) => a.dist - b.dist).slice(0, 25); mapCacheSet(key, out, 30 * 60000); return res.json(out); };
  // 1) Overpass: several free servers asked at the same time, the first good answer wins
  const q = '[out:json][timeout:10];(nwr[' + NEAR_CATS[cat] + '](around:6000,' + lat + ',' + lng + '););out center 40;';
  const ask = (base) => fetch(base, { method: 'POST', headers: { ...MAP_UA, 'Content-Type': 'application/x-www-form-urlencoded', Accept: 'application/json' }, body: 'data=' + encodeURIComponent(q), signal: AbortSignal.timeout(11000) })
    .then((r) => { if (!r.ok) throw new Error('http ' + r.status); return r.json(); }).then((d) => { if (!Array.isArray(d.elements)) throw new Error('bad'); return d.elements; });
  try {
    const els = await Promise.any(['https://overpass-api.de/api/interpreter', 'https://overpass.private.coffee/api/interpreter', 'https://maps.mail.ru/osm/tools/overpass/api/interpreter', 'https://overpass.osm.jp/api/interpreter'].map(ask));
    return finish(els.map((e) => { const t = e.tags || {}; return { name: t.name || t.brand || t.operator || '', lat: e.lat ?? e.center?.lat, lng: e.lon ?? e.center?.lon, open: t.opening_hours || '', phone: t.phone || t['contact:phone'] || '', brand: t.brand || '' }; }));
  } catch (e) { console.log('[maps] overpass busy:', (e.errors || [e]).map((x) => x.message).join(', ').slice(0, 160)); }
  // 2) backup: OpenStreetMap place search (Nominatim) inside a box around the person
  const NOM = { fuel: ['fuel', 'petrol pump'], ev: ['charging_station', 'charging station'], hospital: ['hospital', 'hospital'], atm: ['atm', 'atm'], food: ['restaurant', 'restaurant'], parking: ['parking', 'parking'], hotel: ['hotel', 'hotel'], temple: ['place_of_worship', 'temple'], pharmacy: ['pharmacy', 'pharmacy'], police: ['police', 'police'], toilets: ['toilets', 'toilets'], mechanic: ['car_repair', 'mechanic'] }[cat];
  const box = '&viewbox=' + (lng - 0.06) + ',' + (lat + 0.06) + ',' + (lng + 0.06) + ',' + (lat - 0.06) + '&bounded=1&limit=40&format=jsonv2';
  for (const u of ['https://nominatim.openstreetmap.org/search?amenity=' + NOM[0] + box, 'https://nominatim.openstreetmap.org/search?q=' + encodeURIComponent(NOM[1]) + box]) {
    try {
      const d = await (await fetch(u, { headers: MAP_UA, signal: AbortSignal.timeout(9000) })).json();
      if (Array.isArray(d)) answered = true;
      if (Array.isArray(d) && d.length) return finish(d.map((x) => ({ name: x.name || String(x.display_name || '').split(',')[0], lat: Number(x.lat), lng: Number(x.lon), open: '', phone: '', brand: '' })));
    } catch { /* next */ }
  }
  if (mapCache.has(key)) return res.json(mapCache.get(key).v); // an older answer is better than nothing
  if (answered) return res.json([]);
  res.status(502).json({ error: 'Nearby search is busy. Try again in a moment.' });
});
// ✨ AI: understands "nearest petrol bunk", "Tirupati via Chittoor by bike", "take me home", Telugu / Hindi typed in English letters
app.post('/api/maps/ai', auth, async (req, res) => {
  const text = clip(req.body?.text, 200); if (text.length < 2) return res.status(400).json({ error: 'Say where you want to go.' });
  if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
  const sys = 'You turn a driver\'s request (English, Telugu, Hindi, Tamil or any Indian language, also typed in English letters) into JSON for a map app in India. ' +
    'Return ONLY JSON: {"intent":"route"|"nearby"|"place"|"plan","to":"destination place name with town and state if known, in English","via":["optional stop place names"],"category":"fuel|hospital|food|atm|police|hotel|temple|pharmacy|mechanic or empty","mode":"car|bike|foot","saved":"home|work or empty","reply":"one short friendly sentence in the same language the person used, saying what you will do"}. ' +
    '"plan" = they want a trip / tour / day plan with several places. "nearby" = they want the nearest kind of place (petrol bunk = fuel, hospital, medical shop = pharmacy, lodge = hotel, restaurant/tiffin/meals = food, atm/bank = atm, police station = police, temple/church/mosque = temple, mechanic/puncture = mechanic; for parking, toilets or EV charging use intent place with to = what they asked). "route" = go to a named place. bike for two-wheeler/scooter, foot for walking, else car. Never invent addresses.';
  const r = await gemini(sys, [{ role: 'user', parts: [{ text: text + (req.body?.near ? '\n(Person is near: ' + clip(req.body.near, 100) + ')' : '') }] }], { json: true, maxTokens: 300, temperature: 0.2 });
  if (r.error) return res.status(r.error === 'limit' ? 429 : 502).json({ error: AI_ERR[r.error] || 'AI is busy, try again.' });
  try { const j = JSON.parse(String(r.text).replace(/^```json|```$/g, '').trim());
    res.json({ intent: ['route', 'nearby', 'place', 'plan'].includes(j.intent) ? j.intent : 'place', to: clip(j.to, 120), via: (Array.isArray(j.via) ? j.via : []).slice(0, 3).map((v) => clip(v, 80)), category: NEAR_CATS[j.category] ? j.category : '', mode: ROUTERS[j.mode] ? j.mode : 'car', saved: ['home', 'work'].includes(j.saved) ? j.saved : '', reply: clip(j.reply, 160) });
  } catch { res.status(502).json({ error: 'AI could not understand. Try typing the place name.' }); }
});

// ---------- 🚦 Maata live traffic: road speeds shared without names by Maata drivers while they navigate ----------
const geoDist = (a, b) => { const R = 6371000, r = Math.PI / 180, dl = (b[1] - a[1]) * r, dg = (b[0] - a[0]) * r; const x = Math.sin(dl / 2) ** 2 + Math.cos(a[1] * r) * Math.cos(b[1] * r) * Math.sin(dg / 2) ** 2; return 2 * R * Math.asin(Math.sqrt(x)); };
const okPt = (p) => Array.isArray(p) && p.length === 2 && Math.abs(Number(p[0])) <= 180 && Math.abs(Number(p[1])) <= 90;
const trafficCells = new Map(); const TRAFFIC_AGE = 20 * 60000;
app.post('/api/maps/speed', auth, (req, res) => {
  if (!mapLimit(req, res, 30)) return;
  for (const s of (Array.isArray(req.body?.segs) ? req.body.segs : []).slice(0, 20)) {
    if (!okPt(s?.a) || !okPt(s?.b)) continue; const a = s.a.map(Number), b = s.b.map(Number), r = Number(s.r), d = geoDist(a, b);
    if (!(r >= 0) || d < 15 || d > 800) continue;
    const mid = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2], dir = Math.round(((Math.atan2(b[0] - a[0], b[1] - a[1]) * 180 / Math.PI + 360) % 360) / 45) % 8;
    const key = mid[1].toFixed(3) + ',' + mid[0].toFixed(3) + ':' + dir;
    const c = trafficCells.get(key) || { a, b, rs: [] }; c.a = a.map((x) => +x.toFixed(5)); c.b = b.map((x) => +x.toFixed(5));
    c.rs = [...c.rs.filter((x) => now() - x.t < TRAFFIC_AGE && x.u !== req.user.id), { r: Math.min(1.5, r), t: now(), u: req.user.id }].slice(-12);
    trafficCells.delete(key); trafficCells.set(key, c); if (trafficCells.size > 30000) trafficCells.delete(trafficCells.keys().next().value);
  }
  res.json({ ok: true });
});
// road reports (like Waze): accident, police check, road closed, pothole, water on road, traffic jam
const REPORT_TYPES = { jam: 90, accident: 120, police: 90, closed: 720, pothole: 4320, water: 360, hazard: 120 };
const reportOut = (x, me) => ({ id: x.id, type: x.type, lat: x.lat, lng: x.lng, note: x.note || '', at: x.createdAt, until: x.until, still: (x.still || []).length, mine: x.by === me, byName: (userById(x.by) || {}).name || 'Maata user' });
const liveReports = () => { const t = now(); if (db.mapReports.some((x) => x.until < t)) { db.mapReports = db.mapReports.filter((x) => x.until >= t); save(); } return db.mapReports; };
function bboxOf(q) { const b = String(q.bbox || '').split(',').map(Number); return b.length === 4 && b.every(isFinite) && b[2] - b[0] < 6 && b[3] - b[1] < 6 ? b : null; } // west,south,east,north
const inBox = (b, lng, lat) => lng >= b[0] && lng <= b[2] && lat >= b[1] && lat <= b[3];
app.get('/api/maps/traffic', auth, (req, res) => {
  const b = bboxOf(req.query); if (!b) return res.json({ roads: [], reports: [] });
  const roads = [];
  for (const c of trafficCells.values()) {
    if (!inBox(b, c.a[0], c.a[1]) && !inBox(b, c.b[0], c.b[1])) continue;
    const rs = c.rs.filter((x) => now() - x.t < TRAFFIC_AGE); if (!rs.length) continue;
    const r = rs.reduce((s, x) => s + x.r, 0) / rs.length; roads.push({ a: c.a, b: c.b, l: r < 0.35 ? 3 : r < 0.65 ? 2 : 1, n: rs.length });
    if (roads.length >= 2500) break;
  }
  res.json({ roads, reports: liveReports().filter((x) => inBox(b, x.lng, x.lat)).slice(-300).map((x) => reportOut(x, req.user.id)) });
});
app.post('/api/maps/reports', auth, async (req, res) => {
  const lat = Number(req.body?.lat), lng = Number(req.body?.lng); if (!okPt([lng, lat])) return res.status(400).json({ error: 'Location needed.' });
  if (db.mapReports.filter((x) => x.by === req.user.id && now() - x.createdAt < 86400000).length >= 20) return res.status(429).json({ error: 'You can send up to 20 road reports a day.' });
  let type = REPORT_TYPES[req.body?.type] ? req.body.type : '', note = clip(req.body?.note, 120);
  const said = clip(req.body?.text, 200);
  if (!type && said) { // ✨ spoken report → type + short note
    if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
    const r = await gemini('A driver in India spoke a road report (any Indian language, maybe typed in English letters). Return ONLY JSON {"type":"jam|accident|police|closed|pothole|water|hazard","note":"very short English note, max 8 words"}.', [{ role: 'user', parts: [{ text: said }] }], { json: true, maxTokens: 80, temperature: 0.1 });
    try { const j = JSON.parse(String(r.text).replace(/^```json|```$/g, '').trim()); type = REPORT_TYPES[j.type] ? j.type : 'hazard'; note = note || clip(j.note, 120); } catch { type = 'hazard'; note = note || clip(said, 120); }
  }
  if (!type) return res.status(400).json({ error: 'Choose what is on the road.' });
  const same = liveReports().find((x) => x.type === type && geoDist([x.lng, x.lat], [lng, lat]) < 250);
  if (same) { same.still = [...new Set([...(same.still || []), req.user.id])]; same.until = Math.max(same.until, now() + REPORT_TYPES[type] * 60000); save(); return res.json(reportOut(same, req.user.id)); }
  const x = { id: newId(), by: req.user.id, type, lat: +lat.toFixed(6), lng: +lng.toFixed(6), note, createdAt: now(), until: now() + REPORT_TYPES[type] * 60000, still: [], gone: [] };
  db.mapReports.push(x); save(); res.json(reportOut(x, req.user.id));
});
app.post('/api/maps/reports/:id/vote', auth, (req, res) => {
  const x = liveReports().find((y) => y.id === req.params.id); if (!x) return res.json({ ok: true, gone: true });
  if (req.body?.still) { x.still = [...new Set([...(x.still || []), req.user.id])]; x.until = Math.max(x.until, now() + Math.min(REPORT_TYPES[x.type], 120) * 60000); }
  else { x.gone = [...new Set([...(x.gone || []), req.user.id])]; if (x.by === req.user.id || x.gone.length >= 2) x.until = 0; }
  liveReports(); save(); res.json({ ok: true, gone: !x.until });
});
// 🔥 wildfires (NASA EONET, free) and 🌫️ air quality / 🌦️ weather backup (Open-Meteo, free) for the map layers
app.get('/api/maps/fires', auth, async (req, res) => {
  const b = String(req.query.bbox || '').split(',').map(Number); if (b.length !== 4 || !b.every(isFinite)) return res.json([]);
  const key = 'fires:' + b.map((x) => Math.round(x)).join(','); const hit = mapCacheGet(key); if (hit) return res.json(hit);
  try {
    const d = await (await fetch('https://eonet.gsfc.nasa.gov/api/v3/events?category=wildfires&status=open&days=45&limit=300&bbox=' + [b[0], b[3], b[2], b[1]].map((x) => x.toFixed(2)).join(','), { headers: MAP_UA, signal: AbortSignal.timeout(12000) })).json();
    const out = (d.events || []).map((e) => { const g = (e.geometry || []).slice(-1)[0] || {}; let c = g.coordinates; while (Array.isArray(c) && Array.isArray(c[0])) c = c[0]; return Array.isArray(c) ? { title: clip(e.title, 100), lng: Number(c[0]), lat: Number(c[1]), date: g.date || '' } : null; }).filter((x) => x && isFinite(x.lat));
    mapCacheSet(key, out, 60 * 60000); res.json(out);
  } catch { res.status(502).json({ error: 'Wildfire data is not available right now.' }); }
});
app.get('/api/maps/air', auth, async (req, res) => {
  const la = String(req.query.lat || '').split(',').map(Number).slice(0, 16), lo = String(req.query.lng || '').split(',').map(Number).slice(0, 16), kind = req.query.kind === 'weather' ? 'weather' : 'air';
  if (!la.length || la.length !== lo.length || !la.every(isFinite) || !lo.every(isFinite)) return res.status(400).json({ error: 'Location needed.' });
  const q = 'latitude=' + la.map((x) => x.toFixed(2)).join(',') + '&longitude=' + lo.map((x) => x.toFixed(2)).join(',');
  const key = kind + ':' + q; const hit = mapCacheGet(key); if (hit) return res.json(hit);
  const u = kind === 'air' ? 'https://air-quality-api.open-meteo.com/v1/air-quality?' + q + '&current=us_aqi,pm2_5,pm10' : 'https://api.open-meteo.com/v1/forecast?' + q + '&current=temperature_2m,weather_code,precipitation,wind_speed_10m&timezone=auto';
  try { const d = await (await fetch(u, { headers: MAP_UA, signal: AbortSignal.timeout(10000) })).json(); const out = Array.isArray(d) ? d : [d]; mapCacheSet(key, out, 20 * 60000); res.json(out); }
  catch { res.status(502).json({ error: 'Air quality data is not available right now.' }); }
});
// ✨ AI: "About this place" and "Plan my trip"
const AI_LANG_NAME = { en: 'English', te: 'Telugu', hi: 'Hindi', ta: 'Tamil', kn: 'Kannada', ml: 'Malayalam', mr: 'Marathi', bn: 'Bengali', gu: 'Gujarati', pa: 'Punjabi', ur: 'Urdu', or: 'Odia' };
const aiJson = (r) => JSON.parse(String(r.text).replace(/^```json|```$/g, '').trim());
app.post('/api/maps/about', auth, async (req, res) => {
  const name = clip(req.body?.name, 100), addr = clip(req.body?.addr, 160), lang = AI_LANG_NAME[req.body?.lang] ? req.body.lang : 'en';
  if (name.length < 2) return res.status(400).json({ error: 'Choose a place.' });
  const key = 'about:' + lang + ':' + name.toLowerCase() + ':' + Number(req.body?.lat || 0).toFixed(2) + ',' + Number(req.body?.lng || 0).toFixed(2); const hit = mapCacheGet(key); if (hit) return res.json(hit);
  if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
  const r = await gemini('You are a helpful local guide for India inside a map app. Write in ' + AI_LANG_NAME[lang] + '. Return ONLY JSON {"summary":"2-3 short sentences about what this place is and why people go there","tips":["up to 3 short practical tips: best time, what to carry, parking, crowd, dress code"],"known":true or false}. If you do not really know this exact place, set known=false and give only general tips for this kind of place — never invent facts, phone numbers, prices or timings.',
    [{ role: 'user', parts: [{ text: 'Place: ' + name + (addr ? '\nArea: ' + addr : '') }] }], { json: true, maxTokens: 400, temperature: 0.3 });
  if (r.error) return res.status(r.error === 'limit' ? 429 : 502).json({ error: AI_ERR[r.error] || 'AI is busy, try again.' });
  try { const j = aiJson(r); const out = { summary: clip(j.summary, 500), tips: (Array.isArray(j.tips) ? j.tips : []).slice(0, 3).map((t) => clip(t, 140)), known: j.known !== false }; mapCacheSet(key, out, 7 * 86400000); res.json(out); }
  catch { res.status(502).json({ error: 'AI could not answer. Try again.' }); }
});
app.post('/api/maps/voicename', auth, async (req, res) => {
  const lang = AI_LANG_NAME[req.body?.lang] ? req.body.lang : 'en', name = clip(req.body?.name, 40).trim();
  if (!name || lang === 'en') return res.json({ name });
  const key = 'vname:' + lang + ':' + name.toLowerCase(); const hit = mapCacheGet(key); if (hit) return res.json({ name: hit });
  const r = await gemini('Write the given Indian person name in ' + AI_LANG_NAME[lang] + ' script exactly as it is pronounced (transliterate, do not translate). Return ONLY JSON {"name":"..."}.', [{ role: 'user', parts: [{ text: name }] }], { json: true, maxTokens: 60, temperature: 0 });
  try { const n = clip(aiJson(r).name, 40).trim(); if (!n) throw 0; mapCacheSet(key, n, 30 * 86400000); res.json({ name: n }); } catch { res.json({ name }); }
});
app.post('/api/maps/plan', auth, async (req, res) => {
  const text = clip(req.body?.text, 300), lang = AI_LANG_NAME[req.body?.lang] ? req.body.lang : 'en'; if (text.length < 4) return res.status(400).json({ error: 'Say what trip you want.' });
  if (!aiQuota(req.user, 2)) return res.status(429).json({ error: AI_ERR.quota });
  const r = await gemini('You plan short road trips in India for a map app. Return ONLY JSON {"title":"short trip title in ' + AI_LANG_NAME[lang] + '","stops":[{"name":"real, well-known place name with town and state in English so a map search finds it","why":"one short line in ' + AI_LANG_NAME[lang] + '","time":"suggested time like 7:00 AM or empty"}],"tip":"one short travel tip in ' + AI_LANG_NAME[lang] + '"}. 2 to 6 stops in a sensible driving order. Only real places you are sure exist.',
    [{ role: 'user', parts: [{ text: text + (req.body?.near ? '\n(Starting near: ' + clip(req.body.near, 100) + ')' : '') }] }], { json: true, maxTokens: 700, temperature: 0.4 });
  if (r.error) return res.status(r.error === 'limit' ? 429 : 502).json({ error: AI_ERR[r.error] || 'AI is busy, try again.' });
  try { const j = aiJson(r); res.json({ title: clip(j.title, 100), tip: clip(j.tip, 200), stops: (Array.isArray(j.stops) ? j.stops : []).slice(0, 6).map((s) => ({ name: clip(s.name, 120), why: clip(s.why, 160), time: clip(s.time, 20) })).filter((s) => s.name) }); }
  catch { res.status(502).json({ error: 'AI could not plan this. Try again.' }); }
});

// ---------- 🛣️ roads added by Maata people (village roads missing on the public map) ----------
const ROAD_KINDS = ['road', 'mud', 'foot'];
const roadOut = (x, me) => ({ id: x.id, name: x.name || '', kind: x.kind, coords: x.coords, byName: (userById(x.by) || {}).name || 'Maata user', mine: x.by === me, confirms: (x.ok || []).length, createdAt: x.createdAt });
app.get('/api/maps/roads', auth, (req, res) => {
  const b = bboxOf(req.query); if (!b) return res.json([]);
  res.json(db.mapRoads.filter((x) => !x.hidden && x.bbox[0] <= b[2] && x.bbox[2] >= b[0] && x.bbox[1] <= b[3] && x.bbox[3] >= b[1]).slice(-400).map((x) => roadOut(x, req.user.id)));
});
app.post('/api/maps/roads', auth, (req, res) => {
  if (db.mapRoads.filter((x) => x.by === req.user.id && now() - x.createdAt < 86400000).length >= 30) return res.status(429).json({ error: 'You can add up to 30 roads a day.' });
  const pts = (Array.isArray(req.body?.coords) ? req.body.coords : []).slice(0, 3000).filter(okPt).map((p) => [+Number(p[0]).toFixed(6), +Number(p[1]).toFixed(6)]);
  if (pts.length < 2) return res.status(400).json({ error: 'Draw the road with at least two points.' });
  let len = 0; for (let i = 1; i < pts.length; i++) len += geoDist(pts[i - 1], pts[i]);
  if (len < 20) return res.status(400).json({ error: 'The road is too short.' }); if (len > 60000) return res.status(400).json({ error: 'Add long roads in parts (up to 60 km).' });
  const bbox = [Math.min(...pts.map((p) => p[0])), Math.min(...pts.map((p) => p[1])), Math.max(...pts.map((p) => p[0])), Math.max(...pts.map((p) => p[1]))];
  const x = { id: newId(), by: req.user.id, name: clip(req.body?.name, 80), kind: ROAD_KINDS.includes(req.body?.kind) ? req.body.kind : 'road', coords: pts, bbox, length: Math.round(len), ok: [], reports: [], createdAt: now() };
  db.mapRoads.push(x); save(); res.json(roadOut(x, req.user.id));
});
app.post('/api/maps/roads/:id/vote', auth, (req, res) => {
  const x = db.mapRoads.find((y) => y.id === req.params.id && !y.hidden); if (!x) return res.status(404).json({ error: 'Not found.' });
  if (req.body?.ok) x.ok = [...new Set([...(x.ok || []), req.user.id])];
  else { x.reports = [...new Set([...(x.reports || []), req.user.id])]; if (x.reports.length >= 3 && x.reports.length > (x.ok || []).length) x.hidden = true; }
  save(); res.json(roadOut(x, req.user.id));
});
app.delete('/api/maps/roads/:id', auth, (req, res) => {
  const x = db.mapRoads.find((y) => y.id === req.params.id && y.by === req.user.id); if (!x) return res.status(404).json({ error: 'Not found.' });
  db.mapRoads = db.mapRoads.filter((y) => y !== x); save(); res.json({ ok: true });
});

// place / address search — Google-like: spelling fix, big cities first, many results
// [name, state, lat, lng, population rank (1 = biggest), other spellings]
const IN_CITIES = [
  ['Bengaluru', 'Karnataka', 12.9716, 77.5946, 1, 'bangalore|banglore|bengalore|bangaluru|bengaluru|blr|bangalure|banglor'],
  ['Mumbai', 'Maharashtra', 19.076, 72.8777, 1, 'bombay|mumbai|mumbay'], ['Delhi', 'Delhi', 28.6139, 77.209, 1, 'new delhi|dilli|delhi|newdelhi'],
  ['Hyderabad', 'Telangana', 17.385, 78.4867, 1, 'hyderbad|hydrabad|hyd|hyderabad|haidarabad|hydarabad'], ['Chennai', 'Tamil Nadu', 13.0827, 80.2707, 1, 'madras|chenai|chennai|chennnai'],
  ['Kolkata', 'West Bengal', 22.5726, 88.3639, 1, 'calcutta|kolkatta|kolkata'], ['Pune', 'Maharashtra', 18.5204, 73.8567, 2, 'poona|pune'], ['Ahmedabad', 'Gujarat', 23.0225, 72.5714, 2, 'amdavad|ahmadabad'],
  ['Visakhapatnam', 'Andhra Pradesh', 17.6868, 83.2185, 2, 'vizag|vishakapatnam|visakapatnam|vishakhapatnam|vizagapatnam|waltair'], ['Vijayawada', 'Andhra Pradesh', 16.5062, 80.648, 2, 'bezawada|vijaywada|vijayavada|bejawada'],
  ['Tirupati', 'Andhra Pradesh', 13.6288, 79.4192, 3, 'tirupathi|tirupaty|thirupati|thirupathi'], ['Tirumala', 'Andhra Pradesh', 13.6833, 79.3474, 4, 'thirumala|tirumalai'],
  ['Chittoor', 'Andhra Pradesh', 13.2172, 79.1003, 3, 'chitoor|chittor|chitur|chittur'], ['Nellore', 'Andhra Pradesh', 14.4426, 79.9865, 3, 'nelore|nellure'], ['Guntur', 'Andhra Pradesh', 16.3067, 80.4365, 3, 'guntoor'],
  ['Kurnool', 'Andhra Pradesh', 15.8281, 78.0373, 3, 'karnool|kurnul'], ['Kadapa', 'Andhra Pradesh', 14.4673, 78.8242, 3, 'cuddapah|kadappa|kadapah'], ['Anantapur', 'Andhra Pradesh', 14.6819, 77.6006, 3, 'anantapuram|ananthapur|ananthapuram'],
  ['Rajahmundry', 'Andhra Pradesh', 17.0005, 81.804, 3, 'rajamahendravaram|rajamundry|rajahmundri'], ['Kakinada', 'Andhra Pradesh', 16.9891, 82.2475, 3, 'kakinada'], ['Eluru', 'Andhra Pradesh', 16.7107, 81.0952, 3, 'ellore'],
  ['Ongole', 'Andhra Pradesh', 15.5057, 80.0499, 3, 'ongol'], ['Vizianagaram', 'Andhra Pradesh', 18.1067, 83.3956, 3, 'vijayanagaram|vizianagram'], ['Srikakulam', 'Andhra Pradesh', 18.2949, 83.8938, 3, 'srikakulam'],
  ['Machilipatnam', 'Andhra Pradesh', 16.1875, 81.1389, 4, 'masulipatnam|bandar'], ['Amaravati', 'Andhra Pradesh', 16.5131, 80.5165, 4, 'amravati ap'], ['Madanapalle', 'Andhra Pradesh', 13.5503, 78.5029, 4, 'madanapalli'],
  ['Srikalahasti', 'Andhra Pradesh', 13.7499, 79.6984, 4, 'kalahasti|sri kalahasti'], ['Puttur', 'Andhra Pradesh', 13.4416, 79.5514, 5, 'puttur'], ['Nagari', 'Andhra Pradesh', 13.3215, 79.5858, 5, 'nagari'],
  ['Palamaner', 'Andhra Pradesh', 13.2, 78.75, 5, 'palamaneru'], ['Kuppam', 'Andhra Pradesh', 12.7495, 78.3425, 5, 'kuppam'], ['Punganur', 'Andhra Pradesh', 13.3667, 78.5833, 5, 'punganuru'],
  ['Hindupur', 'Andhra Pradesh', 13.8283, 77.4911, 4, 'hindupuram'], ['Proddatur', 'Andhra Pradesh', 14.7502, 78.5481, 4, 'proddutur'], ['Nandyal', 'Andhra Pradesh', 15.4786, 78.4831, 4, 'nandyala'],
  ['Tenali', 'Andhra Pradesh', 16.2379, 80.6444, 4, 'tenali'], ['Bhimavaram', 'Andhra Pradesh', 16.5449, 81.5212, 4, 'bheemavaram'], ['Gudur', 'Andhra Pradesh', 14.1453, 79.8514, 5, 'gudur'],
  ['Warangal', 'Telangana', 17.9689, 79.5941, 2, 'warangal|orugallu'], ['Karimnagar', 'Telangana', 18.4386, 79.1288, 3, 'karimnagar'], ['Nizamabad', 'Telangana', 18.6725, 78.0941, 3, 'nizamabad'],
  ['Khammam', 'Telangana', 17.2473, 80.1514, 3, 'khamam'], ['Secunderabad', 'Telangana', 17.4399, 78.4983, 2, 'secundrabad|sikindrabad'],
  ['Coimbatore', 'Tamil Nadu', 11.0168, 76.9558, 2, 'kovai|coimbatur|coimbator'], ['Madurai', 'Tamil Nadu', 9.9252, 78.1198, 2, 'madhurai'], ['Vellore', 'Tamil Nadu', 12.9165, 79.1325, 3, 'velur|vellur'],
  ['Tiruchirappalli', 'Tamil Nadu', 10.7905, 78.7047, 2, 'trichy|tiruchi|thiruchirapalli|trichi'], ['Salem', 'Tamil Nadu', 11.6643, 78.146, 3, 'salem'], ['Kanchipuram', 'Tamil Nadu', 12.8342, 79.7036, 3, 'kanchi|kancheepuram|conjeevaram'],
  ['Hosur', 'Tamil Nadu', 12.7409, 77.8253, 3, 'hosur'], ['Krishnagiri', 'Tamil Nadu', 12.5266, 78.2141, 4, 'krishnagiri'], ['Tiruvannamalai', 'Tamil Nadu', 12.2253, 79.0747, 3, 'thiruvannamalai|arunachala'],
  ['Pondicherry', 'Puducherry', 11.9416, 79.8083, 3, 'puducherry|pondy|pondichery'], ['Kanyakumari', 'Tamil Nadu', 8.0883, 77.5385, 4, 'kanyakumari|cape comorin'], ['Rameswaram', 'Tamil Nadu', 9.2876, 79.3129, 4, 'rameshwaram'],
  ['Ooty', 'Tamil Nadu', 11.4102, 76.695, 4, 'udhagamandalam|ooty|uti'], ['Mysuru', 'Karnataka', 12.2958, 76.6394, 2, 'mysore|maisur|mysuru'], ['Mangaluru', 'Karnataka', 12.9141, 74.856, 2, 'mangalore|mangaluru|kudla'],
  ['Hubballi', 'Karnataka', 15.3647, 75.124, 2, 'hubli|hubballi'], ['Belagavi', 'Karnataka', 15.8497, 74.4977, 3, 'belgaum'], ['Kolar', 'Karnataka', 13.1367, 78.1292, 4, 'kolar'],
  ['Kochi', 'Kerala', 9.9312, 76.2673, 2, 'cochin|ernakulam|kochi'], ['Thiruvananthapuram', 'Kerala', 8.5241, 76.9366, 2, 'trivandrum|tvm'], ['Kozhikode', 'Kerala', 11.2588, 75.7804, 3, 'calicut'],
  ['Thrissur', 'Kerala', 10.5276, 76.2144, 3, 'trichur'], ['Panaji', 'Goa', 15.4909, 73.8278, 3, 'panjim|goa'], ['Nagpur', 'Maharashtra', 21.1458, 79.0882, 2, 'nagpur'], ['Nashik', 'Maharashtra', 19.9975, 73.7898, 2, 'nasik'],
  ['Aurangabad', 'Maharashtra', 19.8762, 75.3433, 2, 'sambhajinagar'], ['Solapur', 'Maharashtra', 17.6599, 75.9064, 3, 'sholapur'], ['Surat', 'Gujarat', 21.1702, 72.8311, 2, 'surat'], ['Vadodara', 'Gujarat', 22.3072, 73.1812, 2, 'baroda'],
  ['Rajkot', 'Gujarat', 22.3039, 70.8022, 2, 'rajkot'], ['Jaipur', 'Rajasthan', 26.9124, 75.7873, 1, 'jaipur|pink city'], ['Jodhpur', 'Rajasthan', 26.2389, 73.0243, 2, 'jodhpur'], ['Udaipur', 'Rajasthan', 24.5854, 73.7125, 3, 'udaipur'],
  ['Lucknow', 'Uttar Pradesh', 26.8467, 80.9462, 1, 'lakhnau'], ['Kanpur', 'Uttar Pradesh', 26.4499, 80.3319, 2, 'cawnpore'], ['Varanasi', 'Uttar Pradesh', 25.3176, 82.9739, 2, 'banaras|benares|kashi'],
  ['Agra', 'Uttar Pradesh', 27.1767, 78.0081, 2, 'agra'], ['Prayagraj', 'Uttar Pradesh', 25.4358, 81.8463, 2, 'allahabad'], ['Noida', 'Uttar Pradesh', 28.5355, 77.391, 2, 'noida'], ['Gurugram', 'Haryana', 28.4595, 77.0266, 2, 'gurgaon'],
  ['Chandigarh', 'Chandigarh', 30.7333, 76.7794, 2, 'chandigarh'], ['Amritsar', 'Punjab', 31.634, 74.8723, 2, 'amritsar'], ['Ludhiana', 'Punjab', 30.901, 75.8573, 2, 'ludhiana'], ['Dehradun', 'Uttarakhand', 30.3165, 78.0322, 3, 'dehradun'],
  ['Haridwar', 'Uttarakhand', 29.9457, 78.1642, 3, 'hardwar'], ['Shimla', 'Himachal Pradesh', 31.1048, 77.1734, 3, 'simla'], ['Srinagar', 'Jammu and Kashmir', 34.0837, 74.7973, 2, 'srinagar'], ['Jammu', 'Jammu and Kashmir', 32.7266, 74.857, 3, 'jammu'],
  ['Bhopal', 'Madhya Pradesh', 23.2599, 77.4126, 2, 'bhopal'], ['Indore', 'Madhya Pradesh', 22.7196, 75.8577, 2, 'indore'], ['Raipur', 'Chhattisgarh', 21.2514, 81.6296, 2, 'raipur'], ['Patna', 'Bihar', 25.5941, 85.1376, 2, 'patna'],
  ['Ranchi', 'Jharkhand', 23.3441, 85.3096, 2, 'ranchi'], ['Bhubaneswar', 'Odisha', 20.2961, 85.8245, 2, 'bhubaneshwar|bbsr'], ['Puri', 'Odisha', 19.8135, 85.8312, 3, 'jagannath puri'], ['Guwahati', 'Assam', 26.1445, 91.7362, 2, 'gauhati'],
  ['Shillong', 'Meghalaya', 25.5788, 91.8933, 3, 'shillong'], ['Gangtok', 'Sikkim', 27.3389, 88.6065, 3, 'gangtok'], ['Darjeeling', 'West Bengal', 27.041, 88.2663, 3, 'darjiling'], ['Siliguri', 'West Bengal', 26.7271, 88.3953, 3, 'siliguri'],
  ['Agartala', 'Tripura', 23.8315, 91.2868, 3, 'agartala'], ['Imphal', 'Manipur', 24.817, 93.9368, 3, 'imphal'], ['Shirdi', 'Maharashtra', 19.7645, 74.4762, 4, 'shiridi|sai baba shirdi'],
  ['Srisailam', 'Andhra Pradesh', 16.0726, 78.8686, 4, 'srisailam|sri sailam'], ['Bhadrachalam', 'Telangana', 17.6688, 80.8936, 4, 'badrachalam'], ['Yadagirigutta', 'Telangana', 17.5875, 78.9442, 4, 'yadadri'],
  ['Puttaparthi', 'Andhra Pradesh', 14.1653, 77.8117, 4, 'puttaparthy|prasanthi nilayam'], ['Mantralayam', 'Andhra Pradesh', 15.9417, 77.4271, 4, 'mantralaya'], ['Kalahasti', 'Andhra Pradesh', 13.7499, 79.6984, 5, ''],
];
const normQ = (s) => String(s || '').toLowerCase().normalize('NFKD').replace(/[̀-ͯ]/g, '').replace(/[^a-z0-9ऀ-෿ ]+/g, ' ').replace(/\s+/g, ' ').trim();
function editDist(a, b, max) { // Damerau-Levenshtein (swapped letters count as one mistake)
  if (Math.abs(a.length - b.length) > max) return max + 1;
  const d = []; for (let i = 0; i <= a.length; i++) { d[i] = [i]; }
  for (let j = 1; j <= b.length; j++) d[0][j] = j;
  for (let i = 1; i <= a.length; i++) { let best = 1e9; for (let j = 1; j <= b.length; j++) {
    const c = a[i - 1] === b[j - 1] ? 0 : 1; d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + c);
    if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) d[i][j] = Math.min(d[i][j], d[i - 2][j - 2] + 1);
    best = Math.min(best, d[i][j]); } if (best > max) return max + 1; }
  return d[a.length][b.length];
}
const CITY_SPELL = { Bengaluru: 'bangalore', Mysuru: 'mysore', Mangaluru: 'mangalore', Hubballi: 'hubli' }; // the spelling most shop / place names use
const citySpell = (c) => CITY_SPELL[c[0]] || normQ(c[0]);
const CITY_KEYS = IN_CITIES.flatMap((c) => [c[0], ...String(c[5] || '').split('|')].filter(Boolean).map((k) => [normQ(k), c]));
// "banglore palace" → { city: Bengaluru, fixed: "bangalore palace" }
function cityFix(q) {
  const n = normQ(q); if (!n) return null; const words = n.split(' ');
  let best = null;
  for (let take = Math.min(3, words.length); take >= 1; take--) {
    const head = words.slice(0, take).join(' '), rest = words.slice(take).join(' ');
    if (head.length < 3) continue;
    const allow = head.length >= 8 ? 2 : head.length >= 5 ? 1 : 0;
    for (const [k, c] of CITY_KEYS) {
      const dist = k === head ? 0 : editDist(head, k, allow);
      if (dist <= allow && (!best || dist < best.dist || (dist === best.dist && c[4] < best.city[4]))) best = { city: c, dist, word: k, rest, take };
    }
    if (best) break;
  }
  if (!best) { // the city name later in the text: "palace bangalore"
    for (let i = 1; i < words.length; i++) { const w = words[i]; if (w.length < 5) continue; const allow = w.length >= 8 ? 2 : 1;
      for (const [k, c] of CITY_KEYS) { if (k.includes(' ')) continue; const dist = editDist(w, k, allow); if (dist <= allow && (!best || dist < best.dist)) best = { city: c, dist, word: k, rest: '', at: i }; } }
    if (!best) return null;
    const ws = [...words]; const was = ws[best.at]; ws[best.at] = citySpell(best.city); return { city: best.city, exact: false, fixed: ws.join(' '), changed: was !== ws[best.at] && was !== normQ(best.city[0]) };
  }
  const head = words.slice(0, best.take).join(' '), sp = citySpell(best.city);
  return { city: best.city, exact: !best.rest, fixed: sp + (best.rest ? ' ' + best.rest : ''), changed: head !== sp && head !== normQ(best.city[0]) };
}
const PLACE_ICON = (k, v) => k === 'place' ? (/city|town|state|county|district|municipality/.test(v) ? '🏙️' : '🏘️') : k === 'aeroway' ? '✈️' : k === 'railway' ? '🚆'
  : v === 'bus_station' || v === 'bus_stop' ? '🚌' : v === 'hospital' || v === 'clinic' ? '🏥' : v === 'place_of_worship' ? '🛕' : /school|college|university/.test(v) ? '🎓'
  : v === 'fuel' ? '⛽' : k === 'highway' ? '🛣️' : k === 'tourism' ? (v === 'hotel' ? '🏨' : '📸') : v === 'restaurant' || v === 'cafe' ? '🍽️' : k === 'shop' ? '🛒' : k === 'boundary' ? '🗺️' : '📍';
const placeWeight = (k, v) => k === 'place' ? ({ state: 9, city: 10, county: 6, district: 6, town: 8, municipality: 7, suburb: 5, village: 5, hamlet: 3, locality: 3, neighbourhood: 3 }[v] || 3) : k === 'boundary' ? 6 : k === 'aeroway' ? 5 : k === 'railway' && v === 'station' ? 5 : 2;
async function photonFind(q, la, lo) {
  const near = isFinite(la) && la && lo ? '&lat=' + la + '&lon=' + lo + '&location_bias_scale=0.3&zoom=9' : '';
  const r = await fetch('https://photon.komoot.io/api/?limit=15&lang=en&bbox=68,6,98,37.5&q=' + encodeURIComponent(q) + near, { headers: OSM_UA, signal: AbortSignal.timeout(6000) });
  const d = await r.json();
  return (d.features || []).filter((f) => !f.properties.countrycode || f.properties.countrycode === 'IN').map((f) => {
    const p = f.properties, [lng, lat] = f.geometry.coordinates;
    const sub = [p.street && p.street !== p.name ? p.street : '', p.district && p.district !== p.name ? p.district : '', p.city && p.city !== p.name ? p.city : '', p.county && p.county !== p.city && p.county !== p.name ? p.county : '', p.state && p.state !== p.name ? p.state : ''].filter(Boolean);
    const name = p.name || [p.housenumber, p.street].filter(Boolean).join(' '); if (!name) return null;
    return { name, sub: [...new Set(sub)].slice(0, 3).join(', '), lat, lng, k: p.osm_key, v: p.osm_value };
  }).filter(Boolean);
}
async function nominatimFind(q, la, lo) {
  const near = isFinite(la) && la && lo ? '&viewbox=' + (lo - 1.5) + ',' + (la + 1.5) + ',' + (lo + 1.5) + ',' + (la - 1.5) + '&bounded=0' : '';
  const d = await (await fetch('https://nominatim.openstreetmap.org/search?format=jsonv2&limit=8&addressdetails=0&countrycodes=in&q=' + encodeURIComponent(q) + near, { headers: OSM_UA, signal: AbortSignal.timeout(7000) })).json();
  return (Array.isArray(d) ? d : []).map((x) => { const parts = String(x.display_name || '').split(', ').filter((s) => s !== 'India'); return { name: parts[0], sub: parts.slice(1, 4).join(', '), lat: Number(x.lat), lng: Number(x.lon), k: x.category, v: x.type, imp: Number(x.importance) || 0 }; }).filter((x) => x.name && isFinite(x.lat));
}
const geoSearchCache = new Map(), geoSearchHits = new Map();
// ---------- 🟢 Ola Maps (India data) — used only inside the free monthly allowance ----------
// Free: 100,000 calls a month for each API. We stop at OLA_CAP so the bill always stays ₹0.
const OLA_KEY = process.env.OLA_MAPS_KEY || '', OLA_CAP = Math.min(95000, Number(process.env.OLA_MONTHLY_CAP) || 90000);
const olaMonth = () => new Date(Date.now() + 5.5 * 3600000).toISOString().slice(0, 7); // India month
function olaUse() {
  let m = db.meta.find((x) => x.id === 'olaUse');
  if (!m) { m = { id: 'olaUse', month: olaMonth(), n: {}, createdAt: now() }; db.meta.push(m); }
  if (m.month !== olaMonth()) { m.month = olaMonth(); m.n = {}; }
  return m;
}
let olaSaveT = null, olaDownUntil = 0;
function olaTake(api) { // true = allowed to call
  if (!OLA_KEY || now() < olaDownUntil) return false;
  const m = olaUse(); if ((m.n[api] || 0) >= OLA_CAP) return false;
  m.n[api] = (m.n[api] || 0) + 1;
  if (!olaSaveT) olaSaveT = setTimeout(() => { olaSaveT = null; save(); }, 30000);
  return true;
}
async function olaGet(api, path, params) {
  if (!olaTake(api)) return null;
  const u = 'https://api.olamaps.io' + path + '?' + new URLSearchParams({ ...params, api_key: OLA_KEY });
  try {
    const r = await fetch(u, { headers: { 'X-Request-Id': newId() }, signal: AbortSignal.timeout(5000) });
    if (r.status === 401 || r.status === 403) { olaDownUntil = now() + 600000; console.error('[ola] key refused', r.status); return null; }
    if (r.status === 429 || r.status === 402) { olaDownUntil = now() + 600000; console.error('[ola] limit / credits', r.status); return null; }
    if (!r.ok) return null;
    return await r.json();
  } catch { return null; }
}
const olaTypeKind = (types = []) => { const t = types.join(' ');
  return /locality|sublocality|administrative|political|city|town|village|state|district|country|postal/.test(t) ? ['place', /village|sublocality/.test(t) ? 'village' : /state/.test(t) ? 'state' : 'town']
    : /airport/.test(t) ? ['aeroway', 'aerodrome'] : /train|railway|transit/.test(t) ? ['railway', 'station'] : /bus/.test(t) ? ['amenity', 'bus_station'] : /hospital|clinic|doctor|health/.test(t) ? ['amenity', 'hospital']
    : /temple|church|mosque|worship/.test(t) ? ['amenity', 'place_of_worship'] : /school|college|university/.test(t) ? ['amenity', 'school'] : /gas_station|petrol|fuel/.test(t) ? ['amenity', 'fuel'] : /lodging|hotel/.test(t) ? ['tourism', 'hotel']
    : /restaurant|cafe|food/.test(t) ? ['amenity', 'restaurant'] : /route|road|street/.test(t) ? ['highway', 'road'] : /store|shop|market/.test(t) ? ['shop', 'shop'] : ['ola', 'poi']; };
function olaPreds(d, me) {
  return ((d && d.predictions) || []).map((p) => {
    const loc = (p.geometry && p.geometry.location) || {}, lat = Number(loc.lat), lng = Number(loc.lng);
    const sf = p.structured_formatting || {}, name = sf.main_text || String(p.description || '').split(',')[0];
    let sub = sf.secondary_text || String(p.description || '').split(', ').slice(1).join(', ');
    sub = sub.split(', ').map((s) => s.replace(/\s*\d{6}$/, '').trim()).filter((s) => s && s !== 'India').slice(-3).join(', ');
    const [k, v] = olaTypeKind(p.types || []);
    return { name, sub, lat, lng, k, v, ola: 1, dist: Number(p.distance_meters) || (me && isFinite(lat) ? geoDist(me, [lng, lat]) : null) };
  }).filter((x) => x.name && isFinite(x.lat) && isFinite(x.lng) && Math.abs(x.lat) > 0.1);
}
async function olaFind(q, la, lo) {
  const near = isFinite(la) && la && lo ? { location: la + ',' + lo } : {};
  return olaPreds(await olaGet('autocomplete', '/places/v1/autocomplete', { input: q, ...near }), near.location ? [lo, la] : null);
}
// nearby: "petrol bunk" etc. typed around you, only places within ~25 km
const OLA_NEAR_Q = { fuel: ['petrol bunk', 'petrol pump'], hospital: ['hospital', 'clinic'], pharmacy: ['medical shop', 'pharmacy'], hotel: ['lodge', 'hotel rooms'], food: ['restaurant', 'tiffin centre'], atm: ['atm', 'bank'],
  police: ['police station'], temple: ['temple', 'church', 'mosque'], mechanic: ['bike mechanic', 'car mechanic'], parking: ['parking'], ev: ['ev charging station'], toilets: ['public toilet'] };
app.get('/api/maps/olanear', auth, async (req, res) => {
  const cat = String(req.query.cat || ''), la = Number(req.query.lat), lo = Number(req.query.lng);
  if (!OLA_KEY || !OLA_NEAR_Q[cat] || !(Math.abs(la) <= 90 && Math.abs(lo) <= 180)) return res.json([]);
  const ck = 'near|' + cat + '|' + la.toFixed(2) + ',' + lo.toFixed(2);
  if (geoSearchCache.has(ck)) return res.json(geoSearchCache.get(ck));
  const got = (await Promise.all(OLA_NEAR_Q[cat].map((q) => olaGet('autocomplete', '/places/v1/autocomplete', { input: q, location: la + ',' + lo, radius: 25000, strictbounds: 'true' }))))
    .flatMap((d) => olaPreds(d, [lo, la]));
  const out = got.filter((x) => x.dist != null && x.dist < 25000).map((x) => ({ name: x.name, lat: +x.lat.toFixed(6), lng: +x.lng.toFixed(6), dist: Math.round(x.dist), addr: x.sub, src: 'ola' }));
  geoSearchCache.set(ck, out); res.json(out);
});
console.log(OLA_KEY ? '[maps] Ola Maps: on (search + nearby, stops at ' + OLA_CAP + ' calls a month per API, so no bill)' : '[maps] Ola Maps: off (add OLA_MAPS_KEY for better Indian place search)');
app.get('/api/maps/ola-status', auth, (req, res) => { const m = olaUse(); res.json({ on: !!OLA_KEY, month: m.month, used: m.n, cap: OLA_CAP, paused: now() < olaDownUntil }); });
app.get('/api/geo/search', auth, async (req, res) => {
  const q = clip(req.query.q, 120); if (q.trim().length < 2) return res.json([]);
  const hits = (geoSearchHits.get(req.user.id) || []).filter((t) => now() - t < 60000); if (hits.length > 40) return res.status(429).json({ error: 'Too many searches. Wait a minute.' });
  hits.push(now()); geoSearchHits.set(req.user.id, hits);
  const la = Number(req.query.lat), lo = Number(req.query.lng);
  const ck = normQ(q) + '|' + Math.round(la || 0) + ',' + Math.round(lo || 0);
  if (geoSearchCache.has(ck)) return res.json(geoSearchCache.get(ck));
  const fix = cityFix(q), fq = fix && fix.changed ? fix.fixed : '';
  const jobs = [photonFind(q, la, lo), nominatimFind(fq || q, la, lo), olaFind(fq || q, la, lo)]; if (fq) jobs.push(photonFind(fq, la, lo));
  if (fix) jobs.push(photonFind(fix.city[0], la, lo));
  const got = (await Promise.allSettled(jobs)).flatMap((r) => (r.status === 'fulfilled' ? r.value : []));
  const nq = normQ(fq || q), nq0 = normQ(q), me = isFinite(la) && la && lo ? [lo, la] : null;
  const seen = []; const items = [];
  for (const x of got) {
    const nn = normQ(x.name); if (seen.some((s) => s.n === nn && geoDist([s.lng, s.lat], [x.lng, x.lat]) < (x.k === 'place' || x.k === 'boundary' ? 30000 : 400))) continue;
    seen.push({ n: nn, lat: x.lat, lng: x.lng });
    let score = placeWeight(x.k, x.v) * 3 + (x.imp || 0) * 10 + (x.ola ? 6 : 0);
    if (nn === nq || nn === nq0) score += 30; else if (nn.startsWith(nq) || nn.startsWith(nq0)) score += 14; else if (nn.includes(nq) || nn.includes(nq0)) score += 8;
    if (fix && normQ(fix.city[0]) === nn) score += 25;
    if (me) { const km = geoDist(me, [x.lng, x.lat]) / 1000; score -= Math.min(10, km / 60); }
    items.push({ ...x, score });
  }
  if (fix) { // make sure the city itself is there, at the top
    const c = fix.city, nc = normQ(c[0]);
    const have = items.find((x) => (normQ(x.name) === nc || CITY_KEYS.some(([k, cc]) => cc === c && k === normQ(x.name))) && geoDist([c[3], c[2]], [x.lng, x.lat]) < 40000);
    if (have) { have.score += fix.exact ? 60 : 20; have.k = 'place'; have.v = 'city'; }
    else items.push({ name: c[0], sub: c[1], lat: c[2], lng: c[3], k: 'place', v: 'city', score: fix.exact ? 200 : 40 });
  }
  items.sort((a, b) => b.score - a.score);
  const out = items.slice(0, 12).map((x, i) => ({ title: [x.name, x.sub].filter(Boolean).join(', '), name: x.name, sub: x.sub || '', icon: PLACE_ICON(x.k, x.v), lat: +x.lat.toFixed(6), lng: +x.lng.toFixed(6), ...(x.ola ? { src: 'ola' } : {}), ...(i === 0 && fix && fix.changed ? { fixed: fix.city[0] } : {}) }));
  if (!out.length && got.length === 0 && !fix) return res.status(502).json({ error: 'Place search is not available right now. Check internet and try again.' });
  if (out.length) { geoSearchCache.set(ck, out); if (geoSearchCache.size > 1000) geoSearchCache.delete(geoSearchCache.keys().next().value); }
  res.json(out);
});
app.get('/api/geo/reverse', auth, async (req, res) => {
  const lat = Number(req.query.lat), lng = Number(req.query.lng);
  if (!(Math.abs(lat) <= 90 && Math.abs(lng) <= 180)) return res.status(400).json({ error: 'Bad location.' });
  if (!geoLimit(req, res)) return;
  const key = lat.toFixed(4) + ',' + lng.toFixed(4);
  if (geoCache.has(key)) return res.json(geoCache.get(key));
  try {
    const wait = 1100 - (now() - lastNominatim); if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastNominatim = now();
    const d = await (await fetch(`https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=${lat}&lon=${lng}&zoom=18&addressdetails=1`, { headers: OSM_UA, signal: AbortSignal.timeout(8000) })).json();
    const a = d.address || {};
    const place = a.suburb || a.neighbourhood || a.village || a.town || a.city_district || a.city || a.county || '';
    const city = a.city || a.town || a.village || a.county || a.state_district || '';
    const out = {
      title: [place && place !== city ? place : null, city, a.state, a.country].filter(Boolean).join(', '),
      address: String(d.display_name || '').split(', ').filter((x) => x !== a.country).join(', '),
      countryCode: String(a.country_code || '').toUpperCase(),
    };
    geoCache.set(key, out); if (geoCache.size > 2000) geoCache.delete(geoCache.keys().next().value);
    res.json(out);
  } catch (e) { console.error('[geo]', e.message); res.status(502).json({ error: 'Address not available right now.' }); }
});
app.get('/api/geo/tile', auth, async (req, res) => {
  const z = Math.min(18, Math.max(3, parseInt(req.query.z, 10) || 16)), x = parseInt(req.query.x, 10), y = parseInt(req.query.y, 10), n = 2 ** z;
  if (!(x >= 0 && x < n && y >= 0 && y < n)) return res.status(400).end();
  const key = z + '/' + x + '/' + y;
  let buf = tileCache.get(key);
  if (!buf) {
    if (!geoLimit(req, res)) return;
    try {
      const r = await fetch('https://tile.openstreetmap.org/' + key + '.png', { headers: OSM_UA, signal: AbortSignal.timeout(8000) });
      if (!r.ok) return res.status(502).end();
      buf = Buffer.from(await r.arrayBuffer());
      tileCache.set(key, buf); if (tileCache.size > 300) tileCache.delete(tileCache.keys().next().value);
    } catch { return res.status(502).end(); }
  }
  res.setHeader('Content-Type', 'image/png'); res.setHeader('Cache-Control', 'private, max-age=86400'); res.end(buf);
});

// Broadcast: one message to many people, sent to each one privately.
// Only people who saved YOUR number receive it (stops spam), up to 256 per broadcast.
const bcHits = new Map();
app.post('/api/broadcast', auth, (req, res) => {
  const text = String(req.body?.text || '').trim().slice(0, 4000);
  const tos = [...new Set((Array.isArray(req.body?.to) ? req.body.to : []).map(String))].slice(0, 256);
  if (!text || !tos.length) return res.status(400).json({ error: 'Write a message and choose people.' });
  const list = (bcHits.get(req.user.id) || []).filter((t) => now() - t < 3600_000);
  if (list.length >= 10) return res.status(429).json({ error: 'You can send up to 10 broadcasts an hour.' });
  list.push(now()); bcHits.set(req.user.id, list);
  let sent = 0, skipped = 0;
  for (const to of tos) {
    const ok = canMessage(req.user, to);
    if (ok.error || !hasContact(ok.target, req.user.id)) { skipped++; continue; }
    deliverMessage(req.user, ok.target, { type: 'text', text, broadcast: true }, null); sent++;
  }
  audit('customer:' + req.user.phone, 'broadcast', sent + ' people', text.slice(0, 80));
  res.json({ sent, skipped });
});
// Post a chat photo / video / text to my status
app.post('/api/status/from-message', auth, async (req, res) => {
  const m = db.messages.find((x) => x.id === String(req.body?.id || '') && (x.from === req.user.id || x.to === req.user.id));
  if (!m || !['text', 'image', 'video'].includes(m.type || 'text')) return res.status(400).json({ error: 'Only text, photos and videos can go to status.' });
  if (tooManyStatuses(req.user)) return res.status(429).json({ error: 'You can post up to 30 status updates a day.' });
  const base = { id: newId(), userId: req.user.id, createdAt: now(), expiresAt: now() + STATUS_TTL, views: [] };
  let st;
  if (!m.type || m.type === 'text') st = { ...base, type: 'text', text: String(m.text || '').slice(0, 700), bg: '#0F4C5C' };
  else {
    try {
      let buf;
      if (isR2(m.file.id)) {
        if (m.file.size > 16 * 1024 * 1024) return res.status(400).json({ error: 'This file is too big for status (16 MB max).' });
        const r2r = await fetch(r2Url('GET', m.file.id, 300), { signal: AbortSignal.timeout(60000) }); if (!r2r.ok) throw new Error('r2'); buf = Buffer.from(await r2r.arrayBuffer());
      } else { const chunks = []; await new Promise((ok, bad) => { const r = chatFileStream(m.file.id, 0, m.file.size - 1); r.on('data', (c) => chunks.push(c)); r.on('end', ok); r.on('error', bad); }); buf = Buffer.concat(chunks); }
      await saveMedia(base.id, buf, m.file.mime);
    } catch { return res.status(500).json({ error: 'Could not copy this file to status.' }); }
    st = { ...base, type: m.type, caption: String(m.text || '').slice(0, 300), mime: m.file.mime, size: m.file.size };
  }
  db.statuses.push(st); save(); notifyStatus(req.user);
  res.json({ ok: true });
});
function emitUpdate(m) { if (m.group) return io.to('grp:' + m.group).emit('msg:update', m); for (const id of [m.from, m.to]) io.to('user:' + id).emit('msg:update', m); }

app.post('/api/chat/upload', auth, express.raw({ type: () => true, limit: '16mb' }), async (req, res) => {
  const ok = canMessage(req.user, String(req.query.to || ''));
  if (ok.error) return res.status(400).json({ error: ok.error });
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Choose a file first.' });
  if (req.body.length > MAX_FILE) return res.status(413).json({ error: 'File is too big. The limit is 16 MB.' });
  const mime = String(req.query.mime || req.headers['content-type'] || 'application/octet-stream').split(';')[0].trim().slice(0, 100);
  let type = String(req.query.kind || '');
  if (!['image', 'video', 'audio', 'file'].includes(type)) type = 'file';
  const name = String(req.query.name || 'file').replace(/[\\/\r\n"]/g, '_').slice(0, 120);
  const id = newId();
  try { await saveChatFile(id, req.body, mime); } catch (e) { console.error('[chat file]', e.message); return res.status(500).json({ error: 'Could not save the file. Try again.' }); }
  const viewOnce = req.query.once === '1' && (type === 'image' || type === 'video');
  const file = { id, name, mime, size: req.body.length, ...(req.query.voice === '1' ? { voice: true } : {}), ...(req.query.sticker === '1' && type === 'image' ? { sticker: true } : {}), ...(Number(req.query.dur) > 0 ? { duration: Math.round(Number(req.query.dur)) } : {}) };
  const m = deliverMessage(req.user, ok.target, { type, text: viewOnce ? '' : String(req.query.caption || '').trim().slice(0, 1000), file, ...(viewOnce ? { viewOnce: true, openedBy: {} } : {}) }, String(req.query.sid || '') || null);
  res.json({ message: m });
});

// Files open only for the two people in that chat. <img>/<video> send the login token as ?t=
// ---------- Big files (up to 2 GB) on Cloudflare R2 (S3-compatible) ----------
// Set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET. Phones upload straight to R2 with a
// short-lived signed link, so big files never pass through (or fill up) this server or MongoDB.
const R2 = { account: (process.env.R2_ACCOUNT_ID || '').trim(), key: (process.env.R2_ACCESS_KEY_ID || '').trim(), secret: (process.env.R2_SECRET_ACCESS_KEY || '').trim(), bucket: (process.env.R2_BUCKET || '').trim() };
const R2_ON = !!(R2.account && R2.key && R2.secret && R2.bucket);
const MAX_BIG_FILE = 2 * 1024 * 1024 * 1024;
console.log(R2_ON ? '[files] Cloudflare R2 storage on: files up to 2 GB (bucket ' + R2.bucket + ')' : '[files] Files up to 16 MB (add R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET for 2 GB)');
const isR2 = (id) => typeof id === 'string' && id.startsWith('r2-');
const rfc3986 = (s) => encodeURIComponent(s).replace(/[!'()*]/g, (c) => '%' + c.charCodeAt(0).toString(16).toUpperCase());
function r2Url(method, key, expires = 3600, extra = {}) {
  const host = R2.account + '.r2.cloudflarestorage.com', region = 'auto', service = 's3';
  const amz = new Date().toISOString().replace(/[:-]|\.\d{3}/g, ''), date = amz.slice(0, 8);
  const scope = `${date}/${region}/${service}/aws4_request`;
  const p = '/' + rfc3986(R2.bucket) + '/' + rfc3986(key);
  const q = { 'X-Amz-Algorithm': 'AWS4-HMAC-SHA256', 'X-Amz-Credential': R2.key + '/' + scope, 'X-Amz-Date': amz, 'X-Amz-Expires': String(expires), 'X-Amz-SignedHeaders': 'host', ...extra };
  const qs = Object.keys(q).sort().map((k) => rfc3986(k) + '=' + rfc3986(q[k])).join('&');
  const canonical = [method, p, qs, 'host:' + host + '\n', 'host', 'UNSIGNED-PAYLOAD'].join('\n');
  const toSign = ['AWS4-HMAC-SHA256', amz, scope, crypto.createHash('sha256').update(canonical).digest('hex')].join('\n');
  const h = (k, d) => crypto.createHmac('sha256', k).update(d).digest();
  const sig = crypto.createHmac('sha256', h(h(h(h('AWS4' + R2.secret, date), region), service), 'aws4_request')).update(toSign).digest('hex');
  return `https://${host}${p}?${qs}&X-Amz-Signature=${sig}`;
}
async function r2Delete(id) { try { await fetch(r2Url('DELETE', id, 300), { method: 'DELETE', signal: AbortSignal.timeout(15000) }); } catch (e) { console.error('[r2] delete', e.message); } }
const pendingUploads = new Map(); // uploadId -> details, until the phone says "done"
app.post('/api/chat/upload-url', auth, (req, res) => {
  if (!R2_ON) return res.status(400).json({ error: 'Big files are not switched on.' });
  const b = req.body || {}, ok = canMessage(req.user, String(b.to || ''));
  if (ok.error) return res.status(400).json({ error: ok.error });
  const size = Number(b.size);
  if (!(size > 0)) return res.status(400).json({ error: 'Choose a file first.' });
  if (size > MAX_BIG_FILE) return res.status(413).json({ error: 'File is too big. The limit is 2 GB.' });
  const mine = [...pendingUploads.values()].filter((u) => u.by === req.user.id && u.exp > now());
  if (mine.length >= 10) return res.status(429).json({ error: 'Too many uploads at once. Wait for the others to finish.' });
  const fileId = 'r2-' + newId(), uploadId = newId();
  const kind = ['image', 'video', 'audio', 'file'].includes(b.kind) ? b.kind : 'file';
  pendingUploads.set(uploadId, { by: req.user.id, to: String(b.to), fileId, kind, size, exp: now() + 3 * 3600_000,
    name: String(b.name || 'file').replace(/[\\/\r\n"]/g, '_').slice(0, 120), mime: String(b.mime || 'application/octet-stream').split(';')[0].slice(0, 100),
    caption: String(b.caption || '').trim().slice(0, 1000), voice: !!b.voice, dur: Math.round(Number(b.dur) || 0), once: !!b.once && (kind === 'image' || kind === 'video'), sticker: !!b.sticker && kind === 'image' });
  res.json({ uploadId, url: r2Url('PUT', fileId, 3 * 3600) });
});
app.post('/api/chat/upload-done', auth, async (req, res) => {
  const u = pendingUploads.get(String(req.body?.uploadId || ''));
  if (!u || u.by !== req.user.id) return res.status(404).json({ error: 'Upload not found. Try again.' });
  pendingUploads.delete(String(req.body.uploadId));
  let realSize = 0;
  try { const h = await fetch(r2Url('HEAD', u.fileId, 300), { method: 'HEAD', signal: AbortSignal.timeout(15000) }); realSize = Number(h.headers.get('content-length')) || 0; if (!h.ok) throw new Error('status ' + h.status); }
  catch (e) { console.error('[r2] check', e.message); return res.status(400).json({ error: 'The file did not finish uploading. Try again.' }); }
  if (realSize > MAX_BIG_FILE) { r2Delete(u.fileId); return res.status(413).json({ error: 'File is too big. The limit is 2 GB.' }); }
  const ok = canMessage(req.user, u.to);
  if (ok.error) { r2Delete(u.fileId); return res.status(400).json({ error: ok.error }); }
  const file = { id: u.fileId, name: u.name, mime: u.mime, size: realSize, ...(u.voice ? { voice: true } : {}), ...(u.dur ? { duration: u.dur } : {}), ...(u.sticker ? { sticker: true } : {}) };
  const m = deliverMessage(req.user, ok.target, { type: u.kind, text: u.caption, file, ...(u.once ? { viewOnce: true, openedBy: {} } : {}) }, String(req.body.sid || '') || null);
  res.json({ message: m });
});
setInterval(() => { for (const [k, u] of pendingUploads) if (u.exp < now()) { pendingUploads.delete(k); r2Delete(u.fileId); } }, 3600_000);

app.get('/api/chat/file/:id', (req, res) => {
  let viewer = null;
  try { const h = req.headers.authorization || ''; viewer = findUserByToken(h.startsWith('Bearer ') ? h.slice(7) : String(req.query.t || '')); } catch { /* invalid */ }
  if (!viewer) return res.status(401).end();
  const m = db.messages.find((x) => x.file && x.file.id === req.params.id && (x.from === viewer.id || x.to === viewer.id || (x.group && gMember(groupById(x.group), viewer.id))));
  if (!m) return res.status(404).end();
  if (m.viewOnce && (m.from === viewer.id || (m.openedBy || {})[viewer.id])) return res.status(410).end(); // view once: opened already (or sender)
  if (m.file.purged) { res.setHeader('X-Maata-Expired', '1'); return res.status(410).end(); } // older than the keep period: only phones have it now
  if (isR2(m.file.id)) { // big file on R2: send the phone a short-lived signed link (supports seeking in videos)
    const inline2 = /^(image|video|audio)\//.test(m.file.mime) || m.file.mime === 'application/pdf';
    res.setHeader('Cache-Control', 'private, max-age=300');
    return res.redirect(302, r2Url('GET', m.file.id, 3600, { 'response-content-type': m.file.mime, 'response-content-disposition': (req.query.dl === '1' || !inline2 ? 'attachment' : 'inline') + "; filename*=UTF-8''" + rfc3986(m.file.name) }));
  }
  const size = m.file.size;
  let start = 0, end = size - 1, code = 200;
  const r = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
  if (r) {
    if (r[1]) { start = parseInt(r[1], 10); end = r[2] ? Math.min(parseInt(r[2], 10), size - 1) : size - 1; }
    else if (r[2]) start = Math.max(0, size - parseInt(r[2], 10));
    if (start > end || start >= size) { res.setHeader('Content-Range', `bytes */${size}`); return res.status(416).end(); }
    code = 206; res.setHeader('Content-Range', `bytes ${start}-${end}/${size}`);
  }
  res.status(code);
  res.setHeader('Content-Type', m.file.mime); res.setHeader('Accept-Ranges', 'bytes');
  res.setHeader('Content-Length', end - start + 1); res.setHeader('Cache-Control', 'private, max-age=604800');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  const inline = /^(image|video|audio)\//.test(m.file.mime) || m.file.mime === 'application/pdf';
  res.setHeader('Content-Disposition', (req.query.dl === '1' || !inline ? 'attachment' : 'inline') + '; filename="' + encodeURIComponent(m.file.name) + '"');
  const st = chatFileStream(m.file.id, start, end);
  st.on('error', () => res.destroy());
  st.pipe(res);
});

// ---------- Groups: messages, members, admins, invite links, group photo ----------
const GROUP_MAX = 1024;          // members per group
// Group calls: with a LiveKit media server (LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET) up to 50 people,
// because each phone sends its video once to the server. Without it, phones connect to each other directly (max 8).
const LK_URL = (process.env.LIVEKIT_URL || '').trim(), LK_KEY = (process.env.LIVEKIT_API_KEY || '').trim(), LK_SECRET = (process.env.LIVEKIT_API_SECRET || '').trim();
const LIVEKIT = !!(LK_URL && LK_KEY && LK_SECRET);
const GCALL_MAX = LIVEKIT ? 50 : 8;
console.log(LIVEKIT ? '[calls] Group calls: LiveKit media server on, up to 50 people (' + LK_URL + ')' : '[calls] Group calls: direct mode, up to 8 people (add LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET for 50)');
// LiveKit access token = a signed JWT (same format their SDK makes), so no extra package is needed
function livekitToken(user, room) {
  return jwt.sign({ name: user.name, video: { room, roomJoin: true, canPublish: true, canSubscribe: true, canPublishData: false } },
    LK_SECRET, { issuer: LK_KEY, subject: user.id, expiresIn: '3h', notBefore: 0, jwtid: newId() });
}
const gidOf = (to) => (typeof to === 'string' && to.startsWith('g:') ? to.slice(2) : null);
const groupById = (id) => db.groups.find((g) => g.id === id);
const gMember = (g, uid) => !!g && g.members.some((x) => x.id === uid);
const gAdmin = (g, uid) => !!g && g.members.some((x) => x.id === uid && x.role === 'admin');
const gcalls = new Map(); // groupId -> live group call
function groupView(viewer, g, extra = {}) {
  const saved = new Map(contactsOf(viewer).map((c) => [c.id, c.name]));
  const me2 = g.members.find((x) => x.id === viewer.id);
  const c = gcalls.get(g.id);
  return {
    id: 'g:' + g.id, isGroup: true, name: g.name, displayName: g.name, phone: '', description: g.description || '',
    photo: !!g.photo, photoV: g.photoV || 0, memberCount: g.members.length, myRole: me2 ? me2.role : null, adhoc: !!g.adhoc,
    members: g.members.map((x) => { const u = userById(x.id); return { id: x.id, name: u ? saved.get(x.id) || u.name : 'Deleted user', phone: u ? u.phone : '', role: x.role, lang: u ? u.lang || null : null }; }),
    settings: g.settings, createdBy: g.createdBy, createdAt: g.createdAt, inContacts: true, online: false,
    community: g.communityId && db.communities.find((x) => x.id === g.communityId) ? { id: g.communityId, name: db.communities.find((x) => x.id === g.communityId).name, announce: !!g.isAnnounce } : null,
    ...(me2 && me2.role === 'admin' ? { invite: g.invite } : {}),
    activeCall: c ? { id: c.id, kind: c.kind, count: c.parts.size } : null,
    lastMessage: null, unread: 0, ...extra,
  };
}
function groupsFor(viewer) {
  const mine = db.groups.filter((g) => gMember(g, viewer.id));
  if (!mine.length) return [];
  const by = {}, hidden = new Set(viewer.hiddenMsgs || []), cleared = viewer.clearedChats || {};
  for (const g of mine) by['g:' + g.id] = { lastMessage: null, unread: 0 };
  for (const m of db.messages) {
    const b = m.group && by['g:' + m.group]; if (!b || hidden.has(m.id) || m.ts <= (cleared['g:' + m.group] || 0)) continue;
    b.lastMessage = m; if (m.from !== viewer.id && m.type !== 'system' && !(m.readBy || {})[viewer.id]) b.unread++;
  }
  return mine.map((g) => groupView(viewer, g, by['g:' + g.id]));
}
const joinGroupRoom = (uid, gid) => io.in('user:' + uid).socketsJoin('grp:' + gid);
const leaveGroupRoom = (uid, gid) => io.in('user:' + uid).socketsLeave('grp:' + gid);
function groupChanged(g, extraIds = []) {
  io.to('grp:' + g.id).emit('group:update', { id: 'g:' + g.id }); for (const id of extraIds) io.to('user:' + id).emit('group:update', { id: 'g:' + g.id });
  if (g.communityId && !g.isAnnounce) { const c = db.communities.find((x) => x.id === g.communityId); if (c) syncCommunity(c); } // new group members join the community's announcements
}
function sysMsg(g, actor, text) { return deliverMessage(actor, { id: 'g:' + g.id, name: g.name, isGroup: true }, { type: 'system', text }, null); }
function groupFor(req, res, needAdmin) {
  const g = groupById(req.params.id);
  if (!g || !gMember(g, req.user.id)) { res.status(404).json({ error: 'Group not found.' }); return null; }
  if (needAdmin && !gAdmin(g, req.user.id)) { res.status(403).json({ error: 'Only group admins can do this.' }); return null; }
  return g;
}
const inviteCode = () => crypto.randomBytes(9).toString('base64url');
function canAddTo(adder, uid) { const u = userById(uid); return u && u.status !== 'blocked' && uid !== adder.id && !blockedOf(u).includes(adder.id); }

app.post('/api/groups', auth, (req, res) => {
  const name = String(req.body?.name || '').trim().slice(0, 60);
  if (!name) return res.status(400).json({ error: 'Give the group a name.' });
  const ids = [...new Set((Array.isArray(req.body?.members) ? req.body.members : []).map(String))].filter((id) => canAddTo(req.user, id)).slice(0, GROUP_MAX - 1);
  if (!ids.length) return res.status(400).json({ error: 'Add at least one member.' });
  const g = { id: newId(), name, description: String(req.body?.description || '').trim().slice(0, 500), createdBy: req.user.id, createdAt: now(),
    members: [{ id: req.user.id, role: 'admin', joinedAt: now() }, ...ids.map((id) => ({ id, role: 'member', joinedAt: now(), addedBy: req.user.id }))],
    settings: { onlyAdminsMessage: false, onlyAdminsEdit: false }, invite: inviteCode() };
  db.groups.push(g); save();
  for (const x of g.members) joinGroupRoom(x.id, g.id);
  sysMsg(g, req.user, req.user.name + ' created the group "' + name + '"');
  groupChanged(g);
  res.json(groupView(req.user, g));
});
app.get('/api/groups/:id', auth, (req, res) => { const g = groupFor(req, res); if (g) res.json(groupView(req.user, g)); });
app.put('/api/groups/:id', auth, (req, res) => {
  const g = groupFor(req, res); if (!g) return;
  const admin = gAdmin(g, req.user.id), b = req.body || {};
  if ((b.name !== undefined || b.description !== undefined) && g.settings.onlyAdminsEdit && !admin) return res.status(403).json({ error: 'Only admins can edit group info.' });
  if (b.name !== undefined) { const n = String(b.name).trim().slice(0, 60); if (n && n !== g.name) { g.name = n; sysMsg(g, req.user, req.user.name + ' changed the group name to "' + n + '"'); } }
  if (b.description !== undefined) { g.description = String(b.description).trim().slice(0, 500); sysMsg(g, req.user, req.user.name + ' changed the group description'); }
  if (b.settings && admin) {
    for (const k of ['onlyAdminsMessage', 'onlyAdminsEdit']) if (typeof b.settings[k] === 'boolean' && g.settings[k] !== b.settings[k]) {
      g.settings[k] = b.settings[k];
      sysMsg(g, req.user, req.user.name + (k === 'onlyAdminsMessage' ? (b.settings[k] ? ' allowed only admins to send messages' : ' allowed all members to send messages') : (b.settings[k] ? ' allowed only admins to edit group info' : ' allowed all members to edit group info')));
    }
  }
  save(); groupChanged(g); res.json(groupView(req.user, g));
});
app.post('/api/groups/:id/members', auth, (req, res) => {
  const g = groupFor(req, res, true); if (!g) return;
  const ids = [...new Set((Array.isArray(req.body?.ids) ? req.body.ids : []).map(String))].filter((id) => canAddTo(req.user, id) && !gMember(g, id));
  if (g.members.length + ids.length > GROUP_MAX) return res.status(400).json({ error: 'A group can have up to ' + GROUP_MAX + ' members.' });
  for (const id of ids) { g.members.push({ id, role: 'member', joinedAt: now(), addedBy: req.user.id }); joinGroupRoom(id, g.id); }
  if (ids.length) sysMsg(g, req.user, req.user.name + ' added ' + ids.map((id) => userById(id).name).join(', '));
  save(); groupChanged(g); res.json(groupView(req.user, g));
});
app.delete('/api/groups/:id/members/:uid', auth, (req, res) => {
  const g = groupFor(req, res, true); if (!g) return;
  const uid = req.params.uid; if (!gMember(g, uid) || uid === req.user.id) return res.status(400).json({ error: 'Not a member.' });
  g.members = g.members.filter((x) => x.id !== uid); leaveGroupRoom(uid, g.id);
  sysMsg(g, req.user, req.user.name + ' removed ' + (userById(uid)?.name || 'a member'));
  save(); groupChanged(g, [uid]); res.json(groupView(req.user, g));
});
app.post('/api/groups/:id/admin', auth, (req, res) => {
  const g = groupFor(req, res, true); if (!g) return;
  const x = g.members.find((y) => y.id === String(req.body?.uid)); if (!x) return res.status(400).json({ error: 'Not a member.' });
  x.role = req.body?.admin ? 'admin' : 'member';
  if (!g.members.some((y) => y.role === 'admin')) { x.role = 'admin'; return res.status(400).json({ error: 'A group needs at least one admin.' }); }
  sysMsg(g, req.user, (userById(x.id)?.name || 'A member') + (x.role === 'admin' ? ' is now an admin' : ' is no longer an admin'));
  save(); groupChanged(g); res.json(groupView(req.user, g));
});
app.post('/api/groups/:id/leave', auth, (req, res) => {
  const g = groupFor(req, res); if (!g) return;
  sysMsg(g, req.user, req.user.name + ' left');
  g.members = g.members.filter((x) => x.id !== req.user.id); leaveGroupRoom(req.user.id, g.id);
  if (g.members.length && !g.members.some((x) => x.role === 'admin')) g.members.sort((a, b) => a.joinedAt - b.joinedAt)[0].role = 'admin';
  if (!g.members.length) db.groups = db.groups.filter((x) => x.id !== g.id);
  save(); groupChanged(g, [req.user.id]); res.json({ ok: true });
});
app.post('/api/groups/:id/invite', auth, (req, res) => { const g = groupFor(req, res, true); if (!g) return; g.invite = inviteCode(); save(); res.json(groupView(req.user, g)); });
app.post('/api/groups/join', auth, (req, res) => {
  const code = String(req.body?.code || ''); const g = code.length > 8 && db.groups.find((x) => x.invite === code);
  if (!g) return res.status(404).json({ error: 'This invite link is not valid any more.' });
  if (!gMember(g, req.user.id)) {
    if (g.members.length >= GROUP_MAX) return res.status(400).json({ error: 'This group is full.' });
    g.members.push({ id: req.user.id, role: 'member', joinedAt: now(), via: 'link' }); joinGroupRoom(req.user.id, g.id);
    sysMsg(g, req.user, req.user.name + ' joined using the invite link'); save(); groupChanged(g);
  }
  res.json(groupView(req.user, g));
});
app.get('/api/groups/preview/:code', auth, (req, res) => {
  const g = db.groups.find((x) => x.invite === req.params.code);
  if (!g) return res.status(404).json({ error: 'This invite link is not valid any more.' });
  res.json({ name: g.name, memberCount: g.members.length, description: g.description || '', member: gMember(g, req.user.id) });
});
app.post('/api/groups/:id/photo', auth, express.raw({ type: ['image/*'], limit: '3mb' }), async (req, res) => {
  const g = groupFor(req, res); if (!g) return;
  if (g.settings.onlyAdminsEdit && !gAdmin(g, req.user.id)) return res.status(403).json({ error: 'Only admins can change the group photo.' });
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Choose a photo.' });
  const old = g.photo, id = newId();
  try { await saveChatFile(id, req.body, String(req.headers['content-type']).split(';')[0]); } catch { return res.status(500).json({ error: 'Could not save the photo.' }); }
  Object.assign(g, { photo: id, photoSize: req.body.length, photoMime: String(req.headers['content-type']).split(';')[0], photoV: now() });
  if (old) deleteChatFile(old);
  sysMsg(g, req.user, req.user.name + ' changed the group photo'); save(); groupChanged(g);
  res.json(groupView(req.user, g));
});
app.get('/api/groups/:id/photo', (req, res) => {
  let viewer = null; try { viewer = findUserByToken(String(req.query.t || '')); } catch { /* bad token */ }
  const g = groupById(req.params.id);
  if (!viewer || !g || !g.photo || !gMember(g, viewer.id)) return res.status(404).end();
  res.setHeader('Content-Type', g.photoMime); res.setHeader('Cache-Control', 'private, max-age=86400');
  const st = chatFileStream(g.photo, 0, g.photoSize - 1); st.on('error', () => res.destroy()); st.pipe(res);
});

// ---------- AI (Google Gemini, free tier) ----------
// Set GEMINI_API_KEY (free from aistudio.google.com). Optional GEMINI_MODEL.
// Accept the key even if the variable name was typed slightly differently on Render (spaces, case, GEMINI_KEY…)
const GEMINI_KEY = (() => {
  const direct = process.env.GEMINI_API_KEY || process.env.GEMINI_KEY || process.env.GOOGLE_AI_KEY || process.env.GOOGLE_API_KEY;
  if (direct) return direct.trim();
  const k = Object.keys(process.env).find((n) => ['GEMINIAPIKEY', 'GEMINIKEY'].includes(n.replace(/[\s_-]/g, '').toUpperCase()) && String(process.env[n]).trim());
  return k ? String(process.env[k]).trim() : '';
})();
console.log(GEMINI_KEY
  ? '[ai] Maata AI: Gemini key found (' + GEMINI_KEY.slice(0, 4) + '…' + GEMINI_KEY.slice(-4) + ', ' + GEMINI_KEY.length + ' characters)' + (/^(AIza|AQ\.)/.test(GEMINI_KEY) ? '' : ' - WARNING: Gemini keys normally start with "AIza"')
  : '[ai] Maata AI: OFF - no GEMINI_API_KEY in Environment. Variables seen: ' + Object.keys(process.env).filter((n) => /KEY|SECRET|URI|PASSWORD/i.test(n)).join(', '));
let aiLastError = null;
const GEMINI_MODELS = [...new Set([process.env.GEMINI_MODEL, 'gemini-flash-latest', 'gemini-2.5-flash', 'gemini-2.0-flash'].filter(Boolean))];
let geminiModelOk = null, aiCount = 0;
// How the key is sent: Google's classic keys ("AIza…") and newer keys ("AQ.…") are accepted in different ways,
// so we try each way once and remember the one that works.
const GEMINI_BASE = 'https://generativelanguage.googleapis.com/v1beta/';
let geminiAuthOk = null;
const AUTH_WAYS = ['header', 'query', 'bearer'];
function geminiFetch(path, way, init = {}) {
  const headers = { 'Content-Type': 'application/json', ...(init.headers || {}) };
  let url = GEMINI_BASE + path;
  if (way === 'header') headers['x-goog-api-key'] = GEMINI_KEY;
  else if (way === 'query') url += (url.includes('?') ? '&' : '?') + 'key=' + encodeURIComponent(GEMINI_KEY);
  else headers.Authorization = 'Bearer ' + GEMINI_KEY;
  return fetch(url, { ...init, headers, signal: AbortSignal.timeout(20000) });
}
// Ask Google which models this key can use, and pick the best "flash" model (fast and free-tier friendly)
let modelListAt = 0;
async function discoverModels() {
  if (geminiModelOk || now() - modelListAt < 600000) return;
  modelListAt = now();
  for (const way of geminiAuthOk ? [geminiAuthOk] : AUTH_WAYS) {
    try {
      const r = await geminiFetch('models?pageSize=200', way, { method: 'GET' });
      const d = await r.json();
      if (!r.ok) { aiLastError = 'list models (' + way + '): ' + r.status + ' ' + String(d.error?.message || '').slice(0, 150); console.error('[ai]', aiLastError); continue; }
      geminiAuthOk = way;
      const names = (d.models || []).filter((m) => (m.supportedGenerationMethods || []).includes('generateContent')).map((m) => String(m.name).replace(/^models\//, ''));
      const score = (n) => (/flash/.test(n) ? 100 : 0) - (/lite/.test(n) ? 20 : 0) - (/preview|exp|image|tts|audio|live|thinking|embedding|vision/.test(n) ? 50 : 0) + (/latest/.test(n) ? 30 : 0) + (parseFloat((n.match(/(\d+(\.\d+)?)/) || [0, 0])[1]) || 0);
      const best = names.sort((a, b) => score(b) - score(a)).slice(0, 3);
      if (best.length) { GEMINI_MODELS.splice(0, GEMINI_MODELS.length, ...new Set([process.env.GEMINI_MODEL, ...best].filter(Boolean))); console.log('[ai] models available for this key:', best.join(', '), '| auth:', way); }
      return;
    } catch (e) { aiLastError = 'list models: ' + e.message; console.error('[ai]', aiLastError); }
  }
}
// Spare models to switch to when Google says one is busy (503) — tried after the preferred ones
const GEMINI_SPARES = ['gemini-flash-lite-latest', 'gemini-2.5-flash', 'gemini-2.5-flash-lite', 'gemini-2.0-flash', 'gemini-2.0-flash-lite'];
const busyUntil = new Map(); // model -> time; skip a model for a minute after it says "busy"
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function gemini(system, contents, { json = false, maxTokens = 600, temperature = 0.7, fast = false } = {}) {
  if (!GEMINI_KEY) return { error: 'not-configured' };
  await discoverModels();
  // fast: no "thinking" (live call interpreter must answer in 1–2 s, and thinking can use up the whole answer)
  const bodyFor = (model) => JSON.stringify({ system_instruction: { parts: [{ text: system }] }, contents,
    generationConfig: { temperature, maxOutputTokens: maxTokens, ...(json ? { responseMimeType: 'application/json' } : {}), ...(fast && /2\.5-flash/.test(model) ? { thinkingConfig: { thinkingBudget: 0 } } : {}) } });
  // the model that worked last time first, then the others, then spares; models that were busy a moment ago go last
  const all = [...new Set([geminiModelOk, ...GEMINI_MODELS, ...GEMINI_SPARES].filter(Boolean))];
  const models = [...all.filter((m) => !(busyUntil.get(m) > now())), ...all.filter((m) => busyUntil.get(m) > now())].slice(0, 6);
  const ways = geminiAuthOk ? [geminiAuthOk] : AUTH_WAYS;
  let last = { error: 'service' }, sawBusy = false;
  for (const model of models) {
    for (const way of ways) {
      let r, d;
      for (let attempt = 0; attempt < 2; attempt++) { // a busy model gets one quick retry before we move on
        try { r = await geminiFetch('models/' + model + ':generateContent', way, { method: 'POST', body: bodyFor(model) }); d = await r.json().catch(() => ({})); }
        catch (e) { r = null; d = { error: { message: e.message } }; }
        const busy = !r || [500, 502, 503, 504].includes(r.status) || /overloaded|high demand|UNAVAILABLE|try again later/i.test(String(d?.error?.message || d?.error?.status || ''));
        if (!busy) break;
        sawBusy = true; aiLastError = model + ': busy (' + (r ? r.status : 'network') + ')'; console.error('[ai]', aiLastError);
        if (attempt === 0) await sleep(1200);
        else { busyUntil.set(model, now() + 60000); r = null; }
      }
      if (!r) break; // still busy → next model
      if (r.status === 429) { aiLastError = model + ': rate limit (429)'; busyUntil.set(model, now() + 60000); last = { error: 'limit' }; break; } // free limit for this model → try another model
      if (r.status === 404) { aiLastError = 'model not found: ' + model; console.error('[ai]', aiLastError); last = { error: 'service' }; break; }
      if (r.status === 401 || (r.status === 400 && /API key|credential|UNAUTHENTICATED/i.test(JSON.stringify(d)))) {
        aiLastError = 'auth (' + way + '): ' + r.status + ' ' + String(d.error?.message || '').slice(0, 150); console.error('[ai]', aiLastError);
        last = { error: /API_KEY_INVALID|API key not valid/i.test(JSON.stringify(d)) ? 'badkey' : 'service' }; continue;
      }
      if (d.error) {
        const msg = String(d.error.message || ''), why = JSON.stringify(d.error.details || '');
        aiLastError = model + ': ' + r.status + ' ' + msg.slice(0, 180); console.error('[ai]', aiLastError);
        if (r.status === 403 || /SERVICE_DISABLED|has not been used|is disabled|PERMISSION_DENIED/i.test(msg + why)) return { error: 'disabled' };
        if (/location is not supported|User location/i.test(msg)) return { error: 'region' };
        last = { error: 'service' }; break;
      }
      const text = (d.candidates?.[0]?.content?.parts || []).map((p) => p.text || '').join('').trim();
      if (!text) return { error: d.promptFeedback?.blockReason ? 'blocked' : 'empty' };
      if (model !== geminiModelOk && !busyUntil.has(geminiModelOk)) geminiModelOk = model;
      geminiAuthOk = way; aiCount++; aiLastError = null;
      return { text };
    }
  }
  return sawBusy && last.error === 'service' ? { error: 'busy' } : last;
}
const AI_ERR = { busy: 'Maata AI is very busy right now (Google servers are crowded). Please try again in a minute. 🙏', badkey: 'The Gemini API key is not valid. Please copy the key again from aistudio.google.com and update GEMINI_API_KEY on Render.', disabled: 'The Gemini API is not enabled for this key. Create the key at aistudio.google.com (it switches the API on automatically).', region: 'Gemini is not available for this server location right now.', 'not-configured': 'Maata AI is not switched on yet. The admin needs to add GEMINI_API_KEY.', limit: 'Maata AI is busy (free limit reached). Please try again later.', service: 'Maata AI is not available right now. Try again.', blocked: 'Maata AI cannot answer that.', empty: 'Maata AI had no answer. Try asking differently.', quota: 'You have used your Maata AI requests for today. Try again tomorrow.' };
const aiUse = new Map();
function aiQuota(u, n = 1) {
  const day = dayKey(now()), q = aiUse.get(u.id);
  const used = q && q.day === day ? q.n : 0;
  if (used + n > 60) return false;
  aiUse.set(u.id, { day, n: used + n }); return true;
}
// Maata AI chat: English unless the person asks for another language, then stay in that language
const AI_CHAT_LANG = 'LANGUAGE RULE: Reply in simple, clear English by default — even if the person writes in Telugu, Hindi or another language, or in Telugu/Hindi typed with English letters. ' +
  'Switch to another language ONLY when the person clearly asks for it, for example "reply in Telugu", "Telugu lo cheppu", "Tamil la sollu", "Hindi mein batao", "Kannada alli heli". ' +
  'Then write in that language using its own script (Telugu లిపి, Tamil தமிழ், Hindi हिन्दी, etc.) and keep using it for the rest of the chat, until the person asks for a different language or for English again. ' +
  'Keep replies short and friendly, suitable for a phone chat. Use plain text, no markdown symbols like ** or #.';
const LANG_HINT = 'Reply in the same language and script the person used. If they write Telugu in English letters (like "ela unnav"), reply the same way. Keep replies short and friendly, suitable for a phone chat. Use plain text, no markdown symbols like ** or #.';
const plain = (t) => String(t || '').replace(/\*\*(.*?)\*\*/g, '$1').replace(/^#+\s*/gm, '').replace(/^\s*[-*]\s+/gm, '• ').trim();
app.get('/api/ai/status', auth, (req, res) => res.json({ enabled: !!GEMINI_KEY, model: geminiModelOk, lastError: aiLastError }));

// 1) Maata AI chat (history is private to each customer)
app.get('/api/ai/history', auth, (req, res) => res.json(db.aichats.filter((m) => m.userId === req.user.id).slice(-100)));
app.delete('/api/ai/history', auth, (req, res) => { db.aichats = db.aichats.filter((m) => m.userId !== req.user.id); save(); res.json({ ok: true }); });
// Maata AI answers in English unless the person asks for another language. The server remembers the choice
// (per person) and tells Gemini the exact language with EVERY question — old Telugu-style replies in the chat
// history, or a Telugu name, can no longer pull the answers away from English.
const AI_LANGS = {
  English: ['english', 'ఇంగ్లీష్', 'अंग्रेज़ी', 'अंग्रेजी'], Telugu: ['telugu', 'తెలుగు'], Hindi: ['hindi', 'हिंदी', 'हिन्दी'], Tamil: ['tamil', 'தமிழ்'],
  Kannada: ['kannada', 'ಕನ್ನಡ'], Malayalam: ['malayalam', 'മലയാളം'], Bengali: ['bengali', 'bangla', 'বাংলা'], Marathi: ['marathi', 'मराठी'],
  Gujarati: ['gujarati', 'ગુજરાતી'], Punjabi: ['punjabi', 'ਪੰਜਾਬੀ'], Urdu: ['urdu', 'اردو'], Odia: ['odia', 'oriya', 'ଓଡ଼ିଆ'],
};
const LANG_SCRIPT = { Telugu: 'Telugu script (తెలుగు లిపి)', Hindi: 'Devanagari script (हिन्दी)', Tamil: 'Tamil script (தமிழ்)', Kannada: 'Kannada script (ಕನ್ನಡ)', Malayalam: 'Malayalam script (മലയാളം)', Bengali: 'Bengali script (বাংলা)', Marathi: 'Devanagari script (मराठी)', Gujarati: 'Gujarati script (ગુજરાતી)', Punjabi: 'Gurmukhi script (ਪੰਜਾਬੀ)', Urdu: 'Urdu script (اردو)', Odia: 'Odia script (ଓଡ଼ିଆ)' };
// "reply in Telugu", "telugu lo cheppu", "Tamil la sollu", "Hindi mein batao", "తెలుగులో చెప్పు", "speak English" …
function askedLanguage(text) {
  const t = String(text || '').toLowerCase();
  const trigger = /(reply|answer|respond|speak|talk|write|say|tell|explain|switch|change|language|bhasha|bhaasha|lo |lo$|lone|mein|me |la |alli|il |లో|में|மொழி|ல்|ನಲ್ಲಿ|ഇൽ|ഭാഷ|plz|please)/;
  for (const [lang, words] of Object.entries(AI_LANGS)) {
    for (const w of words) {
      const i = t.indexOf(w); if (i < 0) continue;
      const around = t.slice(Math.max(0, i - 25), i + w.length + 25);
      if (trigger.test(around) || t.trim() === w || /^[^a-z]*$/.test(w)) return lang;
    }
  }
  return null;
}
app.post('/api/ai/chat', auth, async (req, res) => {
  const text = String(req.body?.text || '').trim().slice(0, 2000);
  if (!text) return res.status(400).json({ error: 'Type a question.' });
  if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
  const mine = { id: newId(), userId: req.user.id, role: 'user', text, ts: now() };
  const asked = askedLanguage(text);
  if (asked && asked !== (req.user.aiLang || 'English')) { req.user.aiLang = asked; save(); }
  const lang = req.user.aiLang || 'English';
  const langRule = lang === 'English'
    ? 'Reply ONLY in proper English (English words and grammar). Never write Telugu, Hindi or any Indian language, not even typed in English letters (no "Nenu", "meeku", "cheppandi", "kya", "aap", etc.), even if the person writes that way or earlier replies did. If the person wants another language, they will ask for it.'
    : 'Reply ONLY in ' + lang + ', written in ' + LANG_SCRIPT[lang] + ' — not in English letters. Keep using ' + lang + ' until the person asks for a different language.';
  const past = db.aichats.filter((m) => m.userId === req.user.id).slice(-10);
  const r = await gemini('You are Maata AI, a helpful assistant inside the Maata chat app, made in India. The person chatting with you is ' + req.user.name + '. LANGUAGE: ' + langRule + ' Keep replies short and friendly for a phone chat. Use plain text, no markdown symbols like ** or #. Never claim to be a human. For medical, legal or money decisions, give general information and suggest asking an expert.',
    [...past.map((m) => ({ role: m.role === 'ai' ? 'model' : 'user', parts: [{ text: m.text }] })),
      { role: 'user', parts: [{ text: text + '\n\n[Instruction for Maata AI, not written by the person: answer in ' + lang + (lang === 'English' ? ' only' : ' using ' + LANG_SCRIPT[lang]) + '.]' }] }]);
  if (r.error) return res.status(r.error === 'limit' ? 429 : 502).json({ error: AI_ERR[r.error] + (r.error === 'service' && aiLastError ? ' (' + aiLastError + ')' : '') });
  const ai = { id: newId(), userId: req.user.id, role: 'ai', text: plain(r.text).slice(0, 4000), ts: now() };
  db.aichats.push(mine, ai);
  const all = db.aichats.filter((m) => m.userId === req.user.id);
  if (all.length > 200) { const drop = new Set(all.slice(0, all.length - 200).map((m) => m.id)); db.aichats = db.aichats.filter((m) => !drop.has(m.id)); }
  save(); res.json({ user: mine, ai });
});

// 2) Writing help & smart replies (only what the person chooses to send)
const WRITE_TASKS = {
  polite: 'Rewrite this message to be polite and respectful.', short: 'Make this message shorter and clearer.', fix: 'Fix spelling and grammar. Keep the meaning and language.',
  friendly: 'Rewrite this message in a warm, friendly tone.', formal: 'Rewrite this message in a formal, professional tone for a business.',
  te: 'Translate this message to Telugu script.', en: 'Translate this message to simple English.', hi: 'Translate this message to Hindi (Devanagari).',
  write: 'Write a short chat message for this request.',
};
app.post('/api/ai/write', auth, async (req, res) => {
  const text = String(req.body?.text || '').trim().slice(0, 2000), task = WRITE_TASKS[req.body?.task] ? req.body.task : 'write';
  if (!text) return res.status(400).json({ error: 'Type something first.' });
  if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
  const r = await gemini('You help people write chat messages in the Maata app. ' + WRITE_TASKS[task] + ' ' + (['te', 'en', 'hi'].includes(task) ? '' : LANG_HINT) + ' Return ONLY the final message text, nothing else.', [{ role: 'user', parts: [{ text }] }], { temperature: 0.5 });
  if (r.error) return res.status(r.error === 'limit' ? 429 : 502).json({ error: AI_ERR[r.error] });
  res.json({ text: plain(r.text).replace(/^"|"$/g, '').slice(0, 4000) });
});
// Maata Studio: AI subtitles. The phone sends the reel's sound (small WAV); Gemini listens and returns timed lines,
// in the spoken language or translated into the chosen Indian language (in its own script).
const SUB_LANGS = { te: 'Telugu (Telugu script)', en: 'English', hi: 'Hindi (Devanagari)', ta: 'Tamil (Tamil script)', kn: 'Kannada (Kannada script)', ml: 'Malayalam (Malayalam script)', bn: 'Bengali (Bengali script)', mr: 'Marathi (Devanagari)', gu: 'Gujarati (Gujarati script)', pa: 'Punjabi (Gurmukhi)', ur: 'Urdu (Urdu script)', or: 'Odia (Odia script)' };
app.post('/api/ai/subtitles', auth, express.raw({ type: ['audio/*'], limit: '6mb' }), async (req, res) => {
  if (!Buffer.isBuffer(req.body) || req.body.length < 1000) return res.status(400).json({ error: 'No sound found in this video.' });
  if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
  const lang = SUB_LANGS[String(req.query.lang || '')] || null, mime = String(req.headers['content-type'] || 'audio/wav').split(';')[0];
  const prompt = 'Listen to this short video sound and write subtitles. Return ONLY a JSON array like [{"start":0.0,"end":2.4,"text":"..."}]. ' +
    'start/end are seconds from the beginning. Keep each line short (at most 7 words), in time order, no overlaps. ' +
    (lang ? 'Write every line in ' + lang + ' (translate if the speech is in another language).' : 'Write the words in the language that is spoken, in that language\'s own script.') +
    ' If there is no speech (only music or silence), return [].';
  const r = await gemini('You are a careful subtitle writer.', [{ role: 'user', parts: [{ inline_data: { mime_type: mime, data: req.body.toString('base64') } }, { text: prompt }] }], { json: true, maxTokens: 2000, temperature: 0.2 });
  if (r.error) return res.status(r.error === 'limit' ? 429 : 502).json({ error: AI_ERR[r.error] || 'AI is busy, try again.' });
  let segs = []; try { segs = JSON.parse(r.text.replace(/^```json|```$/g, '').trim()); } catch { segs = []; }
  segs = (Array.isArray(segs) ? segs : []).map((x) => ({ start: Math.max(0, Number(x.start) || 0), end: Math.max(0, Number(x.end) || 0), text: String(x.text || '').trim().slice(0, 80) }))
    .filter((x) => x.text && x.end > x.start).slice(0, 120);
  res.json({ segments: segs });
});
// Catalogue: AI writes the product description from the name, price, shop type and (if added) the photo
app.post('/api/ai/product-desc', auth, express.raw({ type: ['image/*'], limit: '2mb' }), async (req, res) => {
  const name = clip(req.query.name, 80); if (name.length < 2) return res.status(400).json({ error: 'Enter the product name first.' });
  if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
  const price = Number(req.query.price) > 0 ? Number(req.query.price) : null;
  const lang = { en: 'English', te: 'Telugu (Telugu script)', both: 'English, then the same in Telugu (Telugu script) on the next line', hi: 'Hindi (Devanagari script)' }[String(req.query.lang || 'en')] || 'English';
  const shop = req.user.business ? req.user.business.name + ' (' + req.user.business.category + ')' : 'a local shop in India';
  const hasPhoto = Buffer.isBuffer(req.body) && req.body.length > 500;
  const parts = [];
  if (hasPhoto) parts.push({ inline_data: { mime_type: String(req.headers['content-type'] || 'image/jpeg').split(';')[0], data: req.body.toString('base64') } });
  parts.push({ text: 'Product name: ' + name + (price ? '\nPrice: Rs ' + price : '') + '\nShop: ' + shop + (hasPhoto ? '\nThe photo shows the product.' : '') +
    '\n\nWrite a catalogue description for this product in ' + lang + '. 2 to 3 short sentences, at most 280 characters (per language). Say what it is, who it is for and its main useful features. ' +
    'Use only facts that are typical for this kind of product' + (hasPhoto ? ' or clearly visible in the photo' : '') + ' — do NOT invent exact specifications (battery mAh, warranty years, model numbers) unless they are in the product name. ' +
    'Friendly, simple words for Indian customers. No price, no emojis overload (max 1), no hashtags, no markdown. Return only the description.' });
  const r = await gemini('You write short, honest product descriptions for small shops in India.', [{ role: 'user', parts }], { maxTokens: 400, temperature: 0.6 });
  if (r.error) return res.status(r.error === 'limit' ? 429 : 502).json({ error: AI_ERR[r.error] || 'AI is busy, try again.' });
  res.json({ description: plain(r.text).replace(/^["“]|["”]$/g, '').trim().slice(0, 500) });
});
app.post('/api/ai/replies', auth, async (req, res) => {
  const msgs = (Array.isArray(req.body?.messages) ? req.body.messages : []).slice(-6).map((m) => ({ who: m.mine ? 'Me' : 'Them', text: String(m.text || '').slice(0, 500) })).filter((m) => m.text);
  if (!msgs.length) return res.status(400).json({ error: 'No messages to reply to.' });
  if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
  const r = await gemini('Suggest exactly 3 different short replies (max 12 words each) that "Me" could send next in this chat. ' + LANG_HINT + ' Return a JSON array of 3 strings only.',
    [{ role: 'user', parts: [{ text: msgs.map((m) => m.who + ': ' + m.text).join('\n') }] }], { json: true, temperature: 0.8, maxTokens: 200 });
  if (r.error) return res.status(r.error === 'limit' ? 429 : 502).json({ error: AI_ERR[r.error] });
  let list = []; try { list = JSON.parse(r.text); } catch { list = r.text.split('\n'); }
  res.json({ replies: list.map((x) => plain(String(x)).replace(/^\d+[.)]\s*/, '')).filter(Boolean).slice(0, 3) });
});

// 3) Shop auto-reply: answers customers from the owner's business details
app.get('/api/ai/autoreply', auth, (req, res) => res.json(req.user.autoReply || { on: false, info: '', mode: 'offline', from: '21:00', to: '09:00' }));
app.put('/api/ai/autoreply', auth, (req, res) => {
  const b = req.body || {}, hhmm = (x, d) => (/^\d{2}:\d{2}$/.test(x || '') ? x : d);
  req.user.autoReply = { on: !!b.on, info: String(b.info || '').slice(0, 3000), mode: ['offline', 'always', 'hours'].includes(b.mode) ? b.mode : 'offline', from: hhmm(b.from, '21:00'), to: hhmm(b.to, '09:00') };
  save(); res.json(req.user.autoReply);
});
const arHits = new Map();
function inQuietHours(ar) {
  const d = new Date(now() + TZ * 60000), cur = d.getUTCHours() * 60 + d.getUTCMinutes();
  const [fh, fm] = ar.from.split(':').map(Number), [th, tm] = ar.to.split(':').map(Number), f = fh * 60 + fm, t = th * 60 + tm;
  return f <= t ? cur >= f && cur < t : cur >= f || cur < t;
}
async function maybeAutoReply(customer, owner, m) {
  const ar = owner.autoReply;
  if (!ar || !ar.on || !GEMINI_KEY || m.type !== 'text' || m.ai || customer.id === owner.id) return;
  if (ar.mode === 'offline' && isOnline(owner.id)) return;
  if (ar.mode === 'hours' && !inQuietHours(ar)) return;
  const key = owner.id + '|' + customer.id, t = now();
  const hits = (arHits.get(key) || []).filter((x) => t - x < 3600_000);
  const ownerHits = (arHits.get(owner.id) || []).filter((x) => t - x < 3600_000);
  if (hits.length >= 6 || ownerHits.length >= 40 || (hits.length && t - hits[hits.length - 1] < 20_000)) return;
  hits.push(t); ownerHits.push(t); arHits.set(key, hits); arHits.set(owner.id, ownerHits);
  const recent = db.messages.filter((x) => !x.group && ((x.from === customer.id && x.to === owner.id) || (x.from === owner.id && x.to === customer.id)) && x.type === 'text').slice(-6);
  const r = await gemini('You are the automatic assistant replying on behalf of "' + owner.name + '" in the Maata chat app, because they are not available right now. Business details written by the owner:\n"""' + (ar.info || 'No details given.') + '"""\nAnswer the customer using ONLY these details. If the answer is not in the details (or about exact stock, discounts or bookings), say politely that ' + owner.name + ' will reply soon. Never invent prices or promises. ' + LANG_HINT + ' Maximum 3 sentences.',
    [{ role: 'user', parts: [{ text: recent.map((x) => (x.from === owner.id ? owner.name : 'Customer') + ': ' + x.text).join('\n') }] }], { temperature: 0.4, maxTokens: 250 });
  if (r.error || !canMessage(owner, customer.id).target) return;
  deliverMessage(owner, customer, { type: 'text', text: plain(r.text).slice(0, 1500), ai: true }, null);
}

// ---------- App language: interface text translated once per language, then cached for everyone ----------
const UI_LANGS = { hi: 'Hindi', mr: 'Marathi', gu: 'Gujarati', ta: 'Tamil', bn: 'Bengali', te: 'Telugu', kn: 'Kannada', ml: 'Malayalam', pa: 'Punjabi', ur: 'Urdu' };
const i18nHits = new Map(), i18nBusy = new Map();
function i18nDoc(lang) { let d = db.i18n.find((x) => x.id === lang); if (!d) { d = { id: lang, map: {} }; db.i18n.push(d); } return d; }
async function translateUi(lang, list) {
  // 1) Gemini (understands these are app buttons/menus), 2) Google Translate, 3) free service
  if (GEMINI_KEY) {
    const r = await gemini('You translate the user interface of "Maata", a chat and calling app (like WhatsApp), from English to ' + UI_LANGS[lang] + '. Use the short, natural words that ' + UI_LANGS[lang] + ' apps use for buttons and menus. Keep emojis, numbers, symbols and the names "Maata", "Maata AI", "Google", "Gemini", "WhatsApp", "PIN", "GPS", "GIF" unchanged. Return JSON: {"t": [translations in the same order]}.',
      [{ role: 'user', parts: [{ text: JSON.stringify(list) }] }], { json: true, temperature: 0.2, maxTokens: 8000 });
    if (!r.error) { try { const t = JSON.parse(r.text).t; if (Array.isArray(t) && t.length === list.length) return t.map(String); } catch { /* fall through */ } }
  }
  if (GOOGLE_TRANSLATE_KEY) {
    const out = [];
    for (let i = 0; i < list.length; i += 100) {
      const r = await fetch('https://translation.googleapis.com/language/translate/v2?key=' + encodeURIComponent(GOOGLE_TRANSLATE_KEY), {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ q: list.slice(i, i + 100), source: 'en', target: lang, format: 'text' }), signal: AbortSignal.timeout(15000),
      }).then((x) => x.json()).catch(() => null);
      const t = r?.data?.translations; if (!t) return null; out.push(...t.map((x) => x.translatedText));
    }
    return out;
  }
  const out = [];
  for (const s2 of list.slice(0, 25)) out.push(await translate(s2, 'en', lang));
  return out;
}
app.post('/api/i18n', async (req, res) => {
  const lang = String(req.body?.lang || '');
  if (!UI_LANGS[lang]) return res.status(400).json({ error: 'Unknown language.' });
  const doc = i18nDoc(lang);
  const want = [...new Set((Array.isArray(req.body?.strings) ? req.body.strings : []).map((x) => String(x).trim()).filter((x) => x && x.length <= 220))].slice(0, 300);
  const missing = want.filter((x) => !(x in doc.map));
  if (missing.length && Object.keys(doc.map).length < 6000) {
    const key = req.ip, t = now(), l = (i18nHits.get(key) || []).filter((x) => t - x < 3600_000);
    if (l.length < 60 && !i18nBusy.get(lang)) {
      l.push(t); i18nHits.set(key, l); i18nBusy.set(lang, true);
      try {
        const batch = missing.slice(0, 150), out = await translateUi(lang, batch);
        if (out) batch.forEach((s2, i) => { if (out[i] && String(out[i]).trim()) doc.map[s2] = String(out[i]).trim().slice(0, 400); });
        save();
      } catch (e) { console.error('[i18n]', e.message); } finally { i18nBusy.set(lang, false); }
    }
  }
  const map = {}; for (const s2 of want) if (s2 in doc.map) map[s2] = doc.map[s2];
  res.json({ map, missing: want.filter((x) => !(x in doc.map)).length });
});

// ---------- Batch: edit, disappearing, view once, search, two-step, status privacy ----------
const DISAPPEAR = [0, 86400, 604800, 7776000];
const disappearLabel = (s) => ({ 86400: '24 hours', 604800: '7 days', 7776000: '90 days' }[s] || 'off');
function chatTtl(fromUser, targetId) {
  const gid = gidOf(targetId);
  if (gid) return (groupById(gid) || {}).disappearing || 0;
  return (fromUser.disappearing || {})[targetId] || 0;
}
app.post('/api/chats/:id/disappearing', auth, (req, res) => {
  const secs = Number(req.body?.seconds) || 0; if (!DISAPPEAR.includes(secs)) return res.status(400).json({ error: 'Choose 24 hours, 7 days, 90 days or off.' });
  const id = req.params.id, gid = gidOf(id);
  if (gid) {
    const g = groupById(gid); if (!gMember(g, req.user.id)) return res.status(404).json({ error: 'Group not found.' });
    if (g.settings.onlyAdminsEdit && !gAdmin(g, req.user.id)) return res.status(403).json({ error: 'Only admins can change this.' });
    g.disappearing = secs; save(); groupChanged(g);
    sysMsg(g, req.user, req.user.name + (secs ? ' turned on disappearing messages. New messages will disappear after ' + disappearLabel(secs) + '.' : ' turned off disappearing messages.'));
    return res.json({ seconds: secs });
  }
  const other = userById(id); if (!other || !canMessage(req.user, id).target) return res.status(404).json({ error: 'Chat not found.' });
  (req.user.disappearing = req.user.disappearing || {})[id] = secs; (other.disappearing = other.disappearing || {})[req.user.id] = secs; save();
  deliverMessage(req.user, other, { type: 'system', text: req.user.name + (secs ? ' turned on disappearing messages. New messages will disappear after ' + disappearLabel(secs) + '.' : ' turned off disappearing messages.') }, null);
  res.json({ seconds: secs });
});
app.get('/api/chats/:id/settings', auth, (req, res) => {
  const gid = gidOf(req.params.id);
  res.json({ disappearing: gid ? (groupById(gid) || {}).disappearing || 0 : (req.user.disappearing || {})[req.params.id] || 0 });
});
function cleanExpiredMessages() {
  const t = now(), gone = db.messages.filter((m) => m.expiresAt && m.expiresAt <= t);
  if (!gone.length) return;
  const ids = new Set(gone.map((m) => m.id));
  db.messages = db.messages.filter((m) => !ids.has(m.id));
  for (const m of gone) {
    if (m.file) releaseFile(m.file.id);
    if (m.group) io.to('grp:' + m.group).emit('msg:expired', { ids: [m.id] });
    else for (const u of [m.from, m.to]) io.to('user:' + u).emit('msg:expired', { ids: [m.id] });
  }
  save();
}
setInterval(cleanExpiredMessages, 60 * 1000);

// Search my messages across all chats (text, captions, file names)
app.get('/api/search', auth, (req, res) => {
  const q = String(req.query.q || '').trim().toLowerCase(); if (q.length < 2) return res.json([]);
  const me = req.user.id, hidden = new Set(req.user.hiddenMsgs || []), cleared = req.user.clearedChats || {}, out = [];
  for (let i = db.messages.length - 1; i >= 0 && out.length < 60; i--) {
    const m = db.messages[i];
    if (m.deleted || m.type === 'system' || hidden.has(m.id) || (m.expiresAt && m.expiresAt <= now())) continue;
    const chat = m.group ? 'g:' + m.group : m.from === me ? m.to : m.to === me ? m.from : null;
    if (!chat || (m.group && !gMember(groupById(m.group), me)) || m.ts <= (cleared[chat] || 0)) continue;
    const hay = [m.text, m.file && !m.viewOnce ? m.file.name : '', m.contact && m.contact.name, m.poll && m.poll.question, m.event && m.event.title].filter(Boolean).join(' ').toLowerCase();
    if (hay.includes(q)) out.push({ chat, message: m });
  }
  res.json(out);
});

// Two-step verification: a 6-digit PIN asked at every new login
const pinTickets = new Map();
app.post('/api/twostep', auth, async (req, res) => {
  const b = req.body || {};
  if (!(await bcrypt.compare(String(b.password || ''), req.user.passHash))) return res.status(400).json({ error: 'Your current password is wrong.' });
  if (b.off) { delete req.user.twoStep; save(); return res.json({ on: false }); }
  if (!/^\d{6}$/.test(String(b.pin || ''))) return res.status(400).json({ error: 'The PIN must be 6 digits.' });
  req.user.twoStep = { hash: await bcrypt.hash(String(b.pin), 10), hint: String(b.hint || '').slice(0, 60), at: now() }; save();
  res.json({ on: true });
});
app.get('/api/twostep', auth, (req, res) => res.json({ on: !!req.user.twoStep, hint: req.user.twoStep ? req.user.twoStep.hint : '' }));
app.post('/api/login/pin', rateLimit, async (req, res) => {
  const tk = pinTickets.get(String(req.body?.ticket || ''));
  if (!tk || now() - tk.t > 10 * 60000) return res.status(401).json({ error: 'Please log in again.' });
  const u = userById(tk.uid); if (!u || !u.twoStep) return res.status(401).json({ error: 'Please log in again.' });
  if (++tk.tries > 5) { pinTickets.delete(req.body.ticket); return res.status(429).json({ error: 'Too many wrong PINs. Log in again later.' }); }
  if (!(await bcrypt.compare(String(req.body?.pin || ''), u.twoStep.hash))) return res.status(401).json({ error: 'Wrong PIN. ' + (5 - tk.tries) + ' tries left.' });
  pinTickets.delete(req.body.ticket); recordLogin(u, req, 'login'); save();
  res.json({ token: sign(u), user: publicUser(u) });
});

// Status privacy: my contacts / my contacts except… / only share with…
app.get('/api/status/privacy', auth, (req, res) => res.json(req.user.statusPrivacy || { mode: 'contacts', list: [] }));
app.put('/api/status/privacy', auth, (req, res) => {
  const mode = ['contacts', 'except', 'only'].includes(req.body?.mode) ? req.body.mode : 'contacts';
  req.user.statusPrivacy = { mode, list: (Array.isArray(req.body?.list) ? req.body.list : []).map(String).slice(0, 1000) }; save();
  res.json(req.user.statusPrivacy);
});

// ---------- Business: profile, catalogue (products), ads (admin approved) ----------
const BIZ_CATS = ['Mobile & electronics', 'Grocery', 'Clothing', 'Food & restaurant', 'Beauty & salon', 'Health & medical', 'Education', 'Services', 'Automobile', 'Real estate', 'Travel', 'Other'];
const clip = (v, n) => String(v ?? '').trim().slice(0, n);
function bizView(u) {
  const b = u.business; if (!b) return null;
  return { ...b, members: undefined, invSeq: undefined, ordSeq: undefined, settings: undefined, store: undefined, storeOn: !!(b.store && b.store.on), ownerId: u.id, ownerName: u.name, phone: u.phone, productCount: db.products.filter((p) => p.ownerId === u.id).length };
}
app.get('/api/business/:id', auth, (req, res) => {
  const u = userById(req.params.id);
  if (!u || u.status === 'blocked' || blockedOf(u).includes(req.user.id) || !u.business) return res.status(404).json({ error: 'This is not a business account.' });
  res.json(bizView(u));
});
app.put('/api/business', auth, (req, res) => {
  const b = req.body || {};
  if (b.remove) { delete req.user.business; save(); return res.json(null); }
  const name = clip(b.name, 60);
  if (!name) return res.status(400).json({ error: 'Enter your business name.' });
  const web = clip(b.website, 200), email = clip(b.email, 120);
  if (web && !/^https?:\/\/[^\s]+\.[^\s]+$/i.test(web)) return res.status(400).json({ error: 'Website must start with https://' });
  if (email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: 'Enter a valid email address.' });
  req.user.business = { name, category: BIZ_CATS.includes(b.category) ? b.category : 'Other', description: clip(b.description, 600), address: clip(b.address, 300), hours: clip(b.hours, 300), email, website: web, updatedAt: now() };
  save(); res.json(bizView(req.user));
});

// Catalogue
const productOut = (p) => ({ id: p.id, ownerId: p.ownerId, name: p.name, price: p.price, description: p.description, available: p.available, hasPhoto: !!p.photo, photoV: p.photoV || 0, createdAt: p.createdAt });
app.get('/api/catalogue/:ownerId', auth, (req, res) => {
  const u = userById(req.params.ownerId);
  if (!u || u.status === 'blocked' || blockedOf(u).includes(req.user.id)) return res.status(404).json({ error: 'Not found.' });
  const mine = u.id === req.user.id;
  res.json(db.products.filter((p) => p.ownerId === u.id && (mine || p.available)).sort((a, b) => b.createdAt - a.createdAt).map(productOut));
});
app.post('/api/catalogue', auth, (req, res) => {
  if (!req.user.business) return res.status(400).json({ error: 'Create your business profile first.' });
  if (db.products.filter((p) => p.ownerId === req.user.id).length >= 200) return res.status(400).json({ error: 'You can have up to 200 products.' });
  const name = clip(req.body?.name, 80); if (!name) return res.status(400).json({ error: 'Enter a product name.' });
  const price = req.body?.price === '' || req.body?.price == null ? null : Math.round(Number(req.body.price) * 100) / 100;
  if (price !== null && !(price >= 0 && price < 1e8)) return res.status(400).json({ error: 'Enter a valid price.' });
  const p = { id: newId(), ownerId: req.user.id, name, price, description: clip(req.body?.description, 500), available: req.body?.available !== false, createdAt: now() };
  db.products.push(p); save(); res.json(productOut(p));
});
app.put('/api/catalogue/:id', auth, (req, res) => {
  const p = db.products.find((x) => x.id === req.params.id && x.ownerId === req.user.id); if (!p) return res.status(404).json({ error: 'Not found.' });
  const b = req.body || {};
  if (b.name !== undefined) { const n = clip(b.name, 80); if (!n) return res.status(400).json({ error: 'Enter a product name.' }); p.name = n; }
  if (b.price !== undefined) { const pr = b.price === '' || b.price == null ? null : Math.round(Number(b.price) * 100) / 100; if (pr !== null && !(pr >= 0 && pr < 1e8)) return res.status(400).json({ error: 'Enter a valid price.' }); p.price = pr; }
  if (b.description !== undefined) p.description = clip(b.description, 500);
  if (b.available !== undefined) p.available = !!b.available;
  save(); res.json(productOut(p));
});
app.delete('/api/catalogue/:id', auth, (req, res) => {
  const p = db.products.find((x) => x.id === req.params.id && x.ownerId === req.user.id); if (!p) return res.status(404).json({ error: 'Not found.' });
  db.products = db.products.filter((x) => x.id !== p.id); if (p.photo) deleteChatFile(p.photo); save(); res.json({ ok: true });
});
async function savePhotoInto(obj, req, res) {
  if (!Buffer.isBuffer(req.body) || !req.body.length) { res.status(400).json({ error: 'Choose a photo.' }); return false; }
  const id = newId(), mime = String(req.headers['content-type'] || 'image/jpeg').split(';')[0];
  try { await saveChatFile(id, req.body, mime); } catch { res.status(500).json({ error: 'Could not save the photo.' }); return false; }
  if (obj.photo) deleteChatFile(obj.photo);
  Object.assign(obj, { photo: id, photoMime: mime, photoSize: req.body.length, photoV: now() }); save(); return true;
}
app.post('/api/catalogue/:id/photo', auth, express.raw({ type: ['image/*'], limit: '3mb' }), async (req, res) => {
  const p = db.products.find((x) => x.id === req.params.id && x.ownerId === req.user.id); if (!p) return res.status(404).json({ error: 'Not found.' });
  if (await savePhotoInto(p, req, res)) res.json(productOut(p));
});
function streamPhoto(obj, req, res) {
  let viewer = null; try { viewer = findUserByToken(String(req.query.t || '')); } catch { /* bad token */ }
  if (!viewer || !obj || !obj.photo) return res.status(404).end();
  res.setHeader('Content-Type', obj.photoMime); res.setHeader('Cache-Control', 'private, max-age=86400');
  const st = chatFileStream(obj.photo, 0, obj.photoSize - 1); st.on('error', () => res.destroy()); st.pipe(res);
}
app.get('/api/catalogue/photo/:id', (req, res) => streamPhoto(db.products.find((x) => x.id === req.params.id), req, res));


// ==================== 🏪 MAATA BUSINESS: staff, stock, billing (POS), reports, AI ====================
// A business is the owner's account (bizId = owner user id). Staff are other Maata users with a role.
// Every request checks membership and permission on the server; money is kept in paise (whole numbers).
const BIZ_ROLES = { manager: ['bill', 'stock', 'reports', 'void'], cashier: ['bill'], stock: ['stock'], delivery: ['deliver'] };
const BIZ_UNITS = ['pcs', 'kg', 'g', 'l', 'ml', 'box', 'pack', 'dozen', 'm', 'pair', 'set', 'plate'];
const BIZ_TAX = [0, 0.25, 3, 5, 12, 18, 28, 40];
const istNow = () => new Date(now() + 5.5 * 3600000);
const istDayKey = (t) => new Date(t + 5.5 * 3600000).toISOString().slice(0, 10);
const istDayStart = (t = now()) => { const d = new Date(t + 5.5 * 3600000); d.setUTCHours(0, 0, 0, 0); return d.getTime() - 5.5 * 3600000; };
const finYear = (t = now()) => { const d = new Date(t + 5.5 * 3600000), y = d.getUTCFullYear(); return d.getUTCMonth() >= 3 ? y + '-' + String((y + 1) % 100).padStart(2, '0') : (y - 1) + '-' + String(y % 100).padStart(2, '0'); };
const rupees = (p) => '₹' + (Math.round(p) / 100).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
function bizSettings(b) {
  b.settings = { prefix: 'INV', taxMode: 'incl', roundOff: true, allowNegative: false, gstin: '', upi: '', footer: 'Thank you! Visit again.', ...(b.settings || {}) };
  return b.settings;
}
// who is acting, for which business, with which rights (never trusts the bizId alone)
function bizCtx(req, res, need) {
  const bizId = String(req.params.bizId || ''), owner = userById(bizId);
  if (!owner || !owner.business || owner.status === 'blocked') { res.status(404).json({ error: 'Business not found.' }); return null; }
  let role = null, perms = [];
  if (owner.id === req.user.id) { role = 'owner'; perms = ['bill', 'stock', 'reports', 'void', 'settings', 'staff']; }
  else { const m = (owner.business.members || []).find((x) => x.userId === req.user.id && x.status !== 'removed'); if (m) { role = m.role; perms = BIZ_ROLES[m.role] || []; } }
  if (!role) { res.status(403).json({ error: 'You are not part of this business.' }); return null; }
  if (need && !perms.includes(need)) { res.status(403).json({ error: 'Your role (' + role + ') cannot do this. Ask the shop owner.' }); return null; }
  bizSettings(owner.business);
  return { owner, biz: owner.business, bizId: owner.id, role, perms, actor: req.user };
}
function bizLog(ctx, action, ref, detail = '') {
  db.bizLog.push({ id: newId(), bizId: ctx.bizId, actor: ctx.actor.id, actorName: ctx.actor.name, action, ref: ref || '', detail: clip(detail, 300), at: now() });
  if (db.bizLog.length > 60000) db.bizLog.splice(0, db.bizLog.length - 60000);
}
const bizProducts = (bizId) => db.products.filter((p) => p.ownerId === bizId);
const bizProdOut = (p) => ({ ...productOut(p), sku: p.sku || '', barcode: p.barcode || '', unit: p.unit || 'pcs', category: p.category || '', taxRate: p.taxRate ?? 0, stock: p.stock ?? null, lowAt: p.lowAt ?? null, cost: p.cost ?? null, mrp: p.mrp ?? null });
function stockMove(ctx, p, delta, reason, ref) {
  if (p.stock == null) return; // stock not tracked for this product
  p.stock = Math.round((Number(p.stock) + delta) * 1000) / 1000;
  db.stockMoves.push({ id: newId(), bizId: ctx.bizId, productId: p.id, name: p.name, delta, after: p.stock, reason, ref: ref || '', actor: ctx.actor.id, at: now() });
}
function readProductFields(b, p, isNew) {
  if (b.name !== undefined || isNew) { const n = clip(b.name, 80); if (!n) return 'Enter a product name.'; p.name = n; }
  const money = (v) => (v === '' || v == null ? null : Math.round(Number(v) * 100) / 100);
  if (b.price !== undefined) { const v = money(b.price); if (v !== null && !(v >= 0 && v < 1e8)) return 'Enter a valid price.'; p.price = v; }
  if (b.cost !== undefined) { const v = money(b.cost); if (v !== null && !(v >= 0 && v < 1e8)) return 'Enter a valid cost.'; p.cost = v; }
  if (b.mrp !== undefined) { const v = money(b.mrp); if (v !== null && !(v >= 0 && v < 1e8)) return 'Enter a valid MRP.'; p.mrp = v; }
  if (b.description !== undefined) p.description = clip(b.description, 500);
  if (b.available !== undefined) p.available = !!b.available;
  if (b.sku !== undefined) p.sku = clip(b.sku, 40);
  if (b.barcode !== undefined) p.barcode = String(b.barcode).replace(/[^\w-]/g, '').slice(0, 40);
  if (b.unit !== undefined) p.unit = BIZ_UNITS.includes(b.unit) ? b.unit : 'pcs';
  if (b.category !== undefined) p.category = clip(b.category, 40);
  if (b.taxRate !== undefined) { const r = Number(b.taxRate); p.taxRate = BIZ_TAX.includes(r) ? r : 0; }
  if (b.lowAt !== undefined) p.lowAt = b.lowAt === '' || b.lowAt == null ? null : Math.max(0, Number(b.lowAt) || 0);
  if (b.trackStock !== undefined && !b.trackStock) p.stock = null;
  return null;
}
// ---- workspaces I can open ----
app.get('/api/biz/me', auth, (req, res) => {
  const list = [];
  if (req.user.business) list.push({ bizId: req.user.id, name: req.user.business.name, role: 'owner' });
  for (const u of db.users) if (u.business && u.id !== req.user.id) { const m = (u.business.members || []).find((x) => x.userId === req.user.id && x.status !== 'removed'); if (m) list.push({ bizId: u.id, name: u.business.name, role: m.role }); }
  res.json(list);
});
app.get('/api/biz/:bizId', auth, (req, res) => {
  const c = bizCtx(req, res); if (!c) return;
  res.json({ bizId: c.bizId, name: c.biz.name, category: c.biz.category, address: c.biz.address || '', phone: c.owner.phone, role: c.role, perms: c.perms, settings: c.biz.settings, units: BIZ_UNITS, taxRates: BIZ_TAX,
    staff: c.perms.includes('staff') ? (c.biz.members || []).filter((m) => m.status !== 'removed').map((m) => { const u = userById(m.userId); return { userId: m.userId, name: u ? u.name : 'Removed user', phone: u ? u.phone : '', role: m.role, addedAt: m.addedAt }; }) : [] });
});
// ---- dashboard ----
function bizStats(bizId, from, to) {
  const inv = db.invoices.filter((i) => i.bizId === bizId && i.createdAt >= from && i.createdAt < to);
  const ok = inv.filter((i) => i.status !== 'void');
  const sum = (a, f) => a.reduce((s, x) => s + f(x), 0);
  const byPay = {}; for (const i of ok) byPay[i.payment.method] = (byPay[i.payment.method] || 0) + i.grand;
  const items = new Map(); for (const i of ok) for (const l of i.items) { const k = l.productId || l.name; const x = items.get(k) || { name: l.name, qty: 0, amount: 0 }; x.qty += l.qty; x.amount += l.total; items.set(k, x); }
  const taxByRate = {}; for (const i of ok) for (const l of i.items) taxByRate[l.taxRate] = (taxByRate[l.taxRate] || 0) + l.tax;
  const hours = new Array(24).fill(0); for (const i of ok) hours[new Date(i.createdAt + 5.5 * 3600000).getUTCHours()] += i.grand;
  return { bills: ok.length, voids: inv.length - ok.length, sales: sum(ok, (i) => i.grand), tax: sum(ok, (i) => i.tax), discount: sum(ok, (i) => i.discount), unpaid: sum(ok.filter((i) => i.payment.status !== 'paid'), (i) => i.grand), byPay, taxByRate,
    top: [...items.values()].sort((a, b) => b.amount - a.amount).slice(0, 8), bestHour: hours.indexOf(Math.max(...hours)), avgBill: ok.length ? Math.round(sum(ok, (i) => i.grand) / ok.length) : 0 };
}
function bizReorder(bizId) { // 🤖 smart reorder: average sale per day over 14 days vs stock left
  const from = now() - 14 * 86400000, sold = new Map();
  for (const m of db.stockMoves) if (m.bizId === bizId && m.reason === 'sale' && m.at >= from) sold.set(m.productId, (sold.get(m.productId) || 0) - m.delta);
  const out = [];
  for (const p of bizProducts(bizId)) {
    if (p.stock == null) continue; const perDay = (sold.get(p.id) || 0) / 14;
    const daysLeft = perDay > 0 ? p.stock / perDay : Infinity, low = p.lowAt != null && p.stock <= p.lowAt;
    if (low || daysLeft < 5) out.push({ productId: p.id, name: p.name, stock: p.stock, unit: p.unit || 'pcs', perDay: Math.round(perDay * 10) / 10, daysLeft: isFinite(daysLeft) ? Math.max(0, Math.round(daysLeft * 10) / 10) : null, suggest: Math.max(1, Math.ceil(perDay * 14 - p.stock)) });
  }
  return out.sort((a, b) => (a.daysLeft ?? 99) - (b.daysLeft ?? 99)).slice(0, 20);
}
app.get('/api/biz/:bizId/dashboard', auth, (req, res) => {
  const c = bizCtx(req, res); if (!c) return;
  const day = istDayStart(), see = c.perms.includes('reports');
  const prods = bizProducts(c.bizId);
  res.json({
    today: see ? bizStats(c.bizId, day, day + 86400000) : null,
    month: see ? bizStats(c.bizId, Date.UTC(istNow().getUTCFullYear(), istNow().getUTCMonth(), 1) - 5.5 * 3600000, now() + 1) : null,
    products: prods.length, lowStock: prods.filter((p) => p.stock != null && p.lowAt != null && p.stock <= p.lowAt).map(bizProdOut).slice(0, 20),
    reorder: c.perms.includes('stock') || see ? bizReorder(c.bizId) : [],
    recent: db.invoices.filter((i) => i.bizId === c.bizId && (see || i.by === c.actor.id)).slice(-8).reverse().map(invOut),
    orders: c.perms.includes('bill') ? { new: db.orders.filter((o) => o.bizId === c.bizId && o.status === 'PLACED').length, active: db.orders.filter((o) => o.bizId === c.bizId && ORD_ACTIVE.includes(o.status)).length } : null,
  });
});
// ---- 💾 business backup (owner / manager saves a copy to Google Drive or the phone; photos are added by the app) ----
app.get('/api/biz/:bizId/backup', auth, (req, res) => {
  const c = bizCtx(req, res, 'reports'); if (!c) return;
  const b = c.biz;
  res.json({ kind: 'maata-business-backup', v: 1, exportedAt: now(), by: c.actor.name,
    business: { id: c.bizId, name: b.name, address: b.address || '', category: b.category || '', settings: { ...(b.settings || {}) } },
    products: bizProducts(c.bizId).sort((x, y) => x.name.localeCompare(y.name)).map(bizProdOut),
    invoices: db.invoices.filter((i) => i.bizId === c.bizId).map((i) => ({ ...invOut(i), shop: i.shop })) });
});

// 📱 Shop QR: a poster at the counter — customers scan it with any phone camera and order from the shop in Maata
const PUB = () => (process.env.PUBLIC_URL || 'https://maataapp.com').replace(/\/$/, '');
function shopQrOf(owner) {
  const b = owner.business, q = b.shopQr || (b.shopQr = {});
  if (!q.code) { let c; do { c = crypto.randomBytes(5).toString('base64').replace(/[^A-Za-z0-9]/g, '').slice(0, 6); } while (c.length < 6 || db.users.some((u) => u.business && u.business.shopQr && u.business.shopQr.code === c)); q.code = c; save(); }
  return q;
}
const shopByQr = (code) => db.users.find((u) => u.business && u.business.shopQr && u.business.shopQr.code === String(code || '') && u.status !== 'blocked');
const qrScan = (owner) => { const q = shopQrOf(owner), d = new Date().toISOString().slice(0, 10); q.scans = (q.scans || 0) + 1; if (q.day !== d) { q.day = d; q.today = 0; } q.today++; save(); };
app.get('/api/biz/:bizId/shop-qr', auth, async (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return; const q = shopQrOf(c.owner), spot = clip(req.query.spot, 30);
  const link = PUB() + '/s/' + q.code + (spot ? '?t=' + encodeURIComponent(spot) : '');
  const d = new Date().toISOString().slice(0, 10);
  res.json({ code: q.code, link, spot, svg: await QRCode.toString(link, { type: 'svg', margin: 1, errorCorrectionLevel: 'Q' }), storeOn: !!storeOf(c.biz).on,
    stats: { scans: q.scans || 0, today: q.day === d ? q.today || 0 : 0, orders: q.orders || 0, sales: q.sales || 0 }, shop: { name: c.biz.name, address: c.biz.address || '', category: c.biz.category || '', upi: !!bizSettings(c.biz).upi } });
});
app.get('/api/shop-code/:code', auth, (req, res) => { const o = shopByQr(req.params.code); if (!o) return res.status(404).json({ error: 'This shop QR is not valid any more.' }); qrScan(o); res.json({ bizId: o.id, name: o.business.name }); });
app.get('/s/:code', (req, res) => { // a phone camera opened the link
  const o = shopByQr(req.params.code); if (!o) return res.redirect(302, '/');
  qrScan(o); const t = clip(req.query.t, 30);
  res.redirect(302, '/?shop=' + encodeURIComponent(o.id) + '&via=qr' + (t ? '&spot=' + encodeURIComponent(t) : ''));
});
app.get('/s/:code/poster', async (req, res) => { // printable A4 poster for the counter
  const o = shopByQr(req.params.code); if (!o) return res.status(404).send('Not found');
  const t = clip(req.query.t, 30), link = PUB() + '/s/' + o.business.shopQr.code + (t ? '?t=' + encodeURIComponent(t) : ''), e = (x) => String(x || '').replace(/[&<>"]/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[ch]));
  const svg = await QRCode.toString(link, { type: 'svg', margin: 0, errorCorrectionLevel: 'Q', color: { dark: '#0B3B36', light: '#FFFFFF' } }), s = storeOf(o.business), upi = !!bizSettings(o.business).upi;
  res.type('html').send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${e(o.business.name)} · Scan to order</title>
<style>@page{size:A4;margin:0}*{box-sizing:border-box}body{margin:0;font-family:Inter,Arial,'Noto Sans Telugu',sans-serif;background:#eee}.p{width:210mm;height:297mm;margin:0 auto;background:linear-gradient(160deg,#0B5E44,#0F8A5F 45%,#1D4ED8);color:#fff;padding:16mm 14mm;display:flex;flex-direction:column;align-items:center;text-align:center;position:relative;overflow:hidden}
.k{background:#FFD166;color:#0B3B36;border-radius:30px;padding:3mm 8mm;font-weight:900;font-size:16pt;letter-spacing:.04em}h1{font-size:34pt;margin:7mm 0 1mm;line-height:1.1}.a{opacity:.9;font-size:12pt;margin:0}.q{background:#fff;border-radius:10mm;padding:9mm;margin:9mm 0 6mm;box-shadow:0 6mm 18mm rgba(0,0,0,.3)}.q svg{width:105mm;height:105mm;display:block}
.big{font-size:26pt;font-weight:900;margin:0}.te{font-size:17pt;font-weight:700;margin:2mm 0 6mm;color:#FFD166}.st{display:flex;gap:3mm;justify-content:center;width:100%}.st div{flex:1;background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.35);border-radius:6mm;padding:3mm 2mm;font-size:11.5pt;font-weight:700}.st b{display:block;font-size:18pt}
.f{position:absolute;left:0;right:0;bottom:0;background:#fff;color:#0B3B36;padding:5mm;font-weight:800;font-size:12pt;border-top:2.5mm solid;border-image:linear-gradient(90deg,#FF9933 0 33%,#fff 33% 66%,#138808 66%) 1}.f small{display:block;font-weight:600;color:#4C6274;font-size:9.5pt;margin-top:1mm}
.spot{background:#fff;color:#0B5E44;border-radius:4mm;padding:2mm 6mm;font-weight:900;font-size:18pt;margin-top:3mm}.pb{position:fixed;right:12px;top:12px;padding:12px 18px;border-radius:12px;border:0;background:#0F8A5F;color:#fff;font-weight:800;font-size:16px;box-shadow:0 4px 14px rgba(0,0,0,.25)}@media print{body{background:#fff}.pb{display:none}}</style></head><body>
<button class="pb" onclick="print()">🖨️ Print / Save PDF</button><div class="p"><span class="k">📱 SCAN &amp; ORDER</span><h1>${e(o.business.name)}</h1><p class="a">${e(o.business.address)}</p>${t ? `<div class="spot">${e(t)}</div>` : ''}
<div class="q">${svg}</div><p class="big">Scan with your phone camera</p><p class="te">మీ ఫోన్ కెమెరాతో స్కాన్ చేసి ఆర్డర్ చేయండి</p>
<div class="st"><div><b>1</b>Scan the QR</div><div><b>2</b>Choose items</div><div><b>3</b>${upi ? 'Pay by UPI' : 'Pay at counter'}</div><div><b>4</b>Collect at counter</div></div>
<div class="f">🦜 Order on Maata · maataapp.com<small>No queue · See prices & photos · ${upi ? 'UPI or cash' : 'Cash'} · Made in India 🇮🇳</small></div></div></body></html>`);
});
// ---- products & stock ----
app.get('/api/biz/:bizId/products', auth, (req, res) => { const c = bizCtx(req, res); if (!c) return; res.json(bizProducts(c.bizId).sort((a, b) => a.name.localeCompare(b.name)).map(bizProdOut)); });
// product photo (shown to customers in the online shop; not used in billing)
app.post('/api/biz/:bizId/products/:id/photo', auth, express.raw({ type: ['image/*'], limit: '3mb' }), async (req, res) => {
  const c = bizCtx(req, res, 'stock'); if (!c) return; const p = db.products.find((x) => x.id === req.params.id && x.ownerId === c.bizId); if (!p) return res.status(404).json({ error: 'Product not found.' });
  if (await savePhotoInto(p, req, res)) { bizLog(c, 'product-photo', p.id, p.name); res.json(bizProdOut(p)); }
});
app.delete('/api/biz/:bizId/products/:id/photo', auth, (req, res) => {
  const c = bizCtx(req, res, 'stock'); if (!c) return; const p = db.products.find((x) => x.id === req.params.id && x.ownerId === c.bizId); if (!p) return res.status(404).json({ error: 'Product not found.' });
  if (p.photo) deleteChatFile(p.photo); delete p.photo; delete p.photoMime; delete p.photoSize; p.photoV = now(); save(); res.json(bizProdOut(p));
});
app.post('/api/biz/:bizId/products', auth, (req, res) => {
  const c = bizCtx(req, res, 'stock'); if (!c) return;
  if (bizProducts(c.bizId).length >= 3000) return res.status(400).json({ error: 'You can have up to 3,000 products.' });
  const b = req.body || {}, p = { id: newId(), ownerId: c.bizId, available: true, createdAt: now(), stock: null };
  const err = readProductFields(b, p, true); if (err) return res.status(400).json({ error: err });
  if (p.barcode && bizProducts(c.bizId).some((x) => x.barcode === p.barcode)) return res.status(400).json({ error: 'Another product already has this barcode.' });
  db.products.push(p);
  if (b.stock !== undefined && b.stock !== '' && b.stock != null && b.trackStock !== false) { p.stock = 0; stockMove(c, p, Math.round(Number(b.stock) * 1000) / 1000 || 0, 'opening', ''); }
  bizLog(c, 'product-add', p.id, p.name); save(); res.json(bizProdOut(p));
});
app.put('/api/biz/:bizId/products/:pid', auth, (req, res) => {
  const c = bizCtx(req, res, 'stock'); if (!c) return;
  const p = db.products.find((x) => x.id === req.params.pid && x.ownerId === c.bizId); if (!p) return res.status(404).json({ error: 'Product not found.' });
  const b = req.body || {}, err = readProductFields(b, p, false); if (err) return res.status(400).json({ error: err });
  if (p.barcode && bizProducts(c.bizId).some((x) => x !== p && x.barcode === p.barcode)) return res.status(400).json({ error: 'Another product already has this barcode.' });
  if (b.trackStock && p.stock == null) { p.stock = 0; if (Number(b.stock)) stockMove(c, p, Number(b.stock), 'opening', ''); }
  bizLog(c, 'product-edit', p.id, p.name); save(); res.json(bizProdOut(p));
});
app.post('/api/biz/:bizId/products/:pid/stock', auth, (req, res) => {
  const c = bizCtx(req, res, 'stock'); if (!c) return;
  const p = db.products.find((x) => x.id === req.params.pid && x.ownerId === c.bizId); if (!p) return res.status(404).json({ error: 'Product not found.' });
  const delta = Math.round(Number(req.body?.delta) * 1000) / 1000, reason = ['purchase', 'damage', 'count', 'return', 'other'].includes(req.body?.reason) ? req.body.reason : 'other';
  if (!delta || !isFinite(delta) || Math.abs(delta) > 1e7) return res.status(400).json({ error: 'Enter how much stock to add or remove.' });
  if (p.stock == null) p.stock = 0;
  if (!c.biz.settings.allowNegative && p.stock + delta < 0) return res.status(400).json({ error: 'Stock cannot go below 0. In stock: ' + p.stock + '.' });
  stockMove(c, p, delta, reason, clip(req.body?.note, 80)); bizLog(c, 'stock', p.id, p.name + ' ' + (delta > 0 ? '+' : '') + delta + ' (' + reason + ')'); save(); res.json(bizProdOut(p));
});
app.get('/api/biz/:bizId/products/:pid/moves', auth, (req, res) => {
  const c = bizCtx(req, res, 'stock'); if (!c) return;
  res.json(db.stockMoves.filter((m) => m.bizId === c.bizId && m.productId === req.params.pid).slice(-60).reverse().map((m) => ({ ...m, actorName: (userById(m.actor) || {}).name || '' })));
});
// ---- billing ----
const invOut = (i) => ({ id: i.id, number: i.number, createdAt: i.createdAt, byName: i.byName, customer: i.customer, items: i.items, subtotal: i.subtotal, discount: i.discount, taxable: i.taxable, tax: i.tax, roundOff: i.roundOff, grand: i.grand, payment: i.payment, status: i.status, voidReason: i.voidReason || '', taxMode: i.taxMode, key: i.key, sentTo: i.sentTo || null, shop: i.shop || {} });
function priceBill(lines, discountPaise, mode) { // allocate the discount by value, then tax per line
  const base = lines.map((l) => Math.round(l.unitPaise * l.qty)), sumBase = base.reduce((a, b) => a + b, 0);
  const disc = Math.max(0, Math.min(sumBase, Math.round(discountPaise || 0)));
  let left = disc; const out = [];
  lines.forEach((l, i) => {
    const d = i === lines.length - 1 ? left : Math.round(disc * base[i] / (sumBase || 1)); left -= d;
    const net = base[i] - d, r = mode === 'none' ? 0 : l.taxRate || 0;
    let taxable, tax, total;
    if (mode === 'excl') { taxable = net; tax = Math.round(net * r / 100); total = net + tax; }
    else { total = net; taxable = r ? Math.round(net * 100 / (100 + r)) : net; tax = net - taxable; }
    out.push({ ...l, gross: base[i], disc: d, taxable, tax, total });
  });
  return { lines: out, subtotal: sumBase, discount: disc, taxable: out.reduce((s, x) => s + x.taxable, 0), tax: out.reduce((s, x) => s + x.tax, 0), total: out.reduce((s, x) => s + x.total, 0) };
}
app.post('/api/biz/:bizId/invoices', auth, (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const b = req.body || {}, s = c.biz.settings;
  const idem = clip(b.idem, 64);
  if (idem) { const dup = db.invoices.find((i) => i.bizId === c.bizId && i.idem === idem); if (dup) return res.json(invOut(dup)); } // double tap: same bill back
  const raw = Array.isArray(b.items) ? b.items.slice(0, 200) : [];
  if (!raw.length) return res.status(400).json({ error: 'Add at least one item to the bill.' });
  const canPrice = c.perms.includes('stock') || c.role === 'owner' || c.role === 'manager';
  const lines = [], need = new Map();
  for (const it of raw) {
    const qty = Math.round(Number(it.qty) * 1000) / 1000; if (!(qty > 0 && qty < 1e6)) return res.status(400).json({ error: 'Check the quantity of every item.' });
    let p = null; if (it.productId) { p = db.products.find((x) => x.id === it.productId && x.ownerId === c.bizId); if (!p) return res.status(400).json({ error: 'A product on this bill was removed. Refresh and try again.' }); }
    const listPrice = p ? Math.round((p.price || 0) * 100) : null;
    let unitPaise = listPrice; if (it.price != null && it.price !== '' && (canPrice || !p)) unitPaise = Math.round(Number(it.price) * 100);
    if (!(unitPaise >= 0 && unitPaise < 1e10)) return res.status(400).json({ error: (p ? p.name : 'An item') + ' has no price. Add a price first.' });
    const name = p ? p.name : clip(it.name, 80); if (!name) return res.status(400).json({ error: 'Every item needs a name.' });
    const taxRate = p ? p.taxRate || 0 : BIZ_TAX.includes(Number(it.taxRate)) ? Number(it.taxRate) : 0;
    lines.push({ productId: p ? p.id : null, name, unit: p ? p.unit || 'pcs' : clip(it.unit, 10) || 'pcs', qty, unitPaise, taxRate, hsn: p ? p.sku || '' : '' });
    if (p && p.stock != null) need.set(p.id, (need.get(p.id) || 0) + qty);
  }
  if (!s.allowNegative) { // never sell more than the stock we have
    const short = []; for (const [pid, q] of need) { const p = db.products.find((x) => x.id === pid); if (p.stock < q) short.push(p.name + ' (in stock ' + p.stock + ')'); }
    if (short.length) return res.status(400).json({ error: 'Not enough stock: ' + short.join(', ') + '. Add stock first, or turn on "Allow selling below 0" in settings.' });
  }
  const pr = priceBill(lines, Math.round(Number(b.discount || 0) * 100), s.taxMode);
  const grand = s.roundOff ? Math.round(pr.total / 100) * 100 : pr.total;
  const method = b.method === 'upi' ? 'upi' : 'cash';
  if (method === 'upi' && !s.upi) return res.status(400).json({ error: 'Add your shop UPI ID first (More → Settings), so Maata can make the QR code.' });
  c.biz.invSeq = c.biz.invSeq || {}; const fy = finYear(); c.biz.invSeq[fy] = (c.biz.invSeq[fy] || 0) + 1;
  const phone = cleanMobile(b.customer?.phone || '');
  const inv = {
    id: newId(), bizId: c.bizId, idem: idem || null, key: crypto.randomBytes(12).toString('hex'), number: (s.prefix || 'INV') + '/' + fy + '/' + String(c.biz.invSeq[fy]).padStart(4, '0'),
    createdAt: now(), by: c.actor.id, byName: c.actor.name, customer: { name: clip(b.customer?.name, 60), phone: phone || '' }, taxMode: s.taxMode,
    items: pr.lines.map((l) => ({ productId: l.productId, name: l.name, unit: l.unit, qty: l.qty, price: l.unitPaise, taxRate: l.taxRate, hsn: l.hsn, disc: l.disc, taxable: l.taxable, tax: l.tax, total: l.total })),
    subtotal: pr.subtotal, discount: pr.discount, taxable: pr.taxable, tax: pr.tax, roundOff: grand - pr.total, grand,
    payment: { method, status: method === 'upi' ? 'unpaid' : 'paid', paidAt: method === 'upi' ? null : now() }, status: 'final', history: [{ at: now(), by: c.actor.id, what: 'created' }], // UPI: paid when the shop sees the money
    shop: { name: c.biz.name, address: c.biz.address || '', phone: c.owner.phone, gstin: s.gstin || '', upi: s.upi || '', footer: s.footer || '' },
  };
  db.invoices.push(inv);
  for (const l of inv.items) if (l.productId) { const p = db.products.find((x) => x.id === l.productId); if (p) stockMove(c, p, -l.qty, 'sale', inv.id); }
  bizLog(c, 'invoice', inv.id, inv.number + ' ' + rupees(grand)); save();
  res.json(invOut(inv));
});
app.get('/api/biz/:bizId/invoices', auth, (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const q = String(req.query.q || '').toLowerCase(), from = Number(req.query.from) || 0, to = Number(req.query.to) || now() + 1, all = c.perms.includes('reports');
  const list = db.invoices.filter((i) => i.bizId === c.bizId && i.createdAt >= from && i.createdAt < to && (all || i.by === c.actor.id)
    && (!q || i.number.toLowerCase().includes(q) || (i.customer.name || '').toLowerCase().includes(q) || (i.customer.phone || '').includes(q)));
  res.json(list.slice(-300).reverse().map(invOut));
});
const myInvoice = (c, id) => db.invoices.find((i) => i.id === id && i.bizId === c.bizId && (c.perms.includes('reports') || i.by === c.actor.id));
app.get('/api/biz/:bizId/invoices/:id', auth, (req, res) => { const c = bizCtx(req, res, 'bill'); if (!c) return; const i = myInvoice(c, req.params.id); if (!i) return res.status(404).json({ error: 'Bill not found.' }); res.json({ ...invOut(i), shop: i.shop }); });
app.post('/api/biz/:bizId/invoices/:id/void', auth, (req, res) => {
  const c = bizCtx(req, res, 'void'); if (!c) return;
  const i = myInvoice(c, req.params.id); if (!i) return res.status(404).json({ error: 'Bill not found.' });
  if (i.status === 'void') return res.status(400).json({ error: 'This bill is already cancelled.' });
  const reason = clip(req.body?.reason, 160); if (!reason) return res.status(400).json({ error: 'Write why this bill is cancelled.' });
  i.status = 'void'; i.voidReason = reason; i.history.push({ at: now(), by: c.actor.id, what: 'void: ' + reason });
  for (const l of i.items) if (l.productId) { const p = db.products.find((x) => x.id === l.productId); if (p) stockMove(c, p, l.qty, 'void', i.id); }
  bizLog(c, 'invoice-void', i.id, i.number + ' – ' + reason); save(); res.json(invOut(i));
});
app.post('/api/biz/:bizId/invoices/:id/pay', auth, (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const i = myInvoice(c, req.params.id); if (!i || i.status === 'void') return res.status(404).json({ error: 'Bill not found.' });
  const method = req.body?.method === 'upi' ? 'upi' : 'cash';
  if (i.payment.status === 'paid') return res.json(invOut(i));
  i.payment = { method, status: 'paid', paidAt: now(), was: i.payment.method }; i.history.push({ at: now(), by: c.actor.id, what: 'paid by ' + method });
  bizLog(c, 'invoice-paid', i.id, i.number); save(); res.json(invOut(i));
});
// send the bill to the customer's Maata chat (from the shop owner's account, which customers know)
app.post('/api/biz/:bizId/invoices/:id/send', auth, (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const i = myInvoice(c, req.params.id); if (!i) return res.status(404).json({ error: 'Bill not found.' });
  const phone = cleanMobile(req.body?.phone || i.customer.phone); if (!phone) return res.status(400).json({ error: 'Enter the customer\'s mobile number.' });
  const to = findByMobile(phone); if (!to) return res.status(404).json({ error: 'This number is not on Maata yet. Share the bill link instead.' });
  const ok = canMessage(c.owner, to.id); if (ok.error) return res.status(400).json({ error: 'Could not send the bill to this person.' });
  const base = (process.env.PUBLIC_URL || 'https://maataapp.com').replace(/\/$/, '');
  const text = '🧾 Bill ' + i.number + ' from ' + i.shop.name + '\n' + i.items.slice(0, 15).map((l) => '• ' + l.name + ' × ' + l.qty + ' = ' + rupees(l.total)).join('\n') + (i.items.length > 15 ? '\n…' : '') + '\nTotal: ' + rupees(i.grand) + ' (' + (i.payment.status === 'paid' ? 'paid by ' + i.payment.method.toUpperCase() : 'to pay') + ')\nFull bill: ' + base + '/bill/' + i.id + '?k=' + i.key;
  deliverMessage(c.owner, ok.target, { type: 'text', text: clip(text, 3000) });
  i.sentTo = phone; if (!i.customer.phone) i.customer.phone = phone; i.history.push({ at: now(), by: c.actor.id, what: 'sent to ' + phone });
  save(); res.json(invOut(i));
});
// printable bill page (anyone with the secret link: the customer, a printer, a PDF)
// 📱 UPI QR with the exact amount (made from the shop's UPI ID; any UPI app can scan it)
const upiLink = (vpa, name, paise, note) => 'upi://pay?pa=' + encodeURIComponent(vpa) + '&pn=' + encodeURIComponent(String(name || 'Shop').slice(0, 40)) + (paise ? '&am=' + (paise / 100).toFixed(2) : '') + '&cu=INR' + (note ? '&tn=' + encodeURIComponent(String(note).slice(0, 50)) : '');
const upiSvg = (link) => QRCode.toString(link, { type: 'svg', margin: 1, errorCorrectionLevel: 'M', color: { dark: '#000000', light: '#FFFFFF' } });
app.get('/bill/:id/upi.svg', async (req, res) => {
  const i = db.invoices.find((x) => x.id === req.params.id);
  if (!i || String(req.query.k || '') !== i.key || !i.shop.upi || i.status === 'void') return res.status(404).end();
  res.setHeader('Cache-Control', 'private, no-store'); res.type('image/svg+xml').send(await upiSvg(upiLink(i.shop.upi, i.shop.name, i.grand, i.number)));
});
app.get('/api/biz/:bizId/upi-qr', auth, async (req, res) => { // test QR from Settings (₹1) so the shop can check its UPI ID
  const c = bizCtx(req, res); if (!c) return; const s = bizSettings(c.biz); if (!s.upi) return res.status(400).json({ error: 'Add your UPI ID first.' });
  const paise = Math.max(0, Math.min(1e9, Math.round(Number(req.query.amount) || 0))), link = upiLink(s.upi, c.biz.name, paise, req.query.note || '');
  res.json({ link, svg: await upiSvg(link), upi: s.upi });
});
app.get('/bill/:id', async (req, res) => {
  const i = db.invoices.find((x) => x.id === req.params.id);
  if (!i || String(req.query.k || '') !== i.key) return res.status(404).type('html').send('<p style="font-family:system-ui;padding:24px">Bill not found.</p>');
  const e = (s) => String(s ?? '').replace(/[&<>"']/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[ch]);
  const taxRows = {}; for (const l of i.items) if (l.tax) taxRows[l.taxRate] = (taxRows[l.taxRate] || 0) + l.tax;
  const upi = i.shop.upi && i.payment.status !== 'paid' && i.status !== 'void' ? upiLink(i.shop.upi, i.shop.name, i.grand, i.number) : '';
  let qr = ''; if (upi) { try { qr = await upiSvg(upi); } catch { qr = ''; } }
  const paper = [58, 80].includes(Number(req.query.paper)) ? Number(req.query.paper) : 0; // thermal receipt layout
  res.setHeader('Cache-Control', 'private, no-store');
  res.type('html').send(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${e(i.number)} · ${e(i.shop.name)}</title>
<style>body{margin:0;background:#EEF2F1;font-family:system-ui,-apple-system,'Segoe UI',sans-serif;color:#13201E}.bill{max-width:420px;margin:16px auto;background:#fff;padding:22px 20px;border-radius:14px}h1{margin:0;font-size:22px;text-align:center}.c{text-align:center;color:#55625F;font-size:13px;line-height:1.5}.row{display:flex;justify-content:space-between;gap:10px;font-size:14px;padding:3px 0}hr{border:0;border-top:1px dashed #B9C4C1;margin:10px 0}table{width:100%;border-collapse:collapse;font-size:13.5px}th{text-align:left;color:#55625F;font-weight:600;padding:4px 0;border-bottom:1px solid #DDE4E2}td{padding:5px 0;vertical-align:top}td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}.tot{font-size:20px;font-weight:800}.void{color:#B42318;font-weight:800;text-align:center;font-size:18px;border:2px solid #B42318;border-radius:10px;padding:6px;margin:8px 0}.pay{display:block;text-align:center;background:#0F7A6E;color:#fff;text-decoration:none;padding:13px;border-radius:12px;font-weight:700;margin-top:12px}.qr{text-align:center;margin:12px 0 4px;display:flex;flex-direction:column;align-items:center;gap:6px}.qr b{font-size:16px}.qr svg{width:200px;height:200px}.qr small{color:#55625F;font-size:12px;line-height:1.5}.btn{display:block;margin:12px auto 24px;padding:12px 20px;border:0;border-radius:12px;background:#13201E;color:#fff;font-size:15px}@media print{body{background:#fff}.bill{margin:0;border-radius:0}.btn,.pay{display:none}}${paper ? `@page{size:${paper}mm auto;margin:0}body{background:#fff}.bill{max-width:none;width:${paper - 6}mm;margin:0 auto;padding:3mm 0;border-radius:0;font-size:12px}h1{font-size:17px}.row,table{font-size:12px}.tot{font-size:16px}.qr svg{width:${paper === 58 ? 38 : 50}mm;height:${paper === 58 ? 38 : 50}mm}` : ''}</style></head><body><div class="bill">
<h1>${e(i.shop.name)}</h1><div class="c">${e(i.shop.address)}${i.shop.phone ? '<br>Phone: ' + e(i.shop.phone) : ''}${i.shop.gstin ? '<br>GSTIN: ' + e(i.shop.gstin) : ''}</div><hr>
${i.status === 'void' ? '<div class="void">CANCELLED – ' + e(i.voidReason) + '</div>' : ''}
<div class="row"><span>Bill no: <b>${e(i.number)}</b></span><span>${e(new Date(i.createdAt + 5.5 * 3600000).toISOString().slice(0, 16).replace('T', ' '))}</span></div>
${i.customer.name || i.customer.phone ? '<div class="row"><span>Customer: ' + e(i.customer.name) + ' ' + e(i.customer.phone) + '</span></div>' : ''}<hr>
<table><thead><tr><th>Item</th><th class="n">Qty</th><th class="n">Rate</th><th class="n">Amount</th></tr></thead><tbody>
${i.items.map((l) => `<tr><td>${e(l.name)}${l.taxRate ? '<br><small style="color:#55625F">GST ' + l.taxRate + '%</small>' : ''}</td><td class="n">${l.qty} ${e(l.unit)}</td><td class="n">${(l.price / 100).toFixed(2)}</td><td class="n">${(l.total / 100).toFixed(2)}</td></tr>`).join('')}
</tbody></table><hr>
<div class="row"><span>Subtotal</span><span>${rupees(i.subtotal)}</span></div>
${i.discount ? '<div class="row"><span>Discount</span><span>– ' + rupees(i.discount) + '</span></div>' : ''}
${Object.entries(taxRows).map(([r, t]) => '<div class="row"><span>GST ' + r + '% ' + (i.taxMode === 'incl' ? '(included)' : '') + '</span><span>' + rupees(t) + '</span></div>').join('')}
${i.roundOff ? '<div class="row"><span>Round off</span><span>' + (i.roundOff > 0 ? '+' : '–') + ' ' + rupees(Math.abs(i.roundOff)) + '</span></div>' : ''}
<div class="row tot"><span>Total</span><span>${rupees(i.grand)}</span></div>
<div class="row"><span>Payment</span><span>${e(i.payment.method.toUpperCase())} · ${i.payment.status === 'paid' ? 'Paid' : 'To pay'}</span></div>
${qr ? '<div class="qr"><b>Scan &amp; pay ' + rupees(i.grand) + '</b>' + qr + '<small>Any UPI app · GPay · PhonePe · Paytm · BHIM<br>UPI ID: ' + e(i.shop.upi) + '</small></div>' : ''}
${upi ? '<a class="pay" href="' + e(upi) + '">Pay ' + rupees(i.grand) + ' by UPI app</a>' : ''}<hr>
<div class="c">${e(i.shop.footer)}<br><small>Made with Maata Business</small></div></div>
<button class="btn" onclick="window.print()">Print / Save as PDF</button></body></html>`);
});
// ---- reports ----
app.get('/api/biz/:bizId/report', auth, (req, res) => {
  const c = bizCtx(req, res, 'reports'); if (!c) return;
  const from = Number(req.query.from) || istDayStart(now() - 6 * 86400000), to = Number(req.query.to) || now() + 1;
  const days = []; for (let t = istDayStart(from); t < to; t += 86400000) { const s = bizStats(c.bizId, t, t + 86400000); days.push({ day: istDayKey(t), sales: s.sales, bills: s.bills }); if (days.length > 92) break; }
  res.json({ from, to, ...bizStats(c.bizId, from, to), days });
});
// ---- staff ----
app.post('/api/biz/:bizId/staff', auth, async (req, res) => {
  const c = bizCtx(req, res, 'staff'); if (!c) return;
  const role = BIZ_ROLES[req.body?.role] ? req.body.role : 'cashier', phone = cleanMobile(req.body?.phone);
  const u = phone && findByMobile(phone); if (!u) return res.status(404).json({ error: 'This number is not on Maata. Ask them to join Maata first.' });
  if (u.id === c.bizId) return res.status(400).json({ error: 'You are the owner.' });
  c.biz.members = (c.biz.members || []).filter((m) => m.userId !== u.id);
  if (c.biz.members.filter((m) => m.status !== 'removed').length >= 30) return res.status(400).json({ error: 'Up to 30 staff.' });
  c.biz.members.push({ userId: u.id, role, status: 'active', addedAt: now(), addedBy: c.actor.id });
  bizLog(c, 'staff-add', u.id, u.name + ' as ' + role); save();
  pushTo(u, { type: 'msg', title: '🏪 ' + c.biz.name, body: 'You were added as ' + role + '. Open Maata → Business to start.', tag: 'biz-' + c.bizId, url: '/?biz=' + c.bizId }).catch(() => {});
  res.json({ ok: true });
});
app.delete('/api/biz/:bizId/staff/:uid', auth, (req, res) => {
  const c = bizCtx(req, res, 'staff'); if (!c) return;
  const m = (c.biz.members || []).find((x) => x.userId === req.params.uid); if (!m) return res.status(404).json({ error: 'Not found.' });
  m.status = 'removed'; m.removedAt = now(); bizLog(c, 'staff-remove', m.userId, ''); save(); res.json({ ok: true });
});
app.put('/api/biz/:bizId/settings', auth, (req, res) => {
  const c = bizCtx(req, res, 'settings'); if (!c) return;
  const b = req.body || {}, s = c.biz.settings;
  if (b.prefix !== undefined) s.prefix = String(b.prefix).toUpperCase().replace(/[^A-Z0-9-]/g, '').slice(0, 8) || 'INV';
  if (b.taxMode !== undefined) s.taxMode = ['incl', 'excl', 'none'].includes(b.taxMode) ? b.taxMode : 'incl';
  if (b.roundOff !== undefined) s.roundOff = !!b.roundOff;
  if (b.allowNegative !== undefined) s.allowNegative = !!b.allowNegative;
  if (b.gstin !== undefined) { const g = String(b.gstin).toUpperCase().replace(/\s/g, ''); if (g && !/^\d{2}[A-Z]{5}\d{4}[A-Z][1-9A-Z]Z[0-9A-Z]$/.test(g)) return res.status(400).json({ error: 'GSTIN should be 15 characters, like 37ABCDE1234F1Z5.' }); s.gstin = g; }
  if (b.upi !== undefined) { const v = String(b.upi).trim(); if (v && !/^[\w.-]{2,}@[\w.-]{2,}$/.test(v)) return res.status(400).json({ error: 'UPI ID looks like name@bank.' }); s.upi = v; }
  if (b.footer !== undefined) s.footer = clip(b.footer, 120);
  bizLog(c, 'settings', '', JSON.stringify(b).slice(0, 200)); save(); res.json(s);
});
app.get('/api/biz/:bizId/log', auth, (req, res) => { const c = bizCtx(req, res, 'reports'); if (!c) return; res.json(db.bizLog.filter((l) => l.bizId === c.bizId).slice(-100).reverse()); });
// ---- 🤖 AI helpers (always a draft; a person confirms) ----
app.post('/api/biz/:bizId/ai/product-photo', auth, express.raw({ type: ['image/*'], limit: '4mb' }), async (req, res) => {
  const c = bizCtx(req, res, 'stock'); if (!c) return;
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Take a photo of the product.' });
  if (!GEMINI_KEY) return res.status(503).json({ error: 'Maata AI is not switched on.' });
  const g = await gemini('You help an Indian shopkeeper add products. Look at the photo and suggest product details. Return ONLY JSON: {"name":"short product name with brand and size if visible","category":"one or two words","unit":"one of ' + BIZ_UNITS.join('|') + '","description":"one short sentence","mrp":number or null (only if an MRP is clearly printed),"barcode":"digits if a barcode number is clearly readable, else empty","sure":true|false}. Never guess prices.',
    [{ role: 'user', parts: [{ inlineData: { mimeType: String(req.headers['content-type'] || 'image/jpeg').split(';')[0], data: req.body.toString('base64') } }, { text: 'What is this product?' }] }], { json: true, maxTokens: 300, temperature: 0.2 });
  let d = null; try { d = JSON.parse(g.text || ''); } catch { /* not json */ }
  if (!d || !d.name) return res.status(502).json({ error: 'AI could not read this photo. Try a clearer photo, or type the name.' });
  res.json({ name: clip(d.name, 80), category: clip(d.category, 40), unit: BIZ_UNITS.includes(d.unit) ? d.unit : 'pcs', description: clip(d.description, 200), mrp: Number(d.mrp) > 0 ? Math.round(Number(d.mrp) * 100) / 100 : null, barcode: String(d.barcode || '').replace(/\D/g, '').slice(0, 20), sure: !!d.sure });
});
app.post('/api/biz/:bizId/ai/voice-bill', auth, async (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const said = clip(req.body?.text, 600); if (!said) return res.status(400).json({ error: 'Say the items.' });
  const prods = bizProducts(c.bizId).filter((p) => p.available !== false).slice(0, 400).map((p) => ({ id: p.id, n: p.name, u: p.unit || 'pcs' }));
  let items = [];
  if (GEMINI_KEY) {
    const g = await gemini('Turn what an Indian shopkeeper said (Telugu, Hindi, English or mixed) into bill items. Match each item to the closest product from the list by meaning (Telugu/Hindi names allowed). Numbers may be words (rendu=2, oka=1, moodu=3, ek=1, do=2, aadha/ara=0.5, pav=0.25). Return ONLY JSON: {"items":[{"id":"product id or empty if no match","said":"words used","qty":number}]}',
      [{ role: 'user', parts: [{ text: 'Products: ' + JSON.stringify(prods) + '\nSaid: ' + said }] }], { json: true, maxTokens: 600, temperature: 0.1 });
    try { items = (JSON.parse(g.text || '{}').items || []).slice(0, 40); } catch { items = []; }
  }
  if (!items.length) { // simple fallback: "2 rice 1 soap"
    const parts = said.toLowerCase().split(/[,;]| and | mariyu | aur /);
    for (const p of parts) { const m = /(\d+(?:\.\d+)?)?\s*(.+)/.exec(p.trim()); if (!m) continue; const name = m[2].trim(); const hit = prods.find((x) => x.n.toLowerCase().includes(name) || name.includes(x.n.toLowerCase())); items.push({ id: hit ? hit.id : '', said: name, qty: Number(m[1]) || 1 }); }
  }
  const ok = new Set(prods.map((p) => p.id));
  res.json({ items: items.map((x) => ({ productId: ok.has(x.id) ? x.id : null, said: clip(x.said, 60), qty: Math.max(0.001, Math.min(1e5, Number(x.qty) || 1)) })) });
});
app.post('/api/biz/:bizId/ai/insights', auth, async (req, res) => {
  const c = bizCtx(req, res, 'reports'); if (!c) return;
  const to = now() + 1, from = istDayStart(now() - 29 * 86400000), wk = istDayStart(now() - 6 * 86400000), pw = istDayStart(now() - 13 * 86400000);
  const m = bizStats(c.bizId, from, to), w = bizStats(c.bizId, wk, to), p = bizStats(c.bizId, pw, wk), re = bizReorder(c.bizId);
  const facts = { period: 'last 30 days (' + istDayKey(from) + ' to ' + istDayKey(now()) + ')', sales30: rupees(m.sales), bills30: m.bills, avgBill: rupees(m.avgBill), thisWeek: rupees(w.sales), lastWeek: rupees(p.sales), bestHour: m.bills ? m.bestHour + ':00' : null, top: m.top.slice(0, 5).map((t) => t.name + ' ' + rupees(t.amount)), payments: Object.fromEntries(Object.entries(m.byPay).map(([k, v]) => [k, rupees(v)])), unpaid: rupees(m.unpaid), reorder: re.slice(0, 5).map((r) => r.name + ' (stock ' + r.stock + ', ~' + (r.daysLeft ?? '?') + ' days left)') };
  let text = '';
  if (GEMINI_KEY && m.bills) { const lang = AI_LANG_NAME[req.user.lang] || 'English'; const g = await gemini('You are a friendly business adviser for a small Indian shop. Using ONLY these numbers, write 4 to 6 short, practical points in ' + lang + ' (plain text, one per line, start each with an emoji). Do not invent numbers.', [{ role: 'user', parts: [{ text: JSON.stringify(facts) }] }], { maxTokens: 500, temperature: 0.4 }); text = String(g.text || '').trim(); }
  if (!text) text = m.bills ? ['📈 Sales in the last 30 days: ' + facts.sales30 + ' from ' + m.bills + ' bills (average ' + facts.avgBill + ').', '🗓️ This week ' + facts.thisWeek + ', last week ' + facts.lastWeek + '.', facts.top.length ? '🏆 Top sellers: ' + facts.top.join(', ') + '.' : '', facts.bestHour ? '⏰ Busiest hour: ' + facts.bestHour + '.' : '', m.unpaid > 0 ? '💳 Money still to collect (credit bills): ' + facts.unpaid + '.' : '', re.length ? '📦 Order soon: ' + facts.reorder.join(', ') + '.' : ''].filter(Boolean).join('\n') : 'No bills yet. Make your first bill in Maata Business, and insights will appear here.';
  res.json({ text: clip(text, 2000), facts });
});


// ==================== 🛒 PHASE 3: ONLINE STORE + ORDERS ====================
// Customers order from a shop inside Maata. Prices are always worked out on the server.
// Flow: PLACED → ACCEPTED (stock reserved) → PREPARING → READY → OUT_FOR_DELIVERY → DELIVERED (bill made)
// Exceptions: REJECTED, CANCELLED (stock released), DELIVERY_FAILED. Delivery and pickup need the customer's 4-digit code.
// Out of town: mode "ship" → READY → SHIPPED (courier / lorry transport, with the booking reference) → DELIVERED (customer or shop confirms).
const ORD_ACTIVE = ['PLACED', 'ACCEPTED', 'PREPARING', 'READY', 'OUT_FOR_DELIVERY', 'DELIVERY_FAILED', 'SHIPPED'];
const ORD_NEXT = { // shop actions allowed from each state
  PLACED: ['ACCEPTED', 'REJECTED'], ACCEPTED: ['PREPARING', 'CANCELLED'], PREPARING: ['READY', 'CANCELLED'],
  READY: ['OUT_FOR_DELIVERY', 'DELIVERED', 'CANCELLED'], OUT_FOR_DELIVERY: ['DELIVERED', 'DELIVERY_FAILED'], DELIVERY_FAILED: ['OUT_FOR_DELIVERY', 'CANCELLED'], SHIPPED: ['DELIVERED'],
};
const ORD_SAY = { PLACED: 'Order placed', ACCEPTED: 'Shop accepted your order', REJECTED: 'Shop could not take your order', PREPARING: 'Your order is being packed', READY: 'Your order is ready', OUT_FOR_DELIVERY: 'Your order is on the way', DELIVERED: 'Delivered. Thank you!', DELIVERY_FAILED: 'Delivery could not be completed', CANCELLED: 'Order cancelled', SHIPPED: 'Your parcel is sent' };
function storeOf(b) {
  b.store = { on: false, delivery: true, pickup: true, fee: 0, freeAbove: 0, minOrder: 0, eta: '30–45 min', area: '', radiusKm: 5, lat: null, lng: null, payCod: true, payUpi: true, ship: false, shipFee: 0, shipToPay: false, shipEta: '3–7 days', shipNote: '', ...(b.store || {}) };
  return b.store;
}
const shopStaffIds = (owner) => [owner.id, ...((owner.business.members || []).filter((m) => m.status !== 'removed' && (BIZ_ROLES[m.role] || []).includes('bill')).map((m) => m.userId))];
function ordOut(o, forShop) {
  return { id: o.id, number: o.number, bizId: o.bizId, shopName: o.shopName, shopPhone: o.shopPhone, customerId: o.customerId, customerName: o.customerName, customerPhone: forShop ? o.customerPhone : undefined,
    items: o.items, subtotal: o.subtotal, discount: 0, tax: o.tax, fee: o.fee, grand: o.grand, mode: o.mode, address: o.address, note: o.note, pay: o.pay, paidBy: o.paidBy || null, status: o.status, history: o.history, createdAt: o.createdAt,
    otp: !forShop && o.mode !== 'ship' && ['READY', 'OUT_FOR_DELIVERY', 'DELIVERY_FAILED'].includes(o.status) ? o.otp : undefined, invoiceId: o.invoiceId || null, invoiceKey: o.invoiceKey || null, reason: o.reason || '', custPaidAt: o.custPaidAt || null, custRef: o.custRef || '', prepaid: !!o.prepaid, via: o.via || '', spot: o.spot || '', taxMode: o.taxMode, riderId: o.riderId || null, rider: riderPublic(o), live: o.status === 'OUT_FOR_DELIVERY' && !!o.track, ship: shipOut(o), shipPref: o.shipPref || '', toPay: !!o.toPay };
}
// 🚚 courier / lorry transport booking details (no photo bytes here; the receipt photo has its own link)
const SHIP_KIND = { courier: 'Courier', transport: 'Lorry transport', post: 'India Post', bus: 'Bus parcel', other: 'Other' };
function shipOut(o) { const x = o.ship; if (!x) return null; return { kind: x.kind, kindName: SHIP_KIND[x.kind] || 'Other', carrier: x.carrier, ref: x.ref, link: x.link || '', eta: x.eta || '', pieces: x.pieces || 0, note: x.note || '', toPay: !!x.toPay, hasPhoto: !!(o.shipPhoto && o.shipPhoto.data), photoV: (o.shipPhoto && o.shipPhoto.v) || 0, at: x.at, by: x.byName || '' }; }
const shipSay = (o) => o.ship ? ' by ' + o.ship.carrier + ' · ' + (o.ship.kind === 'transport' ? 'LR' : 'Ref') + ' no. ' + o.ship.ref : '';

// 🔊 spoken alerts for shops and delivery persons (open app speaks it; a closed Android app speaks it from the push)
const VA = {
  new_order: { en: (o) => o.via === 'qr' ? `Counter order from ${o.customerName}${o.spot ? ', ' + o.spot : ''}. ${o.items.length} item${o.items.length > 1 ? 's' : ''}, ${Math.round(o.grand / 100)} rupees. The customer is in the shop.` : `New order from ${o.customerName}. ${o.items.length} item${o.items.length > 1 ? 's' : ''}, ${Math.round(o.grand / 100)} rupees. ${o.mode === 'delivery' ? 'Home delivery' : o.mode === 'ship' ? 'Courier' : 'Pickup'}. Please accept the order.`,
               te: (o) => o.via === 'qr' ? `షాప్ లోపల నుండి ఆర్డర్ వచ్చింది. ${o.customerName}${o.spot ? ', ' + o.spot : ''}, ${o.items.length} ఐటమ్స్, ${Math.round(o.grand / 100)} రూపాయలు. కస్టమర్ షాప్ లోనే ఉన్నారు.` : `కొత్త ఆర్డర్ వచ్చింది. ${o.customerName} నుండి, ${o.items.length} ఐటమ్స్, ${Math.round(o.grand / 100)} రూపాయలు. దయచేసి ఆర్డర్ యాక్సెప్ట్ చేయండి.`,
               hi: (o) => o.via === 'qr' ? `दुकान के अंदर से ऑर्डर आया है. ${o.customerName}${o.spot ? ', ' + o.spot : ''}, ${o.items.length} आइटम, ${Math.round(o.grand / 100)} रुपये.` : `नया ऑर्डर आया है. ${o.customerName} से, ${o.items.length} आइटम, ${Math.round(o.grand / 100)} रुपये. कृपया ऑर्डर स्वीकार करें.` },
  order_wait: { en: (o) => `Reminder. The order from ${o.customerName} is still waiting. Please accept it.`,
                te: (o) => `గుర్తు చేస్తున్నాను. ${o.customerName} ఆర్డర్ ఇంకా వేచి ఉంది. దయచేసి యాక్సెప్ట్ చేయండి.`,
                hi: (o) => `याद दिला रहे हैं. ${o.customerName} का ऑर्डर अभी भी इंतज़ार में है. कृपया स्वीकार करें.` },
  cust_paid: { en: (o) => `${o.customerName} says they paid ${Math.round(o.grand / 100)} rupees by U P I. Please check your U P I app.`,
               te: (o) => `${o.customerName} ${Math.round(o.grand / 100)} రూపాయలు యూపీఐ లో కట్టారు. దయచేసి మీ యూపీఐ యాప్ చూడండి.`,
               hi: (o) => `${o.customerName} ने ${Math.round(o.grand / 100)} रुपये यूपीआई से भेजे हैं. कृपया अपना यूपीआई ऐप देखें.` },
  cancelled: { en: (o) => `Order from ${o.customerName} was cancelled by the customer.`,
               te: (o) => `${o.customerName} ఆర్డర్ క్యాన్సిల్ చేశారు.`,
               hi: (o) => `${o.customerName} ने ऑर्डर रद्द कर दिया.` },
  new_ride: { en: (o) => `New delivery for you. Order from ${o.shopName}${o.address && o.address.text ? ', to ' + o.address.text.split(',')[0] : ''}. ${o.status === 'READY' ? 'It is ready. Please pick it up.' : 'Please get ready to deliver.'}`,
              te: (o) => `మీకు కొత్త డెలివరీ వచ్చింది. ${o.shopName} నుండి ఆర్డర్. ${o.status === 'READY' ? 'ఆర్డర్ రెడీగా ఉంది, తీసుకుని డెలివరీ చేయండి.' : 'డెలివరీకి సిద్ధంగా ఉండండి.'}`,
              hi: (o) => `आपके लिए नई डिलीवरी है. ${o.shopName} से ऑर्डर. ${o.status === 'READY' ? 'ऑर्डर तैयार है, लेकर डिलीवर करें.' : 'डिलीवरी के लिए तैयार रहें.'}` },
  ride_ready: { en: (o) => `Order for ${o.customerName} is ready at ${o.shopName}. Please pick it up and deliver.`,
                te: (o) => `${o.customerName} ఆర్డర్ ${o.shopName} లో రెడీగా ఉంది. దయచేసి తీసుకుని డెలివరీ చేయండి.`,
                hi: (o) => `${o.customerName} का ऑर्डर ${o.shopName} पर तैयार है. कृपया लेकर डिलीवर करें.` },
  ride_wait: { en: (o) => `Reminder. The order for ${o.customerName} is ready and waiting for you. Please start the delivery.`,
               te: (o) => `గుర్తు చేస్తున్నాను. ${o.customerName} ఆర్డర్ రెడీగా ఉంది. దయచేసి డెలివరీ మొదలు పెట్టండి.`,
               hi: (o) => `याद दिला रहे हैं. ${o.customerName} का ऑर्डर तैयार है. कृपया डिलीवरी शुरू करें.` },
};
const VA_TITLE = { new_order: (o) => (o.via === 'qr' ? '🏪 Counter order ' : '🛒 New order ') + o.number + (o.spot ? ' · ' + o.spot : ''), order_wait: (o) => '⏰ Order ' + o.number + ' still waiting', cust_paid: (o) => '💰 ' + o.number + ' · UPI paid', cancelled: (o) => '❌ Order ' + o.number + ' cancelled',
  new_ride: (o) => '🛵 New delivery ' + o.number, ride_ready: (o) => '📦 ' + o.number + ' is ready', ride_wait: (o) => '⏰ ' + o.number + ' waiting for you' };
function voiceAlert(u, kind, o) {
  if (!u || !VA[kind]) return;
  const lang = VA[kind][u.lang] ? u.lang : 'en', say = VA[kind][lang](o), sayEn = VA[kind].en(o);
  const rider = kind.startsWith('ride') || kind === 'new_ride';
  const url = rider ? '/?ride=' + o.id : '/?biz=' + o.bizId;
  const body = rider ? o.shopName + ' → ' + (o.address ? o.address.text : o.customerName) : o.customerName + ' · ' + rupees(o.grand) + ' · ' + (o.mode === 'delivery' ? 'Delivery' : o.mode === 'ship' ? 'Courier / transport' : 'Pickup');
  const title = VA_TITLE[kind](o);
  io.to('user:' + u.id).emit('voice:alert', { kind, say: u.voiceAlerts === false ? '' : say, sayEn, lang, title, body, url, orderId: o.id, bizId: o.bizId });
  pushTo(u, { type: 'order_alert', title, body, tag: (rider ? 'rider-' : 'border-') + o.id, url, say: u.voiceAlerts === false ? '' : say, sayEn: u.voiceAlerts === false ? '' : sayEn, lang }, { urgency: 'high' }).catch(() => {});
}
app.put('/api/me/voice-alerts', auth, (req, res) => { req.user.voiceAlerts = !!req.body?.on; save(); res.json({ on: req.user.voiceAlerts }); });
// reminders: an order nobody accepted, a ready order nobody picked up
setInterval(() => {
  const t = now();
  for (const o of db.orders) {
    if (o.status === 'PLACED' && t - o.createdAt < 60 * 60000) { // after 2 min and again after 5 min
      const age = t - o.createdAt, stage = age > 5 * 60000 ? 2 : age > 2 * 60000 ? 1 : 0;
      if (stage > (o.remStage || 0)) { o.remStage = stage; const owner = userById(o.bizId); if (owner) for (const id of shopStaffIds(owner)) voiceAlert(userById(id), 'order_wait', o); }
    }
    if (o.status === 'READY' && o.riderId && !o.remRide && o.readyAt && t - o.readyAt > 5 * 60000 && t - o.readyAt < 60 * 60000) { o.remRide = true; voiceAlert(userById(o.riderId), 'ride_wait', o); }
  }
}, 30000);
function ordNotify(o, who) {
  const owner = userById(o.bizId), cust = userById(o.customerId);
  if (who !== 'customer' && cust) { io.to('user:' + cust.id).emit('order:update', ordOut(o, false)); pushTo(cust, { type: 'msg', title: '🛒 ' + o.shopName, body: ORD_SAY[o.status] + (o.status === 'SHIPPED' ? shipSay(o) : '') + (o.status === 'OUT_FOR_DELIVERY' ? '. Your delivery code: ' + o.otp : '') + (o.reason ? ' (' + o.reason + ')' : ''), tag: 'order-' + o.id, url: '/?order=' + o.id }).catch(() => {}); }
  if (owner) for (const id of shopStaffIds(owner)) { io.to('user:' + id).emit('biz:order', { bizId: o.bizId, order: ordOut(o, true) }); if (who === 'customer' && (o.status === 'PLACED' || o.status === 'CANCELLED')) voiceAlert(userById(id), o.status === 'PLACED' ? 'new_order' : 'cancelled', o); }
  // the delivery person hears it the moment the order is ready
  if (o.status === 'READY' && o.riderId && who !== 'rider') { o.readyAt = o.readyAt || now(); if (!o.readyTold) { o.readyTold = true; voiceAlert(userById(o.riderId), 'ride_ready', o); } }
}
function ordStock(o, owner, dir, actor) { // dir -1 reserve, +1 release
  const ctx = { bizId: owner.id, actor };
  for (const l of o.items) { const p = db.products.find((x) => x.id === l.productId && x.ownerId === owner.id); if (p && p.stock != null) stockMove(ctx, p, dir * l.qty, dir < 0 ? 'order' : 'order-cancel', o.id); }
  o.reserved = dir < 0;
}
function ordShortStock(o, owner) {
  if (bizSettings(owner.business).allowNegative) return [];
  const need = new Map(); for (const l of o.items) need.set(l.productId, (need.get(l.productId) || 0) + l.qty);
  const short = []; for (const [pid, q] of need) { const p = db.products.find((x) => x.id === pid && x.ownerId === owner.id); if (p && p.stock != null && p.stock < q) short.push(p.name + ' (' + p.stock + ' left)'); }
  return short;
}
function ordToInvoice(o, owner, actor) { // a delivered order becomes a normal bill (stock already moved when it was accepted)
  const b = owner.business, s = bizSettings(b); b.invSeq = b.invSeq || {}; const fy = finYear(); b.invSeq[fy] = (b.invSeq[fy] || 0) + 1;
  const items = o.items.map((l) => ({ productId: l.productId, name: l.name, unit: l.unit, qty: l.qty, price: l.price, taxRate: l.taxRate, hsn: '', disc: 0, taxable: l.taxable, tax: l.tax, total: l.total }));
  if (o.fee) items.push({ productId: null, name: o.mode === 'ship' ? 'Courier / transport charge' : 'Delivery charge', unit: 'trip', qty: 1, price: o.fee, taxRate: 0, hsn: '', disc: 0, taxable: o.fee, tax: 0, total: o.fee });
  const inv = { id: newId(), bizId: owner.id, idem: 'order-' + o.id, key: crypto.randomBytes(12).toString('hex'), number: (s.prefix || 'INV') + '/' + fy + '/' + String(b.invSeq[fy]).padStart(4, '0'), createdAt: now(), by: actor.id, byName: actor.name,
    customer: { name: o.customerName, phone: o.customerPhone }, taxMode: o.taxMode, items, subtotal: o.subtotal + o.fee, discount: 0, taxable: items.reduce((x, l) => x + l.taxable, 0), tax: o.tax, roundOff: 0, grand: o.grand,
    payment: { method: o.paidBy || (o.pay === 'upi' ? 'upi' : 'cash'), status: 'paid', paidAt: now() }, status: 'final', orderId: o.id, history: [{ at: now(), by: actor.id, what: 'from online order ' + o.number }],
    shop: { name: b.name, address: b.address || '', phone: owner.phone, gstin: s.gstin || '', upi: s.upi || '', footer: s.footer || '' } };
  db.invoices.push(inv); o.invoiceId = inv.id; o.invoiceKey = inv.key; return inv;
}
// ---- shop side: store settings ----
app.get('/api/biz/:bizId/store', auth, (req, res) => { const c = bizCtx(req, res); if (!c) return; res.json(storeOf(c.biz)); });
app.put('/api/biz/:bizId/store', auth, (req, res) => {
  const c = bizCtx(req, res, 'settings'); if (!c) return;
  const s = storeOf(c.biz), b = req.body || {}, money = (v) => Math.max(0, Math.min(1e6, Math.round(Number(v) * 100) || 0));
  for (const k of ['on', 'delivery', 'pickup', 'payCod', 'payUpi', 'ship', 'shipToPay']) if (b[k] !== undefined) s[k] = !!b[k];
  if (b.shipFee !== undefined) s.shipFee = money(b.shipFee); if (b.shipEta !== undefined) s.shipEta = clip(b.shipEta, 30); if (b.shipNote !== undefined) s.shipNote = clip(b.shipNote, 160);
  if (b.fee !== undefined) s.fee = money(b.fee); if (b.freeAbove !== undefined) s.freeAbove = money(b.freeAbove); if (b.minOrder !== undefined) s.minOrder = money(b.minOrder);
  if (b.eta !== undefined) s.eta = clip(b.eta, 30); if (b.area !== undefined) s.area = clip(b.area, 120);
  if (b.radiusKm !== undefined) s.radiusKm = Math.max(0.5, Math.min(50, Number(b.radiusKm) || 5));
  if (b.lat !== undefined && b.lng !== undefined) { const la = Number(b.lat), lo = Number(b.lng); if (Math.abs(la) <= 90 && Math.abs(lo) <= 180 && (la || lo)) { s.lat = +la.toFixed(6); s.lng = +lo.toFixed(6); } }
  if (!s.delivery && !s.pickup && !s.ship) s.pickup = true; if (!s.payCod && !s.payUpi) s.payCod = true;
  if (s.on && !bizProducts(c.bizId).some((p) => p.available !== false && p.price != null)) { s.on = false; bizLog(c, 'store', '', 'tried to open with no priced products'); save(); return res.status(400).json({ error: 'Add products with prices first, then open your online store.' }); }
  bizLog(c, 'store', '', s.on ? 'online store ON' : 'online store OFF'); save(); res.json(s);
});
// ---- shop side: orders ----
app.get('/api/biz/:bizId/orders', auth, (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const active = req.query.view !== 'done';
  const list = db.orders.filter((o) => o.bizId === c.bizId && (active ? ORD_ACTIVE.includes(o.status) : !ORD_ACTIVE.includes(o.status)));
  res.json(list.slice(active ? -200 : -100).reverse().map((o) => ordOut(o, true)));
});
app.post('/api/biz/:bizId/orders/:id/status', auth, (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const o = db.orders.find((x) => x.id === req.params.id && x.bizId === c.bizId); if (!o) return res.status(404).json({ error: 'Order not found.' });
  const to = String(req.body?.to || ''), reason = clip(req.body?.reason, 160);
  if (!(ORD_NEXT[o.status] || []).includes(to)) return res.status(400).json({ error: 'This order is "' + o.status.toLowerCase().replace(/_/g, ' ') + '". Refresh and try again.' });
  if (to === 'DELIVERED' && o.mode === 'delivery' && o.status !== 'OUT_FOR_DELIVERY') return res.status(400).json({ error: 'Send it out for delivery first.' });
  if (to === 'OUT_FOR_DELIVERY' && o.mode !== 'delivery') return res.status(400).json({ error: o.mode === 'ship' ? 'This order goes by courier / transport. Use "Sent by courier / transport".' : 'This is a pickup order.' });
  if (to === 'DELIVERED' && o.mode === 'ship' && o.status !== 'SHIPPED') return res.status(400).json({ error: 'Add the courier / transport details first.' });
  if (['REJECTED', 'CANCELLED', 'DELIVERY_FAILED'].includes(to) && !reason) return res.status(400).json({ error: 'Write a short reason for the customer.' });
  if (to === 'DELIVERED' && o.mode !== 'ship') { // proof: the code only the customer has (a parcel is confirmed by the shop or the customer instead)
    o.otpTries = (o.otpTries || 0) + 1; if (o.otpTries > 8) return res.status(429).json({ error: 'Too many wrong codes. Call the customer.' });
    if (String(req.body?.otp || '').trim() !== o.otp) { save(); return res.status(400).json({ error: 'Wrong code. Ask the customer for the 4-digit code in their Maata order.' }); }
  }
  if (to === 'ACCEPTED') { const short = ordShortStock(o, c.owner); if (short.length) return res.status(400).json({ error: 'Not enough stock: ' + short.join(', ') + '. Add stock or reject the order.' }); ordStock(o, c.owner, -1, c.actor); }
  if (['CANCELLED', 'REJECTED'].includes(to) && o.reserved) ordStock(o, c.owner, 1, c.actor);
  if (to === 'OUT_FOR_DELIVERY') { if (!o.riderId) o.riderId = c.actor.id; o.trail = []; }
  if (['DELIVERED', 'CANCELLED', 'DELIVERY_FAILED', 'REJECTED'].includes(to)) trackStop(o);
  o.status = to; o.reason = reason; o.history.push({ s: to, at: now(), by: c.actor.name, why: reason });
  if (to === 'DELIVERED' && o.mode !== 'ship' && !o.prepaid && ['cash', 'upi'].includes(req.body?.paidBy)) { const chose = o.pay === 'upi' ? 'upi' : 'cash'; o.paidBy = req.body.paidBy; o.history.push({ s: 'PAID', at: now(), by: c.actor.name, why: 'Paid by ' + o.paidBy.toUpperCase() + (o.paidBy !== chose ? ' (customer chose ' + (chose === 'upi' ? 'UPI' : 'cash') + ' while ordering)' : '') }); }
  if (to === 'DELIVERED') { if (!o.invoiceId) { if (!o.reserved) ordStock(o, c.owner, -1, c.actor); ordToInvoice(o, c.owner, c.actor); } o.deliveredAt = now(); }
  bizLog(c, 'order-' + to.toLowerCase(), o.id, o.number + (reason ? ' – ' + reason : '')); save(); ordNotify(o, 'shop');
  res.json(ordOut(o, true));
});
// 🚚 out of town: the shop books the parcel with a courier / lorry transport and adds the reference number
const SHIP_PHOTO_MAX = 400 * 1024;
app.post('/api/biz/:bizId/orders/:id/ship', auth, (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const o = db.orders.find((x) => x.id === req.params.id && x.bizId === c.bizId); if (!o) return res.status(404).json({ error: 'Order not found.' });
  if (o.mode !== 'ship') return res.status(400).json({ error: 'This order is not a courier / transport order.' });
  if (!['READY', 'SHIPPED'].includes(o.status)) return res.status(400).json({ error: o.status === 'DELIVERED' ? 'This order is already delivered.' : 'Mark the order Ready (packed) first.' });
  const b = req.body || {}, kind = SHIP_KIND[b.kind] ? b.kind : 'courier';
  const carrier = clip(b.carrier, 60), ref = clip(String(b.ref || '').trim(), 40);
  if (!carrier) return res.status(400).json({ error: 'Write the courier / transport company name.' });
  if (!ref || ref.length < 3) return res.status(400).json({ error: 'Write the ' + (kind === 'transport' ? 'LR (lorry receipt)' : 'tracking / docket') + ' number.' });
  const link = String(b.link || '').trim().slice(0, 300); if (link && !/^https:\/\/[^\s<>"']+$/i.test(link)) return res.status(400).json({ error: 'Tracking link must start with https://' });
  const prev = o.ship || {};
  const x = { kind, carrier, ref, link, eta: clip(b.eta, 40), pieces: Math.max(0, Math.min(999, Math.round(Number(b.pieces) || 0))), note: clip(b.note, 200), toPay: !!b.toPay, at: prev.at || now(), by: c.actor.id, byName: c.actor.name };
  if (b.photo === null) delete o.shipPhoto;
  const first = o.status === 'READY'; o.ship = x;
  if (first) {
    o.status = 'SHIPPED'; o.shippedAt = now(); o.reason = ''; o.history.push({ s: 'SHIPPED', at: now(), by: c.actor.name, why: carrier + ' · ' + ref });
    if (!o.invoiceId) { if (!o.reserved) ordStock(o, c.owner, -1, c.actor); ordToInvoice(o, c.owner, c.actor); } // the sale happens when the parcel leaves the shop
  } else o.history.push({ s: 'SHIP_EDIT', at: now(), by: c.actor.name, why: carrier + ' · ' + ref });
  bizLog(c, first ? 'order-shipped' : 'order-ship-edit', o.id, o.number + ' – ' + carrier + ' ' + ref); save();
  if (first) ordNotify(o, 'shop'); else { const cu = userById(o.customerId); if (cu) { io.to('user:' + cu.id).emit('order:update', ordOut(o, false)); pushTo(cu, { type: 'msg', title: '🚚 ' + o.shopName, body: 'Parcel details updated' + shipSay(o), tag: 'order-' + o.id, url: '/?order=' + o.id }).catch(() => {}); } for (const id of shopStaffIds(c.owner)) io.to('user:' + id).emit('biz:order', { bizId: o.bizId, order: ordOut(o, true) }); }
  res.json(ordOut(o, true));
});
// 🤖 read the courier / LR receipt photo and fill the form (the shopkeeper checks before saving)
app.post('/api/biz/:bizId/ai/ship-receipt', auth, express.raw({ type: ['image/*'], limit: '4mb' }), async (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Take a photo of the receipt.' });
  if (!GEMINI_KEY) return res.status(503).json({ error: 'Maata AI is not switched on. Type the details.' });
  const g = await gemini('You read Indian courier receipts, consignment notes and lorry receipts (LR / bilty, often printed in English with Telugu or Hindi). Return ONLY JSON: {"kind":"courier|transport|post|bus|other","carrier":"company name as printed","ref":"the main tracking / docket / AWB / LR / consignment number exactly as printed, no spaces","pieces":number or 0,"toPay":true if marked To Pay / freight payable at destination else false,"eta":"expected delivery date if printed else empty","sure":true|false}. Never invent numbers; leave empty if not readable.',
    [{ role: 'user', parts: [{ inlineData: { mimeType: String(req.headers['content-type'] || 'image/jpeg').split(';')[0], data: req.body.toString('base64') } }, { text: 'Read this receipt.' }] }], { json: true, maxTokens: 300, temperature: 0.1 });
  let d = null; try { d = JSON.parse(g.text || ''); } catch { /* not json */ }
  if (!d || (!d.ref && !d.carrier)) return res.status(502).json({ error: 'AI could not read this receipt. Try a clearer photo, or type the details.' });
  res.json({ kind: SHIP_KIND[d.kind] ? d.kind : 'courier', carrier: clip(d.carrier, 60), ref: String(d.ref || '').replace(/\s+/g, '').slice(0, 40), pieces: Math.max(0, Math.min(999, Math.round(Number(d.pieces) || 0))), toPay: !!d.toPay, eta: clip(d.eta, 40), sure: !!d.sure });
});
app.post('/api/biz/:bizId/orders/:id/ship-photo', auth, express.raw({ type: ['image/jpeg', 'image/png', 'image/webp'], limit: SHIP_PHOTO_MAX }), (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const o = db.orders.find((x) => x.id === req.params.id && x.bizId === c.bizId); if (!o) return res.status(404).json({ error: 'Order not found.' });
  if (o.mode !== 'ship' || !['READY', 'SHIPPED', 'DELIVERED'].includes(o.status)) return res.status(400).json({ error: 'Add the receipt photo after the parcel is booked.' });
  if (!Buffer.isBuffer(req.body) || req.body.length < 100) return res.status(400).json({ error: 'Take a photo of the receipt.' });
  const mime = String(req.headers['content-type'] || '').split(';')[0];
  o.shipPhoto = { data: 'data:' + mime + ';base64,' + req.body.toString('base64'), v: ((o.shipPhoto && o.shipPhoto.v) || 0) + 1 };
  bizLog(c, 'order-ship-photo', o.id, o.number); save();
  const cu = userById(o.customerId); if (cu) io.to('user:' + cu.id).emit('order:update', ordOut(o, false));
  res.json(ordOut(o, true));
});
app.get('/api/orders/:id/ship-photo', auth, (req, res) => {
  const o = db.orders.find((x) => x.id === req.params.id); if (!o || !o.shipPhoto || !o.shipPhoto.data) return res.status(404).end();
  const owner = userById(o.bizId), isShop = owner && (owner.id === req.user.id || (owner.business?.members || []).some((m) => m.userId === req.user.id && m.status !== 'removed'));
  if (o.customerId !== req.user.id && !isShop) return res.status(403).end();
  const m = /^data:(image\/\w+);base64,(.+)$/.exec(o.shipPhoto.data); if (!m) return res.status(404).end();
  res.set('Content-Type', m[1]); res.set('Cache-Control', 'private, max-age=300'); res.send(Buffer.from(m[2], 'base64'));
});
// ---- customer side ----
const shopCard = (u, me) => { const s = storeOf(u.business); const dist = me && s.lat != null ? Math.round(geoDist(me, [s.lng, s.lat]) / 100) / 10 : null;
  return { bizId: u.id, name: u.business.name, category: u.business.category, address: u.business.address || '', hours: u.business.hours || '', photo: !!u.photo, eta: s.eta, fee: s.fee, freeAbove: s.freeAbove, minOrder: s.minOrder, delivery: s.delivery, pickup: s.pickup, area: s.area, radiusKm: s.radiusKm, distKm: dist, payCod: s.payCod, payUpi: s.payUpi, upi: s.payUpi ? bizSettings(u.business).upi : '', ship: !!s.ship, shipFee: s.shipFee, shipToPay: !!s.shipToPay, shipEta: s.shipEta, shipNote: s.shipNote, taxMode: bizSettings(u.business).taxMode, products: bizProducts(u.id).filter((p) => p.available !== false && p.price != null).length }; };
app.get('/api/shops', auth, (req, res) => {
  const la = Number(req.query.lat), lo = Number(req.query.lng), me = isFinite(la) && isFinite(lo) && (la || lo) ? [lo, la] : null, q = String(req.query.q || '').toLowerCase();
  const list = db.users.filter((u) => u.business && u.status !== 'blocked' && storeOf(u.business).on && !blockedOf(u).includes(req.user.id) && (!q || u.business.name.toLowerCase().includes(q) || (u.business.category || '').toLowerCase().includes(q))).map((u) => shopCard(u, me));
  list.sort((a, b) => (a.distKm ?? 9999) - (b.distKm ?? 9999)); res.json(list.slice(0, 60));
});
app.get('/api/shop/:bizId', auth, (req, res) => {
  const u = userById(req.params.bizId); if (!u || !u.business || u.status === 'blocked' || blockedOf(u).includes(req.user.id)) return res.status(404).json({ error: 'Shop not found.' });
  const s = storeOf(u.business); if (!s.on) return res.status(404).json({ error: 'This shop is not taking online orders right now.' });
  const la = Number(req.query.lat), lo = Number(req.query.lng);
  res.json({ ...shopCard(u, isFinite(la) && isFinite(lo) && (la || lo) ? [lo, la] : null), items: bizProducts(u.id).filter((p) => p.available !== false && p.price != null).sort((a, b) => (a.category || 'zz').localeCompare(b.category || 'zz') || a.name.localeCompare(b.name))
    .map((p) => ({ id: p.id, name: p.name, price: p.price, unit: p.unit || 'pcs', category: p.category || '', barcode: p.barcode || '', description: p.description || '', taxRate: p.taxRate || 0, hasPhoto: !!p.photo, photoV: p.photoV || 0, inStock: p.stock == null || p.stock > 0, left: p.stock != null && p.stock <= 5 ? Math.max(0, p.stock) : null })) });
});
const ordHits = new Map();
app.post('/api/shop/:bizId/orders', auth, (req, res) => {
  const owner = userById(req.params.bizId); if (!owner || !owner.business || owner.status === 'blocked' || blockedOf(owner).includes(req.user.id)) return res.status(404).json({ error: 'Shop not found.' });
  const s = storeOf(owner.business), set = bizSettings(owner.business), b = req.body || {};
  if (!s.on) return res.status(400).json({ error: 'This shop is not taking online orders right now.' });
  const idem = clip(b.idem, 64); if (idem) { const dup = db.orders.find((o) => o.customerId === req.user.id && o.idem === idem); if (dup) return res.json(ordOut(dup, false)); }
  const hits = (ordHits.get(req.user.id) || []).filter((t) => now() - t < 3600000); if (hits.length >= 15) return res.status(429).json({ error: 'Too many orders in an hour. Please wait.' }); hits.push(now()); ordHits.set(req.user.id, hits);
  const mode = b.mode === 'pickup' ? 'pickup' : b.mode === 'ship' ? 'ship' : 'delivery';
  if (mode === 'delivery' && !s.delivery) return res.status(400).json({ error: s.ship ? 'This shop does not do home delivery. Choose 🚚 Courier / transport.' : 'This shop does pickup only.' }); if (mode === 'pickup' && !s.pickup && b.via !== 'qr') return res.status(400).json({ error: 'This shop does not do pickup.' });
  if (mode === 'ship' && !s.ship) return res.status(400).json({ error: 'This shop does not send by courier / transport.' });
  const pay = mode === 'ship' ? (s.payUpi ? 'upi' : 'cod') : b.pay === 'upi' && s.payUpi ? 'upi' : s.payCod ? 'cod' : 'upi';
  const raw = Array.isArray(b.items) ? b.items.slice(0, 100) : []; if (!raw.length) return res.status(400).json({ error: 'Your cart is empty.' });
  const lines = [];
  for (const it of raw) {
    const p = db.products.find((x) => x.id === it.productId && x.ownerId === owner.id && x.available !== false && x.price != null); if (!p) return res.status(400).json({ error: 'Some items are no longer available. Refresh the shop.' });
    const qty = Math.round(Number(it.qty) * 1000) / 1000; if (!(qty > 0 && qty <= 1000)) return res.status(400).json({ error: 'Check quantities.' });
    if (p.stock != null && !set.allowNegative && p.stock < qty) return res.status(400).json({ error: p.name + ': only ' + Math.max(0, p.stock) + ' left.' });
    lines.push({ productId: p.id, name: p.name, unit: p.unit || 'pcs', qty, unitPaise: Math.round(p.price * 100), taxRate: p.taxRate || 0 });
  }
  const pr = priceBill(lines, 0, set.taxMode);
  if (s.minOrder && pr.total < s.minOrder) return res.status(400).json({ error: 'Minimum order is ' + rupees(s.minOrder) + '.' });
  let address = null;
  if (mode === 'delivery') {
    const text = clip(b.address?.text, 300); if (text.length < 8) return res.status(400).json({ error: 'Write your full delivery address.' });
    const la = Number(b.address?.lat), lo = Number(b.address?.lng), hasLoc = Math.abs(la) <= 90 && Math.abs(lo) <= 180 && (la || lo);
    if (hasLoc && s.lat != null && geoDist([s.lng, s.lat], [lo, la]) > s.radiusKm * 1000) return res.status(400).json({ error: 'Sorry, this shop delivers only within ' + s.radiusKm + ' km.' + (s.ship ? ' Choose 🚚 Courier / transport to get it sent to your town.' : ''), far: !!s.ship });
    address = { text, landmark: clip(b.address?.landmark, 100), lat: hasLoc ? +la.toFixed(6) : null, lng: hasLoc ? +lo.toFixed(6) : null };
  }
  let shipPref = '';
  if (mode === 'ship') {
    const text = clip(b.address?.text, 300); if (text.length < 8) return res.status(400).json({ error: 'Write your full address.' });
    const pin = String(b.address?.pin || '').replace(/\D/g, ''); if (!/^[1-9]\d{5}$/.test(pin)) return res.status(400).json({ error: 'Write your 6-digit PIN code.' });
    const city = clip(b.address?.city, 60); if (!city) return res.status(400).json({ error: 'Write your town / city.' });
    address = { text, landmark: clip(b.address?.landmark, 100), pin, city, state: clip(b.address?.state, 40), lat: null, lng: null };
    shipPref = clip(b.shipPref, 100);
  }
  const fee = mode === 'delivery' && !(s.freeAbove && pr.total >= s.freeAbove) ? s.fee : mode === 'ship' && !s.shipToPay ? s.shipFee : 0;
  owner.business.ordSeq = (owner.business.ordSeq || 0) + 1;
  const o = { id: newId(), number: 'OD' + String(owner.business.ordSeq).padStart(5, '0'), bizId: owner.id, shopName: owner.business.name, shopPhone: owner.phone, customerId: req.user.id, customerName: req.user.name, customerPhone: req.user.phone, idem: idem || null,
    items: pr.lines.map((l) => ({ productId: l.productId, name: l.name, unit: l.unit, qty: l.qty, price: l.unitPaise, taxRate: l.taxRate, taxable: l.taxable, tax: l.tax, total: l.total })), subtotal: pr.subtotal, tax: pr.tax, fee, grand: pr.total + fee, taxMode: set.taxMode,
    mode, address, shipPref, toPay: mode === 'ship' && !!s.shipToPay, note: clip(b.note, 200), pay, via: b.via === 'qr' ? 'qr' : '', spot: b.via === 'qr' ? clip(b.spot, 30) : '', status: 'PLACED', otp: String(crypto.randomInt(1000, 10000)), createdAt: now(), history: [{ s: 'PLACED', at: now(), by: req.user.name }] };
  db.orders.push(o); if (o.via === 'qr') { const q = owner.business.shopQr || (owner.business.shopQr = {}); q.orders = (q.orders || 0) + 1; q.sales = (q.sales || 0) + o.grand; }
  save(); ordNotify(o, 'customer');
  res.json(ordOut(o, false));
});
app.get('/api/my/orders', auth, (req, res) => res.json(db.orders.filter((o) => o.customerId === req.user.id).slice(-60).reverse().map((o) => ordOut(o, false))));
app.get('/api/my/orders/:id', auth, (req, res) => { const o = db.orders.find((x) => x.id === req.params.id && x.customerId === req.user.id); if (!o) return res.status(404).json({ error: 'Order not found.' }); res.json(ordOut(o, false)); });
app.post('/api/my/orders/:id/cancel', auth, (req, res) => {
  const o = db.orders.find((x) => x.id === req.params.id && x.customerId === req.user.id); if (!o) return res.status(404).json({ error: 'Order not found.' });
  if (!['PLACED', 'ACCEPTED'].includes(o.status)) return res.status(400).json({ error: 'The shop is already packing this order. Chat with the shop to cancel.' });
  const owner = userById(o.bizId); if (o.reserved && owner) ordStock(o, owner, 1, req.user);
  o.status = 'CANCELLED'; o.reason = clip(req.body?.reason, 160) || 'Cancelled by customer'; o.history.push({ s: 'CANCELLED', at: now(), by: req.user.name, why: o.reason });
  trackStop(o); save(); ordNotify(o, 'customer'); res.json(ordOut(o, false));
});
// 📱 the customer pays the shop by UPI from their phone (QR / UPI app), then tells the shop "I have paid"; the shop or delivery person confirms the money came
const ordPayOpen = (o) => o.pay === 'upi' && !o.paidBy && !['CANCELLED', 'REJECTED', 'DELIVERED'].includes(o.status);
app.get('/api/my/orders/:id/upi', auth, async (req, res) => {
  const o = db.orders.find((x) => x.id === req.params.id && x.customerId === req.user.id); if (!o) return res.status(404).json({ error: 'Order not found.' });
  if (!ordPayOpen(o)) return res.status(400).json({ error: 'Nothing to pay by UPI for this order.' });
  const owner = userById(o.bizId); const s = owner && owner.business ? bizSettings(owner.business) : {};
  if (!s.upi) return res.status(400).json({ error: 'This shop has not added its UPI ID yet. Pay in cash, or chat with the shop.' });
  const link = upiLink(s.upi, owner.business.name, o.grand, o.number);
  res.json({ link, svg: await upiSvg(link), upi: s.upi, amount: o.grand, shop: owner.business.name, number: o.number });
});
app.post('/api/my/orders/:id/upi-paid', auth, (req, res) => {
  const o = db.orders.find((x) => x.id === req.params.id && x.customerId === req.user.id); if (!o) return res.status(404).json({ error: 'Order not found.' });
  if (!ordPayOpen(o)) return res.status(400).json({ error: 'This order does not need a UPI payment now.' });
  o.custPaidAt = now(); o.custRef = clip(String(req.body?.ref || '').replace(/[^\w-]/g, ''), 30);
  o.history.push({ s: 'CUST_PAID', at: now(), by: req.user.name, why: 'Customer says paid ' + (o.grand / 100).toFixed(2) + ' by UPI' + (o.custRef ? ' (ref ' + o.custRef + ')' : '') });
  save(); ordNotify(o, 'customer');
  const owner = userById(o.bizId); if (owner) for (const id of shopStaffIds(owner)) { voiceAlert(userById(id), 'cust_paid', o); }
  res.json(ordOut(o, false));
});
// the shop saw the UPI money in its UPI app / sound box: the order is paid (the delivery person collects nothing)
app.post('/api/biz/:bizId/orders/:id/upi-received', auth, (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return; const o = db.orders.find((x) => x.id === req.params.id && x.bizId === c.bizId); if (!o) return res.status(404).json({ error: 'Order not found.' });
  if (o.paidBy) return res.status(400).json({ error: 'This order is already paid.' });
  if (['CANCELLED', 'REJECTED'].includes(o.status)) return res.status(400).json({ error: 'This order was cancelled.' });
  o.paidBy = 'upi'; o.prepaid = true; o.paidAt = now(); o.history.push({ s: 'PAID', at: now(), by: c.actor.name, why: 'UPI payment received by the shop' + (o.custRef ? ' (ref ' + o.custRef + ')' : '') });
  bizLog(c, 'order-upi-paid', o.id, o.number); save(); ordNotify(o, 'shop');
  const cu = userById(o.customerId); if (cu) pushTo(cu, { type: 'msg', title: '✅ ' + o.shopName, body: 'Payment of ₹' + (o.grand / 100).toFixed(2) + ' received for ' + o.number + '. Thank you!', tag: 'ord-' + o.id });
  res.json(ordOut(o, true));
});
app.post('/api/my/orders/:id/received', auth, (req, res) => { // the customer got the parcel
  const o = db.orders.find((x) => x.id === req.params.id && x.customerId === req.user.id); if (!o) return res.status(404).json({ error: 'Order not found.' });
  if (o.mode !== 'ship' || o.status !== 'SHIPPED') return res.status(400).json({ error: 'This order is not on the way by courier.' });
  o.status = 'DELIVERED'; o.deliveredAt = now(); o.history.push({ s: 'DELIVERED', at: now(), by: req.user.name, why: 'Customer received the parcel' });
  const owner = userById(o.bizId);
  if (owner) for (const id of shopStaffIds(owner)) { io.to('user:' + id).emit('biz:order', { bizId: o.bizId, order: ordOut(o, true) }); const u = userById(id); if (u) pushTo(u, { type: 'msg', title: '✅ ' + o.number + ' received', body: o.customerName + ' got the parcel.', tag: 'border-' + o.id, url: '/?biz=' + o.bizId }).catch(() => {}); }
  save(); res.json(ordOut(o, false));
});
// 🤖 chat-to-order: "1 kg rice, 2 soaps" → items in this shop, for the customer to confirm
app.post('/api/shop/:bizId/ai/list', auth, async (req, res) => {
  const owner = userById(req.params.bizId); if (!owner || !owner.business || !storeOf(owner.business).on) return res.status(404).json({ error: 'Shop not found.' });
  const hits = (ordHits.get('ai' + req.user.id) || []).filter((t) => now() - t < 3600000); if (hits.length >= 30) return res.status(429).json({ error: 'Please wait a little.' }); hits.push(now()); ordHits.set('ai' + req.user.id, hits);
  const said = clip(req.body?.text, 800); if (!said) return res.status(400).json({ error: 'Write or say your list.' });
  const prods = bizProducts(owner.id).filter((p) => p.available !== false && p.price != null).slice(0, 400).map((p) => ({ id: p.id, n: p.name, u: p.unit || 'pcs' }));
  let items = [];
  if (GEMINI_KEY) { const g = await gemini('A customer wrote or said a shopping list (Telugu, Hindi, English or mixed). Match each item to the closest product in this shop by meaning. Numbers may be words (oka=1, rendu=2, moodu=3, ek=1, do=2, teen=3, ara/aadha=0.5). Return ONLY JSON: {"items":[{"id":"product id or empty","said":"the words","qty":number}]}', [{ role: 'user', parts: [{ text: 'Products: ' + JSON.stringify(prods) + '\nList: ' + said }] }], { json: true, maxTokens: 700, temperature: 0.1 }); try { items = (JSON.parse(g.text || '{}').items || []).slice(0, 40); } catch { items = []; } }
  if (!items.length) for (const part of said.toLowerCase().split(/[,;\n]| and | mariyu | aur /)) { const m = /(\d+(?:\.\d+)?)?\s*(.+)/.exec(part.trim()); if (!m) continue; const name = m[2].trim(); const hit = prods.find((x) => x.n.toLowerCase().includes(name) || name.includes(x.n.toLowerCase())); items.push({ id: hit ? hit.id : '', said: name, qty: Number(m[1]) || 1 }); }
  const ok = new Set(prods.map((p) => p.id));
  res.json({ items: items.map((x) => ({ productId: ok.has(x.id) ? x.id : null, said: clip(x.said, 60), qty: Math.max(0.001, Math.min(1000, Number(x.qty) || 1)) })) });
});


// ==================== 🛵 PHASE 4–5: DELIVERY PERSON + LIVE TRACKING ====================
// The shop assigns an order to a delivery person (a staff member with the "delivery" role, or the owner/manager
// themselves). Location is collected ONLY while that person has the order "on the way", is seen ONLY by that
// customer and the shop, and is deleted when the delivery ends.
const riderOk = (owner, uid) => uid === owner.id || (owner.business.members || []).some((m) => m.userId === uid && m.status !== 'removed' && ['delivery', 'manager'].includes(m.role));
const riderList = (owner) => [{ userId: owner.id, name: owner.name + ' (owner)' }, ...(owner.business.members || []).filter((m) => m.status !== 'removed' && ['delivery', 'manager'].includes(m.role)).map((m) => ({ userId: m.userId, name: (userById(m.userId) || {}).name || 'Staff', role: m.role }))];
function riderEta(o) {
  if (!o.track || !o.address || o.address.lat == null) return null;
  const d = geoDist([o.track.lng, o.track.lat], [o.address.lng, o.address.lat]) * 1.35; // roads are longer than a straight line
  const speed = o.track.speed > 2 ? Math.min(12, Math.max(4, o.track.speed)) : 5.5; // m/s, ~20 km/h in town
  return { meters: Math.round(d), minutes: Math.max(1, Math.round(d / speed / 60)) };
}
function trackOut(o) { return o.status === 'OUT_FOR_DELIVERY' && o.track ? { lat: o.track.lat, lng: o.track.lng, acc: o.track.acc, heading: o.track.heading, at: o.track.at, trail: (o.trail || []).slice(-40), eta: riderEta(o) } : null; }
function riderPublic(o) { const r = o.riderId && userById(o.riderId); return r ? { name: r.name, phone: o.status === 'OUT_FOR_DELIVERY' ? r.phone : '' } : null; }
function trackEmit(o) {
  const payload = { orderId: o.id, track: trackOut(o), rider: riderPublic(o) };
  io.to('user:' + o.customerId).emit('order:track', payload);
  const owner = userById(o.bizId); if (owner) for (const id of shopStaffIds(owner)) io.to('user:' + id).emit('order:track', payload);
}
function trackStop(o) { delete o.track; delete o.trail; } // privacy: nothing kept after the trip
// shop: who can deliver, and assign
app.get('/api/biz/:bizId/riders', auth, (req, res) => { const c = bizCtx(req, res, 'bill'); if (!c) return; res.json(riderList(c.owner).map((r) => ({ ...r, busy: db.orders.filter((o) => o.riderId === r.userId && o.status === 'OUT_FOR_DELIVERY').length }))); });
app.post('/api/biz/:bizId/orders/:id/assign', auth, (req, res) => {
  const c = bizCtx(req, res, 'bill'); if (!c) return;
  const o = db.orders.find((x) => x.id === req.params.id && x.bizId === c.bizId); if (!o) return res.status(404).json({ error: 'Order not found.' });
  if (o.mode !== 'delivery' || !['ACCEPTED', 'PREPARING', 'READY', 'DELIVERY_FAILED'].includes(o.status)) return res.status(400).json({ error: 'You can assign a delivery person to a delivery order before it leaves the shop.' });
  const rid = String(req.body?.riderId || ''); if (!riderOk(c.owner, rid)) return res.status(400).json({ error: 'Choose one of your delivery staff.' });
  o.riderId = rid; o.riderAt = now(); o.history.push({ s: 'ASSIGNED', at: now(), by: c.actor.name, why: (userById(rid) || {}).name || '' });
  bizLog(c, 'order-assign', o.id, o.number + ' → ' + ((userById(rid) || {}).name || '')); save();
  if (rid !== c.actor.id) { const r = userById(rid); if (r) { io.to('user:' + rid).emit('rider:job', { orderId: o.id }); o.readyTold = o.status === 'READY'; if (o.status === 'READY') o.readyAt = now(); voiceAlert(r, 'new_ride', o); } }
  ordNotify(o, 'shop'); res.json(ordOut(o, true));
});
// delivery person
const riderJob = (req, res) => { const o = db.orders.find((x) => x.id === req.params.id && x.riderId === req.user.id); if (!o) { res.status(404).json({ error: 'This delivery is not assigned to you.' }); return null; } const owner = userById(o.bizId); if (!owner || !owner.business || !riderOk(owner, req.user.id)) { res.status(403).json({ error: 'You are no longer delivery staff for this shop.' }); return null; } return { o, owner }; };
const riderOut = (o) => ({ ...ordOut(o, true), shopUpi: !!bizSettings((userById(o.bizId) || { business: {} }).business || {}).upi, customerPhone: o.customerPhone, otp: undefined, track: trackOut(o), shopAddress: (userById(o.bizId) || { business: {} }).business.address || '', shopLoc: (() => { const s = (userById(o.bizId) || {}).business?.store; return s && s.lat != null ? { lat: s.lat, lng: s.lng } : null; })() });
app.get('/api/rider/jobs', auth, (req, res) => res.json(db.orders.filter((o) => o.riderId === req.user.id && ['ACCEPTED', 'PREPARING', 'READY', 'OUT_FOR_DELIVERY', 'DELIVERY_FAILED'].includes(o.status)).map(riderOut)
  .concat(db.orders.filter((o) => o.riderId === req.user.id && o.status === 'DELIVERED' && now() - (o.deliveredAt || 0) < 86400000).map(riderOut))));
app.get('/api/rider/jobs/:id', auth, (req, res) => { const j = riderJob(req, res); if (j) res.json(riderOut(j.o)); });
app.get('/api/rider/jobs/:id/upi-qr', auth, async (req, res) => { // customer wants to pay by UPI at the door
  const j = riderJob(req, res); if (!j) return; const { o, owner } = j; const s = bizSettings(owner.business);
  if (!s.upi) return res.status(400).json({ error: 'The shop has not added its UPI ID yet. Take cash, or ask the shop to add UPI in Bill settings.' });
  const link = upiLink(s.upi, owner.business.name, o.grand, o.number); res.json({ svg: await upiSvg(link), upi: s.upi, amount: o.grand, shop: owner.business.name });
});
app.post('/api/rider/jobs/:id/start', auth, (req, res) => {
  const j = riderJob(req, res); if (!j) return; const { o } = j;
  if (!['READY', 'DELIVERY_FAILED', 'PREPARING', 'ACCEPTED'].includes(o.status)) return res.status(400).json({ error: 'This order is ' + o.status.toLowerCase().replace(/_/g, ' ') + '.' });
  if (o.status !== 'READY' && o.status !== 'DELIVERY_FAILED') return res.status(400).json({ error: 'Wait until the shop marks the order Ready.' });
  o.status = 'OUT_FOR_DELIVERY'; o.reason = ''; o.history.push({ s: 'OUT_FOR_DELIVERY', at: now(), by: req.user.name }); o.trail = [];
  save(); ordNotify(o, 'shop'); trackEmit(o); res.json(riderOut(o));
});
app.post('/api/rider/jobs/:id/decline', auth, (req, res) => {
  const j = riderJob(req, res); if (!j) return; const { o, owner } = j;
  if (o.status === 'OUT_FOR_DELIVERY') return res.status(400).json({ error: 'You already started. Use "Could not deliver" with a reason.' });
  o.history.push({ s: 'UNASSIGNED', at: now(), by: req.user.name, why: clip(req.body?.reason, 120) || 'Declined' }); o.riderId = null;
  save(); for (const id of shopStaffIds(owner)) { io.to('user:' + id).emit('biz:order', { bizId: o.bizId, order: ordOut(o, true) }); const u = userById(id); if (u) pushTo(u, { type: 'msg', title: '🛵 ' + req.user.name + ' declined ' + o.number, body: 'Assign another delivery person.', tag: 'border-' + o.id, url: '/?biz=' + o.bizId }).catch(() => {}); }
  res.json({ ok: true });
});
const locHits = new Map();
app.post('/api/rider/jobs/:id/loc', auth, (req, res) => {
  const j = riderJob(req, res); if (!j) return; const { o } = j;
  if (o.status !== 'OUT_FOR_DELIVERY') return res.json({ stop: true }); // the trip ended: the phone stops sharing
  const last = locHits.get(o.id) || 0; if (now() - last < 3000) return res.json({ ok: true }); locHits.set(o.id, now());
  const la = Number(req.body?.lat), lo = Number(req.body?.lng); if (!(Math.abs(la) <= 90 && Math.abs(lo) <= 180) || (!la && !lo)) return res.status(400).json({ error: 'Bad location.' });
  o.track = { lat: +la.toFixed(6), lng: +lo.toFixed(6), acc: Math.round(Number(req.body?.acc) || 0), speed: Math.max(0, Number(req.body?.speed) || 0), heading: Number.isFinite(Number(req.body?.heading)) ? Math.round(Number(req.body.heading)) : null, at: now() };
  const tl = o.trail || (o.trail = []); const p = tl[tl.length - 1];
  if (!p || geoDist([p[1], p[0]], [o.track.lng, o.track.lat]) > 25) { tl.push([o.track.lat, o.track.lng]); if (tl.length > 300) tl.splice(0, tl.length - 300); }
  if (!o.nearTold && o.address && o.address.lat != null && geoDist([o.track.lng, o.track.lat], [o.address.lng, o.address.lat]) < 400) { o.nearTold = true; const cu = userById(o.customerId); if (cu) pushTo(cu, { type: 'msg', title: '🛵 ' + o.shopName, body: 'Your delivery is almost there. Keep your code ready: ' + o.otp, tag: 'order-' + o.id, url: '/?order=' + o.id }).catch(() => {}); }
  save(); trackEmit(o); res.json({ ok: true, eta: riderEta(o) });
});
app.post('/api/rider/jobs/:id/deliver', auth, (req, res) => {
  const j = riderJob(req, res); if (!j) return; const { o, owner } = j;
  if (o.status !== 'OUT_FOR_DELIVERY') return res.status(400).json({ error: 'Start the delivery first.' });
  o.otpTries = (o.otpTries || 0) + 1; if (o.otpTries > 8) { save(); return res.status(429).json({ error: 'Too many wrong codes. Call the customer or the shop.' }); }
  if (String(req.body?.otp || '').trim() !== o.otp) { save(); return res.status(400).json({ error: 'Wrong code. Ask the customer for the 4-digit code in their Maata order.' }); }
  const chose = o.pay === 'upi' ? 'upi' : 'cash';
  const paidBy = o.prepaid ? o.paidBy : ['cash', 'upi'].includes(req.body?.paidBy) ? req.body.paidBy : chose; // how the money was really collected at the door (or already paid by UPI)
  o.paidBy = paidBy;
  o.status = 'DELIVERED'; o.history.push({ s: 'DELIVERED', at: now(), by: req.user.name, why: o.prepaid ? 'Already paid by UPI before delivery' : 'Paid by ' + paidBy.toUpperCase() + (paidBy !== chose ? ' (customer chose ' + (chose === 'upi' ? 'UPI' : 'cash') + ' while ordering)' : '') }); o.deliveredAt = now();
  if (!o.reserved) ordStock(o, owner, -1, req.user); ordToInvoice(o, owner, req.user); trackStop(o);
  db.bizLog.push({ id: newId(), bizId: owner.id, actor: req.user.id, actorName: req.user.name, action: 'order-delivered', ref: o.id, detail: o.number, at: now() });
  save(); ordNotify(o, 'shop'); trackEmit(o); res.json(riderOut(o));
});
app.post('/api/rider/jobs/:id/fail', auth, (req, res) => {
  const j = riderJob(req, res); if (!j) return; const { o } = j;
  if (o.status !== 'OUT_FOR_DELIVERY') return res.status(400).json({ error: 'This delivery is not on the way.' });
  const why = clip(req.body?.reason, 160); if (!why) return res.status(400).json({ error: 'Write why it could not be delivered.' });
  o.status = 'DELIVERY_FAILED'; o.reason = why; o.history.push({ s: 'DELIVERY_FAILED', at: now(), by: req.user.name, why }); trackStop(o);
  save(); ordNotify(o, 'shop'); trackEmit(o); res.json(riderOut(o));
});
// the customer and the shop can read the live position (only while it is on the way)
app.get('/api/orders/:id/track', auth, (req, res) => {
  const o = db.orders.find((x) => x.id === req.params.id); if (!o) return res.status(404).json({ error: 'Order not found.' });
  const owner = userById(o.bizId), isShop = owner && (owner.id === req.user.id || (owner.business?.members || []).some((m) => m.userId === req.user.id && m.status !== 'removed'));
  if (o.customerId !== req.user.id && !isShop) return res.status(403).json({ error: 'Not your order.' });
  res.json({ orderId: o.id, track: trackOut(o), rider: riderPublic(o), dest: o.address && o.address.lat != null ? { lat: o.address.lat, lng: o.address.lng } : null, shop: owner && owner.business?.store?.lat != null ? { lat: owner.business.store.lat, lng: owner.business.store.lng } : null });
});

// Ads: business owners create a promotion → admin approves → shown as "Sponsored" in everyone's Status tab
const adOut = (a, mine) => ({ id: a.id, ownerId: a.ownerId, title: a.title, text: a.text, productId: a.productId || null, hasPhoto: !!a.photo, photoV: a.photoV || 0, days: a.days, status: a.status, reason: a.reason || '', createdAt: a.createdAt, startsAt: a.startsAt || null, endsAt: a.endsAt || null, ...(mine ? { views: (a.viewers || []).length, clicks: a.clicks || 0 } : {}) });
const adLive = (a) => a.status === 'approved' && a.endsAt > now();
app.get('/api/ads/mine', auth, (req, res) => res.json(db.ads.filter((a) => a.ownerId === req.user.id).sort((a, b) => b.createdAt - a.createdAt).map((a) => adOut(a, true))));
app.post('/api/ads', auth, (req, res) => {
  if (!req.user.business) return res.status(400).json({ error: 'Create your business profile first.' });
  const open = db.ads.filter((a) => a.ownerId === req.user.id && (a.status === 'pending' || adLive(a)));
  if (open.length >= 2) return res.status(400).json({ error: 'You can have up to 2 ads waiting or running at a time.' });
  const title = clip(req.body?.title, 60), text = clip(req.body?.text, 300);
  if (!title || !text) return res.status(400).json({ error: 'Add a title and a message for your ad.' });
  const days = [1, 3, 7].includes(Number(req.body?.days)) ? Number(req.body.days) : 3;
  const pid = String(req.body?.productId || ''); const prod = pid && db.products.find((p) => p.id === pid && p.ownerId === req.user.id);
  const a = { id: newId(), ownerId: req.user.id, title, text, productId: prod ? prod.id : null, days, status: 'pending', createdAt: now(), viewers: [], clicks: 0 };
  db.ads.push(a); save();
  io.to('admins').emit('feed', { kind: 'ad', text: 'New ad to review from ' + req.user.business.name, ts: now() });
  res.json(adOut(a, true));
});
// Edit an ad. A live (or stopped / rejected / ended) ad goes back to "waiting for review" — so an approved ad
// cannot be changed into something else without the Maata team seeing it again.
app.put('/api/ads/:id', auth, (req, res) => {
  const a = db.ads.find((x) => x.id === req.params.id && x.ownerId === req.user.id); if (!a) return res.status(404).json({ error: 'Not found.' });
  const title = clip(req.body?.title, 60), text = clip(req.body?.text, 300);
  if (!title || !text) return res.status(400).json({ error: 'Add a title and a message for your ad.' });
  if (a.status !== 'pending' && !adLive(a)) { // re-running an old ad counts towards the limit of 2
    const open = db.ads.filter((x) => x.id !== a.id && x.ownerId === req.user.id && (x.status === 'pending' || adLive(x)));
    if (open.length >= 2) return res.status(400).json({ error: 'You can have up to 2 ads waiting or running at a time.' });
  }
  const pid = String(req.body?.productId || ''); const prod = pid && db.products.find((p) => p.id === pid && p.ownerId === req.user.id);
  const wasLive = adLive(a);
  Object.assign(a, { title, text, productId: prod ? prod.id : null, days: [1, 3, 7].includes(Number(req.body?.days)) ? Number(req.body.days) : a.days, status: 'pending', reason: '', startsAt: null, endsAt: null, editedAt: now() });
  if (req.body?.removePhoto && a.photo) { deleteChatFile(a.photo); a.photo = null; a.photoV = (a.photoV || 0) + 1; }
  save();
  io.to('admins').emit('feed', { kind: 'ad', text: 'Ad edited — please review again: ' + (req.user.business ? req.user.business.name : req.user.name), ts: now() });
  res.json({ ...adOut(a, true), wasLive });
});
app.post('/api/ads/:id/photo', auth, express.raw({ type: ['image/*'], limit: '3mb' }), async (req, res) => {
  const a = db.ads.find((x) => x.id === req.params.id && x.ownerId === req.user.id && x.status === 'pending'); if (!a) return res.status(404).json({ error: 'Not found.' });
  if (await savePhotoInto(a, req, res)) res.json(adOut(a, true));
});
app.delete('/api/ads/:id', auth, (req, res) => {
  const a = db.ads.find((x) => x.id === req.params.id && x.ownerId === req.user.id); if (!a) return res.status(404).json({ error: 'Not found.' });
  if (a.status === 'pending') { db.ads = db.ads.filter((x) => x.id !== a.id); if (a.photo) deleteChatFile(a.photo); } else { a.status = 'stopped'; a.endsAt = Math.min(a.endsAt || now(), now()); }
  save(); res.json({ ok: true });
});
app.get('/api/ads/photo/:id', (req, res) => streamPhoto(db.ads.find((x) => x.id === req.params.id), req, res));
app.get('/api/ads/feed', auth, (req, res) => {
  const live = db.ads.filter((a) => adLive(a) && !blockedOf(req.user).includes(a.ownerId)).filter((a) => { const o = userById(a.ownerId); return o && o.status !== 'blocked' && !blockedOf(o).includes(req.user.id); });
  live.sort(() => Math.random() - 0.5);
  let changed = false;
  const out = live.slice(0, 5).map((a) => {
    if (a.ownerId !== req.user.id && !(a.viewers || []).includes(req.user.id)) { (a.viewers = a.viewers || []).push(req.user.id); if (a.viewers.length > 5000) a.viewers = a.viewers.slice(-5000); changed = true; }
    const o = userById(a.ownerId);
    return { ...adOut(a, false), owner: contactView(req.user, o, { lastMessage: null, unread: 0 }), business: o.business ? { name: o.business.name, category: o.business.category } : null };
  });
  if (changed) save();
  res.json(out);
});
app.post('/api/ads/:id/click', auth, (req, res) => { const a = db.ads.find((x) => x.id === req.params.id); if (a && a.ownerId !== req.user.id) { a.clicks = (a.clicks || 0) + 1; save(); } res.json({ ok: true }); });

// ---------- Communities: many groups under one roof + an Announcements group ----------
const commById = (id) => db.communities.find((c) => c.id === id);
const commAdmin = (c, uid) => !!c && (c.ownerId === uid || (c.admins || []).includes(uid));
function commMembers(c) { const s = new Set([c.ownerId, ...(c.admins || [])]); for (const gid of c.groups) { const g = groupById(gid); if (g) g.members.forEach((m) => s.add(m.id)); } return s; }
const commMember = (c, uid) => commMembers(c).has(uid);
// Everyone in any group of the community is also in its Announcements group
function syncCommunity(c) {
  const ann = groupById(c.announceId); if (!ann) return;
  let added = 0;
  for (const uid of commMembers(c)) if (!gMember(ann, uid) && userById(uid)) { ann.members.push({ id: uid, role: commAdmin(c, uid) ? 'admin' : 'member', joinedAt: now() }); joinGroupRoom(uid, ann.id); added++; }
  if (added) { save(); groupChanged(ann); }
}
function commView(viewer, c) {
  const members = commMembers(c);
  return {
    id: c.id, name: c.name, description: c.description || '', ownerId: c.ownerId, isAdmin: commAdmin(c, viewer.id), memberCount: members.size, createdAt: c.createdAt,
    announce: c.announceId ? { id: 'g:' + c.announceId, joined: gMember(groupById(c.announceId), viewer.id) } : null,
    groups: c.groups.map((gid) => { const g = groupById(gid); return g ? { id: 'g:' + g.id, name: g.name, memberCount: g.members.length, joined: gMember(g, viewer.id), description: g.description || '' } : null; }).filter(Boolean),
  };
}
app.get('/api/communities', auth, (req, res) => res.json(db.communities.filter((c) => commMember(c, req.user.id)).map((c) => commView(req.user, c))));
app.post('/api/communities', auth, (req, res) => {
  const name = clip(req.body?.name, 60); if (!name) return res.status(400).json({ error: 'Give the community a name.' });
  if (db.communities.filter((c) => c.ownerId === req.user.id).length >= 10) return res.status(400).json({ error: 'You can create up to 10 communities.' });
  const gids = [...new Set((Array.isArray(req.body?.groups) ? req.body.groups : []).map((x) => String(x).replace(/^g:/, '')))]
    .filter((gid) => { const g = groupById(gid); return g && gAdmin(g, req.user.id) && !g.communityId; }).slice(0, 50);
  const c = { id: newId(), name, description: clip(req.body?.description, 500), ownerId: req.user.id, admins: [], groups: gids, createdAt: now() };
  const ann = { id: newId(), name: name + ' · Announcements', description: 'Announcements for everyone in ' + name, createdBy: req.user.id, createdAt: now(),
    members: [{ id: req.user.id, role: 'admin', joinedAt: now() }], settings: { onlyAdminsMessage: true, onlyAdminsEdit: true }, invite: inviteCode(), communityId: c.id, isAnnounce: true };
  db.groups.push(ann); c.announceId = ann.id;
  for (const gid of gids) groupById(gid).communityId = c.id;
  db.communities.push(c); save(); joinGroupRoom(req.user.id, ann.id);
  syncCommunity(c);
  sysMsg(ann, req.user, req.user.name + ' created the community "' + name + '"');
  for (const gid of gids) groupChanged(groupById(gid));
  res.json(commView(req.user, c));
});
function commFor(req, res, needAdmin) {
  const c = commById(req.params.id);
  if (!c || !commMember(c, req.user.id)) { res.status(404).json({ error: 'Community not found.' }); return null; }
  if (needAdmin && !commAdmin(c, req.user.id)) { res.status(403).json({ error: 'Only community admins can do this.' }); return null; }
  return c;
}
app.put('/api/communities/:id', auth, (req, res) => {
  const c = commFor(req, res, true); if (!c) return;
  if (req.body?.name !== undefined) { const n = clip(req.body.name, 60); if (n) c.name = n; }
  if (req.body?.description !== undefined) c.description = clip(req.body.description, 500);
  save(); res.json(commView(req.user, c));
});
app.post('/api/communities/:id/groups', auth, (req, res) => {
  const c = commFor(req, res, true); if (!c) return;
  if (c.groups.length >= 50) return res.status(400).json({ error: 'A community can have up to 50 groups.' });
  let g;
  if (req.body?.newName) {
    const n = clip(req.body.newName, 60); if (!n) return res.status(400).json({ error: 'Give the group a name.' });
    g = { id: newId(), name: n, description: '', createdBy: req.user.id, createdAt: now(), members: [{ id: req.user.id, role: 'admin', joinedAt: now() }], settings: { onlyAdminsMessage: false, onlyAdminsEdit: false }, invite: inviteCode() };
    db.groups.push(g); joinGroupRoom(req.user.id, g.id); sysMsg(g, req.user, req.user.name + ' created the group "' + n + '" in ' + c.name);
  } else {
    g = groupById(String(req.body?.groupId || '').replace(/^g:/, ''));
    if (!g || !gAdmin(g, req.user.id)) return res.status(400).json({ error: 'You must be an admin of that group.' });
    if (g.communityId) return res.status(400).json({ error: 'That group is already in a community.' });
  }
  g.communityId = c.id; c.groups.push(g.id); save(); syncCommunity(c); groupChanged(g);
  sysMsg(groupById(c.announceId), req.user, req.user.name + ' added the group "' + g.name + '"');
  res.json(commView(req.user, c));
});
app.delete('/api/communities/:id/groups/:gid', auth, (req, res) => {
  const c = commFor(req, res, true); if (!c) return;
  const gid = req.params.gid.replace(/^g:/, ''); c.groups = c.groups.filter((x) => x !== gid);
  const g = groupById(gid); if (g) { delete g.communityId; groupChanged(g); }
  save(); res.json(commView(req.user, c));
});
// Any community member can join any group inside it (no invite needed)
app.post('/api/communities/:id/groups/:gid/join', auth, (req, res) => {
  const c = commFor(req, res); if (!c) return;
  const g = groupById(req.params.gid.replace(/^g:/, ''));
  if (!g || g.communityId !== c.id) return res.status(404).json({ error: 'Group not found.' });
  if (!gMember(g, req.user.id)) {
    if (g.members.length >= GROUP_MAX) return res.status(400).json({ error: 'This group is full.' });
    g.members.push({ id: req.user.id, role: 'member', joinedAt: now(), via: 'community' }); joinGroupRoom(req.user.id, g.id);
    sysMsg(g, req.user, req.user.name + ' joined from the community'); save(); groupChanged(g);
  }
  res.json(groupView(req.user, g));
});
app.delete('/api/communities/:id', auth, (req, res) => {
  const c = commById(req.params.id);
  if (!c || c.ownerId !== req.user.id) return res.status(403).json({ error: 'Only the owner can deactivate the community.' });
  for (const gid of c.groups) { const g = groupById(gid); if (g) { delete g.communityId; groupChanged(g); } }
  const ann = groupById(c.announceId); if (ann) { ann.isAnnounce = false; delete ann.communityId; sysMsg(ann, req.user, 'The community "' + c.name + '" was deactivated'); }
  db.communities = db.communities.filter((x) => x.id !== c.id); save(); res.json({ ok: true });
});

// ---------- Channels: one-way updates to followers ----------
const chById = (id) => db.channels.find((c) => c.id === id && !c.removed);
const chAdmin = (c, uid) => !!c && (c.ownerId === uid || (c.admins || []).includes(uid));
function chView(viewer, c) {
  const posts = db.channelPosts.filter((p) => p.channelId === c.id);
  const seen = (viewer.chSeen || {})[c.id] || 0, last = posts[posts.length - 1];
  return { id: c.id, name: c.name, description: c.description || '', ownerId: c.ownerId, ownerName: userById(c.ownerId)?.name || '', isAdmin: chAdmin(c, viewer.id),
    following: (c.followers || []).includes(viewer.id), followers: (c.followers || []).length, photo: !!c.photo, photoV: c.photoV || 0, createdAt: c.createdAt,
    lastPost: last ? { text: last.text || (last.type === 'image' ? '📷 Photo' : last.type === 'video' ? '🎥 Video' : ''), ts: last.ts } : null,
    unread: posts.filter((p) => p.ts > seen && p.from !== viewer.id).length, muted: (viewer.chMuted || []).includes(c.id) };
}
function postView(viewer, p, admin) {
  const rs = Object.values(p.reactions || {}), counts = {};
  for (const e of rs) counts[e] = (counts[e] || 0) + 1;
  return { id: p.id, channelId: p.channelId, ts: p.ts, type: p.type, text: p.text || '', file: p.file ? { id: p.file.id, mime: p.file.mime, size: p.file.size, name: p.file.name } : null,
    reactions: counts, total: rs.length, mine: (p.reactions || {})[viewer.id] || null, editedAt: p.editedAt || null, ...(admin ? { views: (p.viewers || []).length } : {}) };
}
app.get('/api/channels', auth, (req, res) => {
  const q = clip(req.query.q, 60).toLowerCase();
  res.json(db.channels.filter((c) => !c.removed && (!q || c.name.toLowerCase().includes(q) || (c.description || '').toLowerCase().includes(q)))
    .sort((a, b) => (b.followers || []).length - (a.followers || []).length).slice(0, 50).map((c) => chView(req.user, c)));
});
app.get('/api/channels/mine', auth, (req, res) => res.json(db.channels.filter((c) => !c.removed && ((c.followers || []).includes(req.user.id) || chAdmin(c, req.user.id))).map((c) => chView(req.user, c)).sort((a, b) => (b.lastPost?.ts || b.createdAt) - (a.lastPost?.ts || a.createdAt))));
app.get('/api/channels/:id', auth, (req, res) => { const c = chById(req.params.id); if (!c) return res.status(404).json({ error: 'Channel not found.' }); res.json(chView(req.user, c)); });
app.post('/api/channels', auth, (req, res) => {
  if (db.channels.filter((c) => c.ownerId === req.user.id && !c.removed).length >= 5) return res.status(400).json({ error: 'You can create up to 5 channels.' });
  const name = clip(req.body?.name, 60); if (!name) return res.status(400).json({ error: 'Give your channel a name.' });
  const c = { id: newId(), name, description: clip(req.body?.description, 500), ownerId: req.user.id, admins: [], followers: [req.user.id], createdAt: now() };
  db.channels.push(c); save(); io.in('user:' + req.user.id).socketsJoin('ch:' + c.id);
  feed('channel', req.user.name + ' created channel "' + name + '"');
  res.json(chView(req.user, c));
});
app.put('/api/channels/:id', auth, (req, res) => {
  const c = chById(req.params.id); if (!chAdmin(c, req.user.id)) return res.status(403).json({ error: 'Only channel admins can do this.' });
  if (req.body?.name !== undefined) { const n = clip(req.body.name, 60); if (n) c.name = n; }
  if (req.body?.description !== undefined) c.description = clip(req.body.description, 500);
  save(); res.json(chView(req.user, c));
});
app.delete('/api/channels/:id', auth, (req, res) => {
  const c = chById(req.params.id); if (!c || c.ownerId !== req.user.id) return res.status(403).json({ error: 'Only the owner can delete the channel.' });
  c.removed = true; save(); io.to('ch:' + c.id).emit('channel:removed', { id: c.id }); res.json({ ok: true });
});
app.post('/api/channels/:id/photo', auth, express.raw({ type: ['image/*'], limit: '3mb' }), async (req, res) => {
  const c = chById(req.params.id); if (!chAdmin(c, req.user.id)) return res.status(403).json({ error: 'Only channel admins can do this.' });
  if (await savePhotoInto(c, req, res)) res.json(chView(req.user, c));
});
app.get('/api/channels/photo/:id', (req, res) => streamPhoto(chById(req.params.id), req, res));
app.post('/api/channels/:id/follow', auth, (req, res) => {
  const c = chById(req.params.id); if (!c) return res.status(404).json({ error: 'Channel not found.' });
  c.followers = c.followers || [];
  if (req.body?.follow === false) { if (c.ownerId === req.user.id) return res.status(400).json({ error: 'You own this channel.' }); c.followers = c.followers.filter((x) => x !== req.user.id); io.in('user:' + req.user.id).socketsLeave('ch:' + c.id); }
  else if (!c.followers.includes(req.user.id)) { c.followers.push(req.user.id); io.in('user:' + req.user.id).socketsJoin('ch:' + c.id); }
  save(); res.json(chView(req.user, c));
});
app.post('/api/channels/:id/mute', auth, (req, res) => {
  const c = chById(req.params.id); if (!c) return res.status(404).json({ error: 'Channel not found.' });
  const m = new Set(req.user.chMuted || []); req.body?.mute ? m.add(c.id) : m.delete(c.id); req.user.chMuted = [...m]; save(); res.json(chView(req.user, c));
});
app.get('/api/channels/:id/posts', auth, (req, res) => {
  const c = chById(req.params.id); if (!c) return res.status(404).json({ error: 'Channel not found.' });
  const admin = chAdmin(c, req.user.id), list = db.channelPosts.filter((p) => p.channelId === c.id).slice(-100);
  let changed = false;
  for (const p of list) if (!(p.viewers || []).includes(req.user.id)) { (p.viewers = p.viewers || []).push(req.user.id); if (p.viewers.length > 20000) p.viewers = p.viewers.slice(-20000); changed = true; }
  req.user.chSeen = { ...(req.user.chSeen || {}), [c.id]: now() };
  if (changed) save(); else save();
  res.json(list.map((p) => postView(req.user, p, admin)));
});
function chPost(req, c, fields) {
  const p = { id: newId(), channelId: c.id, from: req.user.id, ts: now(), reactions: {}, viewers: [req.user.id], ...fields };
  db.channelPosts.push(p); save();
  io.to('ch:' + c.id).emit('channel:post', { channelId: c.id, channelName: c.name, post: postView({ id: '' }, p, false) });
  return p;
}
app.post('/api/channels/:id/posts', auth, (req, res) => {
  const c = chById(req.params.id); if (!chAdmin(c, req.user.id)) return res.status(403).json({ error: 'Only channel admins can post.' });
  const text = clip(req.body?.text, 4000); if (!text) return res.status(400).json({ error: 'Write something first.' });
  res.json(postView(req.user, chPost(req, c, { type: 'text', text }), true));
});
app.post('/api/channels/:id/posts/media', auth, express.raw({ type: ['image/*', 'video/*'], limit: '16mb' }), async (req, res) => {
  const c = chById(req.params.id); if (!chAdmin(c, req.user.id)) return res.status(403).json({ error: 'Only channel admins can post.' });
  const mime = String(req.headers['content-type'] || '').split(';')[0], type = mime.startsWith('image/') ? 'image' : mime.startsWith('video/') ? 'video' : null;
  if (!type || !Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Choose a photo or video.' });
  const id = newId(); try { await saveChatFile(id, req.body, mime); } catch { return res.status(500).json({ error: 'Could not save the file.' }); }
  res.json(postView(req.user, chPost(req, c, { type, text: clip(req.query.caption, 1000), file: { id, mime, size: req.body.length, name: type + '.' + (mime.split('/')[1] || 'bin') } }), true));
});
app.get('/api/channels/file/:fileId', (req, res) => {
  let viewer = null; try { viewer = findUserByToken(String(req.query.t || '')); } catch { /* bad token */ }
  const p = db.channelPosts.find((x) => x.file && x.file.id === req.params.fileId);
  if (!viewer || !p || !chById(p.channelId)) return res.status(404).end();
  const size = p.file.size; let start = 0, end = size - 1, code = 200;
  const r = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
  if (r) { if (r[1]) { start = parseInt(r[1], 10); end = r[2] ? Math.min(parseInt(r[2], 10), size - 1) : size - 1; } else if (r[2]) start = Math.max(0, size - parseInt(r[2], 10)); if (start > end || start >= size) { res.setHeader('Content-Range', `bytes */${size}`); return res.status(416).end(); } code = 206; res.setHeader('Content-Range', `bytes ${start}-${end}/${size}`); }
  res.status(code); res.setHeader('Content-Type', p.file.mime); res.setHeader('Accept-Ranges', 'bytes'); res.setHeader('Content-Length', end - start + 1); res.setHeader('Cache-Control', 'private, max-age=604800');
  const st = chatFileStream(p.file.id, start, end); st.on('error', () => res.destroy()); st.pipe(res);
});
app.post('/api/channels/posts/:pid/react', auth, (req, res) => {
  const p = db.channelPosts.find((x) => x.id === req.params.pid); if (!p || !chById(p.channelId)) return res.status(404).json({ error: 'Not found.' });
  const e = clip(req.body?.emoji, 16); p.reactions = p.reactions || {};
  if (e && p.reactions[req.user.id] !== e) p.reactions[req.user.id] = e; else delete p.reactions[req.user.id];
  save(); const c = chById(p.channelId);
  io.to('ch:' + p.channelId).emit('channel:react', { postId: p.id, reactions: postView(req.user, p, false).reactions });
  res.json(postView(req.user, p, chAdmin(c, req.user.id)));
});
app.delete('/api/channels/posts/:pid', auth, (req, res) => {
  const p = db.channelPosts.find((x) => x.id === req.params.pid); const c = p && chById(p.channelId);
  if (!c || !chAdmin(c, req.user.id)) return res.status(403).json({ error: 'Not allowed.' });
  db.channelPosts = db.channelPosts.filter((x) => x.id !== p.id); if (p.file) deleteChatFile(p.file.id); save();
  io.to('ch:' + c.id).emit('channel:postDeleted', { postId: p.id }); res.json({ ok: true });
});
app.post('/api/channels/:id/report', auth, (req, res) => {
  const c = chById(req.params.id); if (!c) return res.status(404).json({ error: 'Channel not found.' });
  db.reports.push({ id: newId(), by: req.user.id, reported: c.ownerId, reason: 'Channel "' + c.name + '": ' + (clip(req.body?.reason, 300) || 'reported'), ts: now(), status: 'open' });
  save(); io.to('admins').emit('feed', { kind: 'report', text: 'Channel reported: ' + c.name, ts: now() }); res.json({ ok: true });
});

// ---------- Reels social: follow, profile, activity (likes / comments / new followers) ----------
const followersOf = (uid) => db.users.reduce((n, u) => n + ((u.following || []).includes(uid) ? 1 : 0), 0);
function addActivity(to, from, type, reelId, text) {
  if (!to || to === from.id) return;
  db.activity.push({ id: newId(), to, from: from.id, type, reelId: reelId || null, text: text ? String(text).slice(0, 120) : '', ts: now() });
  const mine = db.activity.filter((a) => a.to === to); if (mine.length > 200) { const cut = new Set(mine.slice(0, mine.length - 200).map((a) => a.id)); db.activity = db.activity.filter((a) => !cut.has(a.id)); }
  io.to('user:' + to).emit('activity:new', { type });
}
app.post('/api/follow/:id', auth, (req, res) => {
  const t = userById(req.params.id);
  if (!t || t.id === req.user.id || blockedOf(t).includes(req.user.id)) return res.status(404).json({ error: 'Not found.' });
  const f = new Set(req.user.following || []);
  const on = req.body?.follow !== false && !(req.body?.follow === undefined && f.has(t.id));
  if (on) { if (f.size >= 2000) return res.status(400).json({ error: 'You can follow up to 2000 people.' }); if (!f.has(t.id)) addActivity(t.id, req.user, 'follow'); f.add(t.id); } else f.delete(t.id);
  req.user.following = [...f]; save();
  res.json({ following: on, followers: followersOf(t.id) });
});
app.get('/api/profile/:id', auth, (req, res) => {
  const u = userById(req.params.id);
  if (!u || u.status === 'blocked' || blockedOf(u).includes(req.user.id)) return res.status(404).json({ error: 'Not found.' });
  const saved = contactsOf(req.user).find((c) => c.id === u.id);
  res.json({ id: u.id, name: (saved && saved.name) || u.name, about: u.about || '', photo: !!u.photo, photoV: u.photoV || 0, biz: u.business ? u.business.name : null,
    posts: db.reels.filter((r) => r.by === u.id && !r.removed).length, followers: followersOf(u.id), following: (u.following || []).length,
    isFollowing: (req.user.following || []).includes(u.id), isMe: u.id === req.user.id, inContacts: !!saved,
    likes: db.reels.filter((r) => r.by === u.id && !r.removed).reduce((n, r) => n + (r.likes || []).length, 0) });
});
app.get('/api/reels-creators', auth, (req, res) => {
  const q = clip(req.query.q, 40).toLowerCase(); if (!q) return res.json([]);
  const ids = [...new Set(db.reels.filter((r) => !r.removed).map((r) => r.by))];
  res.json(ids.map((id) => userById(id)).filter((u) => u && u.status !== 'blocked' && !blockedOf(u).includes(req.user.id) && u.name.toLowerCase().includes(q)).slice(0, 20)
    .map((u) => ({ id: u.id, name: u.name, photo: !!u.photo, photoV: u.photoV || 0, followers: followersOf(u.id) })));
});
app.get('/api/activity', auth, (req, res) => {
  const seen = req.user.activitySeen || 0;
  const list = db.activity.filter((a) => a.to === req.user.id).slice(-60).reverse().map((a) => {
    const u = userById(a.from) || {}, r = a.reelId && db.reels.find((x) => x.id === a.reelId);
    return { id: a.id, type: a.type, text: a.text, ts: a.ts, unseen: a.ts > seen, reelId: r ? r.id : null, reelThumb: !!(r && r.thumb), by: { id: a.from, name: u.name || 'Maata user', photo: !!u.photo, photoV: u.photoV || 0 }, iFollow: (req.user.following || []).includes(a.from) };
  });
  res.json(list);
});
app.get('/api/activity/count', auth, (req, res) => { const seen = req.user.activitySeen || 0; res.json({ unseen: db.activity.filter((a) => a.to === req.user.id && a.ts > seen).length }); });
app.post('/api/activity/seen', auth, (req, res) => { req.user.activitySeen = now(); save(); res.json({ ok: true }); });

// ---------- Reels (short videos, up to 60 s) ----------
// Videos go to Cloudflare R2 when it is set up (free downloads, lots of space), otherwise to the database.
const REEL_MAX = 16 * 1024 * 1024, REEL_DAY_LIMIT = 20;
const reelHits = new Map();
async function storeReelFile(buf, mime) {
  if (R2_ON) {
    const id = 'r2-' + newId();
    const r = await fetch(r2Url('PUT', id, 600), { method: 'PUT', body: buf, headers: { 'Content-Type': mime }, signal: AbortSignal.timeout(60000) });
    if (!r.ok) throw new Error('R2 upload failed ' + r.status);
    return id;
  }
  const id = newId(); await saveChatFile(id, buf, mime); return id;
}
// Recently watched reels are kept in memory (up to ~140 MB), so the next person gets them instantly
// instead of the server reading them from the database again.
const memFiles = new Map(); let memBytes = 0; const MEM_MAX = 140 * 1024 * 1024;
async function memFile(fileId, size) {
  if (memFiles.has(fileId)) { const b = memFiles.get(fileId); memFiles.delete(fileId); memFiles.set(fileId, b); return b; }
  if (size > 20 * 1024 * 1024) return null;
  const chunks = []; await new Promise((ok, bad) => { const st = chatFileStream(fileId, 0, size - 1); st.on('data', (c) => chunks.push(c)); st.on('end', ok); st.on('error', bad); });
  const buf = Buffer.concat(chunks); memFiles.set(fileId, buf); memBytes += buf.length;
  while (memBytes > MEM_MAX && memFiles.size > 1) { const [k, v] = memFiles.entries().next().value; memFiles.delete(k); memBytes -= v.length; }
  return buf;
}
function streamStored(fileId, mime, size, req, res) {
  if (isR2(fileId)) { res.setHeader('Cache-Control', 'private, max-age=3000'); return res.redirect(302, r2Url('GET', fileId, 3600, { 'response-content-type': mime, 'response-cache-control': 'private, max-age=2592000, immutable' })); }
  let start = 0, end = size - 1, code = 200;
  const r = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
  if (r) { if (r[1]) { start = parseInt(r[1], 10); end = r[2] ? Math.min(parseInt(r[2], 10), size - 1) : size - 1; } else if (r[2]) start = Math.max(0, size - parseInt(r[2], 10)); if (start > end || start >= size) { res.setHeader('Content-Range', `bytes */${size}`); return res.status(416).end(); } code = 206; res.setHeader('Content-Range', `bytes ${start}-${end}/${size}`); }
  res.status(code); res.setHeader('Content-Type', mime); res.setHeader('Accept-Ranges', 'bytes'); res.setHeader('Content-Length', end - start + 1);
  res.setHeader('Cache-Control', 'private, max-age=2592000, immutable'); res.setHeader('ETag', '"' + fileId + '"'); // a reel never changes: phones keep it
  if (req.headers['if-none-match'] === '"' + fileId + '"') return res.status(304).end();
  memFile(fileId, size).then((buf) => { if (buf) res.end(buf.subarray(start, end + 1)); else { const st = chatFileStream(fileId, start, end); st.on('error', () => res.destroy()); st.pipe(res); } })
    .catch(() => { const st = chatFileStream(fileId, start, end); st.on('error', () => res.destroy()); st.pipe(res); });
}
const reelById = (id) => db.reels.find((r) => r.id === id && !r.removed);
function canSeeReel(viewer, r) { const o = userById(r.by); return !!o && o.status !== 'blocked' && !blockedOf(o).includes(viewer.id) && !blockedOf(viewer).includes(o.id) && (!r.hidden || r.by === viewer.id); }
function reelView(viewer, r) {
  const o = userById(r.by) || {}, saved = contactsOf(viewer).find((c) => c.id === r.by);
  return { id: r.id, caption: r.caption, dur: r.dur || 0, createdAt: r.createdAt, likes: (r.likes || []).length, liked: (r.likes || []).includes(viewer.id), views: r.viewCount || 0,
    comments: db.reelComments.filter((c) => c.reelId === r.id).length, hasThumb: !!r.thumb, mine: r.by === viewer.id, hidden: !!r.hidden,
    shop: (r.products || []).length, place: r.loc ? r.loc.place : '', smart: !!(r.subs && r.subs.length), remix: !!r.remixOf,
    by: { id: r.by, name: (saved && saved.name) || o.name || 'Maata user', photo: !!o.photo, photoV: o.photoV || 0, biz: o.business ? o.business.name : null, followed: (viewer.following || []).includes(r.by) } };
}
app.post('/api/reels', auth, express.raw({ type: ['video/*'], limit: '17mb' }), async (req, res) => {
  const day = (reelHits.get(req.user.id) || []).filter((t) => now() - t < 86400000);
  if (day.length >= REEL_DAY_LIMIT) return res.status(429).json({ error: 'You can post up to ' + REEL_DAY_LIMIT + ' reels a day.' });
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Choose a video.' });
  if (req.body.length > REEL_MAX) return res.status(413).json({ error: 'The video is too big (16 MB max). Try a shorter video.' });
  const dur = Math.round(Number(req.query.dur) || 0);
  if (dur > 62) return res.status(400).json({ error: 'Reels can be up to 60 seconds.' });
  const mime = String(req.headers['content-type'] || 'video/mp4').split(';')[0];
  let fid; try { fid = await storeReelFile(req.body, mime); } catch (e) { console.error('[reels]', e.message); return res.status(500).json({ error: 'Could not save the video. Try again.' }); }
  const r = { id: newId(), by: req.user.id, caption: clip(req.query.caption, 300), file: { id: fid, mime, size: req.body.length }, dur, createdAt: now(), likes: [], viewers: [], viewCount: 0 };
  db.reels.push(r); day.push(now()); reelHits.set(req.user.id, day); save();
  feed('reel', req.user.name + ' posted a reel');
  res.json(reelView(req.user, r));
});
app.post('/api/reels/:id/thumb', auth, express.raw({ type: ['image/*'], limit: '1mb' }), async (req, res) => {
  const r = db.reels.find((x) => x.id === req.params.id && x.by === req.user.id); if (!r) return res.status(404).json({ error: 'Not found.' });
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'No picture.' });
  const id = newId(); await saveChatFile(id, req.body, 'image/jpeg'); r.thumb = { id, size: req.body.length }; save(); res.json({ ok: true });
  aiModerate(r, req.body).catch(() => {});
});
const viewerFromQuery = (req) => { try { return findUserByToken(String(req.query.t || '')); } catch { return null; } };
app.get('/api/reels/file/:id', (req, res) => {
  const v = viewerFromQuery(req), r = reelById(req.params.id) || (v && db.reels.find((x) => x.id === req.params.id && x.by === v.id));
  if (!v || !r || !canSeeReel(v, r)) return res.status(404).end();
  streamStored(r.file.id, r.file.mime, r.file.size, req, res);
});
app.get('/api/reels/thumb/:id', (req, res) => {
  const v = viewerFromQuery(req), r = db.reels.find((x) => x.id === req.params.id);
  if (!v || !r || !r.thumb || r.removed) return res.status(404).end();
  streamStored(r.thumb.id, 'image/jpeg', r.thumb.size, req, res);
});
app.get('/api/reels', auth, (req, res) => {
  const tab = String(req.query.tab || 'foryou'), me2 = req.user, contacts = new Set(contactsOf(me2).map((c) => c.id));
  let list = db.reels.filter((r) => !r.removed && canSeeReel(me2, r));
  const follows = new Set(me2.following || []);
  if (tab === 'nearby') { const me3 = { lat: +req.query.lat, lng: +req.query.lng }; if (!isFinite(me3.lat)) return res.json([]); list = list.filter((r) => r.loc && !r.hidden && geoKm(r.loc, me3) <= 25).sort((a, b) => geoKm(a.loc, me3) - geoKm(b.loc, me3)); }
  else if (tab === 'home') { const mineFeed = list.filter((r) => follows.has(r.by) || contacts.has(r.by) || r.by === me2.id); list = mineFeed.length >= 3 ? mineFeed : list.filter((r) => !r.hidden); }
  else if (tab === 'following') list = list.filter((r) => follows.has(r.by));
  else if (tab === 'friends') list = list.filter((r) => contacts.has(r.by));
  else if (tab === 'mine') list = list.filter((r) => r.by === me2.id);
  else if (tab.startsWith('user:')) list = list.filter((r) => r.by === tab.slice(5));
  else list = list.filter((r) => !r.hidden);
  if (tab === 'explore') list.sort((a, b) => ((b.likes || []).length * 3 + (b.viewCount || 0)) - ((a.likes || []).length * 3 + (a.viewCount || 0)));
  else if (tab === 'home') list.sort((a, b) => b.createdAt - a.createdAt);
  else if (tab === 'foryou') { // simple ranking: fresh + liked + watched, friends a little higher, unseen first
    const score = (r) => { const ageH = (now() - r.createdAt) / 3600000; return ((r.likes || []).length * 3 + (r.viewCount || 0) * 0.3 + (contacts.has(r.by) ? 8 : 0) + 5) / Math.pow(ageH + 2, 1.2) - ((r.viewers || []).includes(me2.id) ? 1000 : 0); };
    list.sort((a, b) => score(b) - score(a));
  } else if (tab !== 'nearby') list.sort((a, b) => b.createdAt - a.createdAt);
  const first = String(req.query.first || '');
  if (first) { const f = db.reels.find((r) => r.id === first && !r.removed && canSeeReel(me2, r)); if (f) list = [f, ...list.filter((r) => r.id !== first)]; }
  const skip = Math.max(0, Number(req.query.skip) || 0);
  res.json(list.slice(skip, skip + 15).map((r) => reelView(me2, r)));
});
app.post('/api/reels/:id/view', auth, (req, res) => {
  const r = reelById(req.params.id); if (!r) return res.status(404).json({});
  if (!(r.viewers || []).includes(req.user.id)) { (r.viewers = r.viewers || []).push(req.user.id); if (r.viewers.length > 20000) r.viewers = r.viewers.slice(-20000); r.viewCount = (r.viewCount || 0) + 1; save(); }
  res.json({ views: r.viewCount });
});
app.post('/api/reels/:id/like', auth, (req, res) => {
  const r = reelById(req.params.id); if (!r || !canSeeReel(req.user, r)) return res.status(404).json({ error: 'Reel not found.' });
  r.likes = r.likes || []; const i = r.likes.indexOf(req.user.id);
  if (i >= 0) r.likes.splice(i, 1); else { r.likes.push(req.user.id); addActivity(r.by, req.user, 'like', r.id); }
  save(); res.json({ likes: r.likes.length, liked: i < 0 });
});
app.get('/api/reels/:id/comments', auth, (req, res) => {
  const r = reelById(req.params.id); if (!r || !canSeeReel(req.user, r)) return res.status(404).json({ error: 'Reel not found.' });
  res.json(db.reelComments.filter((c) => c.reelId === r.id).slice(-200).map((c) => { const u = userById(c.by) || {}; return { id: c.id, text: c.text, ts: c.ts, by: { id: c.by, name: u.name || 'Maata user', photo: !!u.photo, photoV: u.photoV || 0 }, mine: c.by === req.user.id, canDelete: c.by === req.user.id || r.by === req.user.id }; }));
});
const commentHits = new Map();
app.post('/api/reels/:id/comments', auth, (req, res) => {
  const r = reelById(req.params.id); if (!r || !canSeeReel(req.user, r)) return res.status(404).json({ error: 'Reel not found.' });
  const text = clip(req.body?.text, 500); if (!text) return res.status(400).json({ error: 'Write a comment.' });
  const hits = (commentHits.get(req.user.id) || []).filter((t) => now() - t < 60000); if (hits.length >= 20) return res.status(429).json({ error: 'Slow down a little.' });
  hits.push(now()); commentHits.set(req.user.id, hits);
  const c = { id: newId(), reelId: r.id, by: req.user.id, text, ts: now() }; db.reelComments.push(c); addActivity(r.by, req.user, 'comment', r.id, text); save();
  res.json({ id: c.id, text, ts: c.ts, by: { id: req.user.id, name: req.user.name, photo: !!req.user.photo, photoV: req.user.photoV || 0 }, mine: true, canDelete: true });
});
app.delete('/api/reels/comments/:cid', auth, (req, res) => {
  const c = db.reelComments.find((x) => x.id === req.params.cid); const r = c && db.reels.find((x) => x.id === c.reelId);
  if (!c || !(c.by === req.user.id || (r && r.by === req.user.id))) return res.status(403).json({ error: 'Not allowed.' });
  db.reelComments = db.reelComments.filter((x) => x.id !== c.id); save(); res.json({ ok: true });
});
app.delete('/api/reels/:id', auth, (req, res) => {
  const r = db.reels.find((x) => x.id === req.params.id && x.by === req.user.id); if (!r) return res.status(404).json({ error: 'Not found.' });
  deleteChatFile(r.file.id); if (r.thumb) deleteChatFile(r.thumb.id);
  db.reels = db.reels.filter((x) => x.id !== r.id); db.reelComments = db.reelComments.filter((c) => c.reelId !== r.id); save(); res.json({ ok: true });
});
app.post('/api/reels/:id/report', auth, (req, res) => {
  const r = reelById(req.params.id); if (!r) return res.status(404).json({ error: 'Reel not found.' });
  r.reports = r.reports || []; if (!r.reports.some((x) => x.by === req.user.id)) r.reports.push({ by: req.user.id, reason: clip(req.body?.reason, 200), ts: now() });
  if (r.reports.length >= 3 && !r.reviewed) r.hidden = true; // hidden from For You until an admin checks it
  db.reports.push({ id: newId(), by: req.user.id, reported: r.by, reason: 'Reel: ' + (clip(req.body?.reason, 200) || 'reported') + (r.caption ? ' — "' + r.caption.slice(0, 60) + '"' : ''), ts: now(), status: 'open' });
  save(); io.to('admins').emit('feed', { kind: 'report', text: 'Reel reported', ts: now() }); res.json({ ok: true });
});

// ---------- Admin API ----------
function adminAuth(req, res, next) {
  const h = req.headers.authorization || '';
  try {
    const p = jwt.verify(h.startsWith('Bearer ') ? h.slice(7) : String(req.query.token || ''), JWT_SECRET);
    if (p.typ !== 'admin') throw new Error('not admin');
    req.admin = p.admin; next();
  } catch { res.status(401).json({ error: 'Admin session expired. Log in again.' }); }
}
function audit(admin, action, target, detail = '') {
  db.audit.push({ id: newId(), ts: now(), admin, action, target, detail }); save();
}

app.post('/admin/api/login', rateLimit, async (req, res) => {
  if (!ADMIN_HASH) return res.status(503).json({ error: 'Admin password is not set. Add ADMIN_PASSWORD in your server environment settings.' });
  const username = String(req.body?.username || '');
  const ok = username === ADMIN_USERNAME && (await bcrypt.compare(String(req.body?.password || ''), ADMIN_HASH));
  if (!ok) return res.status(401).json({ error: 'Wrong username or password.' });
  audit(username, 'login', '-');
  res.json({ token: jwt.sign({ typ: 'admin', admin: username }, JWT_SECRET, { expiresIn: '12h' }), admin: username });
});

app.get('/admin/api/stats', adminAuth, (req, res) => {
  const days = Math.min(Math.max(Number(req.query.days) || 14, 7), 90);
  const t0 = startOfToday();
  const keys = [];
  for (let i = days - 1; i >= 0; i--) keys.push(dayKey(t0 - i * 86400000 + 3600000));
  const idx = Object.fromEntries(keys.map((k, i) => [k, i]));
  const zero = () => keys.map(() => 0);
  const s = { logins: zero(), signups: zero(), voice: zero(), video: zero(), messages: zero(), callMinutes: zero() };
  const loginSets = keys.map(() => new Set());
  const hours = Array(24).fill(0);
  const weekAgo = t0 - 6 * 86400000;
  const today = { loginEvents: 0, signups: 0, voiceCalls: 0, videoCalls: 0, messages: 0, answered: 0, attempted: 0, talkSec: 0 };
  const uniqueToday = new Set();
  const activity = {}; // userId -> {messages, calls} last 7 days

  for (const l of db.logins) {
    const i = idx[dayKey(l.ts)];
    if (i !== undefined) { loginSets[i].add(l.userId); if (l.kind === 'signup') s.signups[i]++; }
    if (l.ts >= t0) { today.loginEvents++; uniqueToday.add(l.userId); if (l.kind === 'signup') today.signups++; }
    if (l.ts >= weekAgo) hours[hourOf(l.ts)]++;
  }
  for (const c of db.calls) {
    const i = idx[dayKey(c.startedAt)];
    if (i !== undefined) { s[c.kind === 'video' ? 'video' : 'voice'][i]++; s.callMinutes[i] += (c.duration || 0) / 60; }
    if (c.startedAt >= t0) {
      today[c.kind === 'video' ? 'videoCalls' : 'voiceCalls']++;
      today.attempted++;
      if (c.answeredAt) { today.answered++; today.talkSec += c.duration || 0; }
    }
    if (c.startedAt >= weekAgo) {
      hours[hourOf(c.startedAt)]++;
      for (const id of [c.from, c.to]) (activity[id] = activity[id] || { messages: 0, calls: 0 }).calls++;
    }
  }
  for (const m of db.messages) {
    const i = idx[dayKey(m.ts)];
    if (i !== undefined) s.messages[i]++;
    if (m.ts >= t0) today.messages++;
    if (m.ts >= weekAgo) {
      hours[hourOf(m.ts)]++;
      (activity[m.from] = activity[m.from] || { messages: 0, calls: 0 }).messages++;
    }
  }
  s.logins = loginSets.map((x) => x.size);
  s.callMinutes = s.callMinutes.map((x) => Math.round(x));
  const topUsers = Object.entries(activity)
    .map(([id, a]) => ({ id, name: nameOf(id), phone: userById(id)?.phone || '', ...a, score: a.messages + a.calls * 5 }))
    .sort((a, b) => b.score - a.score).slice(0, 8);

  res.json({
    totals: {
      customers: db.users.length,
      blocked: db.users.filter((u) => u.status === 'blocked').length,
      messages: db.messages.length,
      calls: db.calls.length,
      openReports: db.reports.filter((r) => r.status === 'open').length,
      translatedCalls: db.calls.filter((c) => c.translated).length,
      activeStatuses: db.statuses.filter((x) => x.expiresAt > now()).length,
      pendingAds: db.ads.filter((a) => a.status === 'pending').length,
      aiAnswersSinceRestart: aiCount, aiOn: !!GEMINI_KEY, aiError: aiLastError,
      translationsSinceRestart: trCount,
    },
    today: {
      uniqueLogins: uniqueToday.size,
      loginEvents: today.loginEvents,
      signups: today.signups,
      voiceCalls: today.voiceCalls,
      videoCalls: today.videoCalls,
      messages: today.messages,
      avgCallSec: today.answered ? Math.round(today.talkSec / today.answered) : 0,
      successRate: today.attempted ? Math.round((today.answered / today.attempted) * 100) : 0,
      talkMinutes: Math.round(today.talkSec / 60),
    },
    live: liveNow(),
    series: { days: keys, ...s },
    hours,
    topUsers,
  });
});

function userAggregates() {
  const agg = {};
  const get = (id) => (agg[id] = agg[id] || { logins: 0, calls: 0, talkSec: 0, sent: 0 });
  for (const l of db.logins) get(l.userId).logins++;
  for (const c of db.calls) for (const id of [c.from, c.to]) { const a = get(id); a.calls++; a.talkSec += c.duration || 0; }
  for (const m of db.messages) get(m.from).sent++;
  return agg;
}

// Ads review
app.get('/admin/api/ads', adminAuth, (req, res) => {
  res.json(db.ads.slice().sort((a, b) => b.createdAt - a.createdAt).slice(0, 300).map((a) => { const o = userById(a.ownerId); return { ...adOut(a, true), ownerName: o ? o.name : 'Deleted user', ownerPhone: o ? o.phone : '', businessName: o && o.business ? o.business.name : '', live: adLive(a) }; }));
});
app.get('/admin/api/ads/photo/:id', (req, res) => {
  const a = db.ads.find((x) => x.id === req.params.id);
  try { jwt.verify(String(req.query.t || ''), JWT_SECRET); } catch { return res.status(401).end(); }
  if (!a || !a.photo) return res.status(404).end();
  res.setHeader('Content-Type', a.photoMime); const st = chatFileStream(a.photo, 0, a.photoSize - 1); st.on('error', () => res.destroy()); st.pipe(res);
});
app.post('/admin/api/ads/:id/:action', adminAuth, (req, res) => {
  const a = db.ads.find((x) => x.id === req.params.id); if (!a) return res.status(404).json({ error: 'Not found.' });
  const act = req.params.action;
  if (act === 'approve') { Object.assign(a, { status: 'approved', startsAt: now(), endsAt: now() + a.days * 86400000, reason: '' }); }
  else if (act === 'reject') { Object.assign(a, { status: 'rejected', reason: clip(req.body?.reason, 200) || 'Not suitable for Maata.' }); }
  else if (act === 'stop') { Object.assign(a, { status: 'stopped', endsAt: now(), reason: clip(req.body?.reason, 200) }); }
  else return res.status(400).json({ error: 'Unknown action.' });
  save(); audit(req.admin, 'ad-' + act, a.title, a.reason || '');
  io.to('user:' + a.ownerId).emit('ad:update', adOut(a, true));
  res.json(adOut(a, true));
});

// Channels moderation
app.get('/admin/api/channels', adminAuth, (req, res) => res.json(db.channels.slice().sort((a, b) => (b.followers || []).length - (a.followers || []).length).map((c) => ({ id: c.id, name: c.name, description: c.description || '', ownerName: userById(c.ownerId)?.name || 'Deleted user', ownerPhone: userById(c.ownerId)?.phone || '', followers: (c.followers || []).length, posts: db.channelPosts.filter((p) => p.channelId === c.id).length, removed: !!c.removed, createdAt: c.createdAt }))));
app.post('/admin/api/channels/:id/remove', adminAuth, (req, res) => {
  const c = db.channels.find((x) => x.id === req.params.id); if (!c) return res.status(404).json({ error: 'Not found.' });
  c.removed = !c.removed; save(); audit(req.admin, c.removed ? 'channel-remove' : 'channel-restore', c.name, '');
  if (c.removed) io.to('ch:' + c.id).emit('channel:removed', { id: c.id });
  res.json({ removed: c.removed });
});

// ---------- Reels phase 2 & 3: shop the reel, nearby, smart subtitles, AI dubbing, insights, AI moderation ----------
async function ttsLine(text, lang2, gender) { // Google voice for one line → base64 MP3 (cached)
  const lang = TTS_LANG[lang2]; if (!TTS_KEY || !lang || !text) return null;
  const key = lang + '|' + gender + '|' + text; if (ttsCache.has(key)) return ttsCache.get(key);
  try {
    const r = await fetch('https://texttospeech.googleapis.com/v1/text:synthesize?key=' + encodeURIComponent(TTS_KEY), { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ input: { text }, voice: { languageCode: lang, ssmlGender: gender }, audioConfig: { audioEncoding: 'MP3', speakingRate: 1.08 } }), signal: AbortSignal.timeout(9000) });
    const d = await r.json(); if (!d.audioContent) return null;
    ttsCache.set(key, d.audioContent); if (ttsCache.size > 600) ttsCache.delete(ttsCache.keys().next().value);
    return d.audioContent;
  } catch { return null; }
}
async function trLine(text, to) { let out = LANGS.includes(to) ? await translate(text, '', to) : null; if ((!out || out.trim().toLowerCase() === text.toLowerCase()) && GEMINI_KEY) out = (await aiTranslate(text, to)) || out; return out || text; }
const geoKm = (a, b) => { const R = 6371, dLat = (b.lat - a.lat) * Math.PI / 180, dLng = (b.lng - a.lng) * Math.PI / 180, x = Math.sin(dLat / 2) ** 2 + Math.cos(a.lat * Math.PI / 180) * Math.cos(b.lat * Math.PI / 180) * Math.sin(dLng / 2) ** 2; return 2 * R * Math.asin(Math.sqrt(x)); };
// extra details saved with a reel: tagged products, place, smart subtitles, remix credit
app.post('/api/reels/:id/details', auth, (req, res) => {
  const r = db.reels.find((x) => x.id === req.params.id && x.by === req.user.id); if (!r) return res.status(404).json({ error: 'Not found.' });
  const b = req.body || {};
  if (Array.isArray(b.products)) r.products = b.products.map(String).filter((id) => db.products.some((p) => p.id === id && p.ownerId === req.user.id)).slice(0, 5);
  if (b.loc && isFinite(b.loc.lat) && isFinite(b.loc.lng)) r.loc = { lat: Math.round(Number(b.loc.lat) * 100) / 100, lng: Math.round(Number(b.loc.lng) * 100) / 100, place: clip(b.loc.place, 60) }; // ~1 km accuracy only (privacy)
  if (Array.isArray(b.subs)) { r.subs = b.subs.slice(0, 150).map((x) => ({ start: Math.max(0, +x.start || 0), end: Math.max(0, +x.end || 0), text: clip(x.text, 90) })).filter((x) => x.text && x.end > x.start); r.subLang = clip(b.subLang, 5); }
  if (b.remixOf) { const o = db.reels.find((x) => x.id === String(b.remixOf)); if (o) { r.remixOf = o.id; addActivity(o.by, req.user, 'remix', r.id); } }
  save(); res.json(reelView(req.user, r));
});
app.get('/api/reels/:id/extra', auth, (req, res) => {
  const r = reelById(req.params.id); if (!r || !canSeeReel(req.user, r)) return res.status(404).json({ error: 'Reel not found.' });
  const o = r.remixOf && db.reels.find((x) => x.id === r.remixOf), ou = o && userById(o.by);
  res.json({ products: (r.products || []).map((id) => db.products.find((p) => p.id === id && p.available)).filter(Boolean).map(productOut),
    subs: r.subs || [], subLang: r.subLang || '', loc: r.loc ? { place: r.loc.place, km: req.query.lat ? Math.round(geoKm(r.loc, { lat: +req.query.lat, lng: +req.query.lng }) * 10) / 10 : null } : null,
    remixOf: o ? { id: o.id, by: ou ? ou.name : 'Maata user' } : null, canDub: !!(r.subs && r.subs.length), voiceOn: !!TTS_KEY });
});
// smart subtitles in the viewer's language
const dubCache = new Map();
app.post('/api/reels/:id/subs', auth, async (req, res) => {
  const r = reelById(req.params.id); if (!r || !r.subs || !r.subs.length) return res.status(404).json({ error: 'No subtitles.' });
  const to = String(req.body?.lang || 'en'); const ck = r.id + '|s|' + to;
  if (dubCache.has(ck)) return res.json({ subs: dubCache.get(ck) });
  const out = []; for (const s2 of r.subs) out.push({ ...s2, text: r.subLang === to ? s2.text : await trLine(s2.text, to) });
  dubCache.set(ck, out); if (dubCache.size > 400) dubCache.delete(dubCache.keys().next().value);
  res.json({ subs: out });
});
// AI dubbing: the reel's words, translated and spoken in the viewer's language (line by line, in time)
app.post('/api/reels/:id/dub', auth, async (req, res) => {
  const r = reelById(req.params.id); if (!r || !r.subs || !r.subs.length) return res.status(404).json({ error: 'This reel has no speech to dub.' });
  if (!TTS_KEY) return res.status(501).json({ error: 'AI dubbing needs the Google voice key on the server.' });
  const to = String(req.body?.lang || 'en'), gender = req.body?.gender === 'MALE' ? 'MALE' : 'FEMALE', ck = r.id + '|d|' + to + gender;
  if (dubCache.has(ck)) return res.json({ lines: dubCache.get(ck) });
  if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
  const lines = [];
  for (const s2 of r.subs.slice(0, 60)) { const text = r.subLang === to ? s2.text : await trLine(s2.text, to); lines.push({ start: s2.start, end: s2.end, text, audio: await ttsLine(text, to, gender) }); }
  dubCache.set(ck, lines); res.json({ lines });
});
// watch time (for creator insights) — sent when someone moves past a reel
app.post('/api/reels/:id/watch', auth, (req, res) => {
  const r = reelById(req.params.id); if (!r || r.by === req.user.id) return res.json({ ok: true });
  const sec = Math.max(0, Math.min(65, Number(req.body?.sec) || 0)), pct = Math.max(0, Math.min(1, Number(req.body?.pct) || 0));
  r.watchSec = (r.watchSec || 0) + sec; r.watchN = (r.watchN || 0) + 1; if (pct >= 0.9) r.completes = (r.completes || 0) + 1;
  const day = new Date().toISOString().slice(0, 10); r.daily = r.daily || {}; r.daily[day] = (r.daily[day] || 0) + 1;
  const ks = Object.keys(r.daily); if (ks.length > 40) delete r.daily[ks.sort()[0]];
  save(); res.json({ ok: true });
});
app.post('/api/reels/:id/shared', auth, (req, res) => { const r = reelById(req.params.id); if (r) { r.shares = (r.shares || 0) + 1; save(); } res.json({ ok: true }); });
app.post('/api/reels/:id/product-tap', auth, (req, res) => { const r = reelById(req.params.id); if (r && r.by !== req.user.id) { r.productTaps = (r.productTaps || 0) + 1; save(); } res.json({ ok: true }); });
app.get('/api/insights', auth, (req, res) => {
  const mine = db.reels.filter((r) => r.by === req.user.id && !r.removed);
  const days = []; for (let i = 13; i >= 0; i--) days.push(new Date(Date.now() - i * 86400000).toISOString().slice(0, 10));
  const perDay = days.map((d) => mine.reduce((n, r) => n + ((r.daily || {})[d] || 0), 0));
  const reels = mine.map((r) => ({ id: r.id, caption: r.caption, hasThumb: !!r.thumb, createdAt: r.createdAt, views: r.viewCount || 0, likes: (r.likes || []).length,
    comments: db.reelComments.filter((c) => c.reelId === r.id).length, shares: r.shares || 0, productTaps: r.productTaps || 0,
    avgWatch: r.watchN ? Math.round((r.watchSec || 0) / r.watchN * 10) / 10 : 0, completion: r.watchN ? Math.round((r.completes || 0) / r.watchN * 100) : 0, dur: r.dur || 0 }))
    .sort((a, b) => b.views - a.views);
  const sum = (k) => reels.reduce((n, r) => n + r[k], 0);
  res.json({ days, perDay, totals: { reels: reels.length, views: sum('views'), likes: sum('likes'), comments: sum('comments'), shares: sum('shares'), productTaps: sum('productTaps'), followers: followersOf(req.user.id) }, reels });
});
// AI auto-moderation: Gemini looks at the cover picture + caption of every new reel
async function aiModerate(r, jpegBuf) {
  if (!GEMINI_KEY || !r) return;
  const parts = []; if (jpegBuf) parts.push({ inline_data: { mime_type: 'image/jpeg', data: jpegBuf.toString('base64') } });
  parts.push({ text: 'Caption: ' + (r.caption || '(none)') + '\nIs this short-video post safe for a family chat app in India? Return ONLY JSON {"safe":true|false,"reason":"nudity|sexual|violence|gore|hate|weapons|drugs|scam|child-safety|ok"}.' });
  const out = await gemini('You are a strict but fair content safety checker.', [{ role: 'user', parts }], { json: true, maxTokens: 80, temperature: 0 });
  if (out.error) return;
  let v; try { v = JSON.parse(out.text.replace(/^```json|```$/g, '').trim()); } catch { return; }
  r.aiCheck = { safe: !!v.safe, reason: String(v.reason || ''), at: now() };
  if (v.safe === false) { r.hidden = true; r.reports = r.reports || []; r.reports.push({ by: 'maata-ai', reason: 'AI check: ' + v.reason, ts: now() }); io.to('admins').emit('feed', { kind: 'report', text: 'AI flagged a reel (' + v.reason + ')', ts: now() }); io.to('user:' + r.by).emit('reel:review', { id: r.id }); }
  save();
}

// ---------- Help & Support: customers raise complaints about the app, admin replies (ticket numbers) ----------
const SUP_CATS = ['Calls', 'Messages', 'Login / account', 'Notifications', 'Reels', 'Business / catalogue', 'App is slow / crashing', 'Other'];
const supHits = new Map();
const ticketOut = (t) => ({ id: t.id, no: t.no, category: t.category, text: t.text, status: t.status, createdAt: t.createdAt, updatedAt: t.updatedAt, replies: t.replies, unreadForUser: !!t.unreadForUser });
app.post('/api/support', auth, (req, res) => {
  const day = (supHits.get(req.user.id) || []).filter((x) => now() - x < 86400000);
  if (day.length >= 5) return res.status(429).json({ error: 'You can raise up to 5 complaints a day. We will reply to your earlier ones soon.' });
  const text = clip(req.body?.text, 2000); if (text.length < 5) return res.status(400).json({ error: 'Please write what the problem is.' });
  const category = SUP_CATS.includes(req.body?.category) ? req.body.category : 'Other';
  const no = 'MT-' + (1001 + db.tickets.length);
  const t = { id: newId(), no, userId: req.user.id, category, text, device: clip(req.body?.device, 200), status: 'open', replies: [], createdAt: now(), updatedAt: now(), unreadForAdmin: true };
  db.tickets.push(t); day.push(now()); supHits.set(req.user.id, day); save();
  io.to('admins').emit('feed', { kind: 'report', text: '🆘 New complaint ' + no + ' (' + category + ') from ' + req.user.name, ts: now() });
  res.json(ticketOut(t));
});
app.get('/api/support', auth, (req, res) => res.json(db.tickets.filter((t) => t.userId === req.user.id).sort((a, b) => b.updatedAt - a.updatedAt).map(ticketOut)));
app.post('/api/support/:id/reply', auth, (req, res) => {
  const t = db.tickets.find((x) => x.id === req.params.id && x.userId === req.user.id); if (!t) return res.status(404).json({ error: 'Not found.' });
  const text = clip(req.body?.text, 2000); if (!text) return res.status(400).json({ error: 'Write a message.' });
  t.replies.push({ by: 'user', text, ts: now() }); if (t.status === 'resolved') t.status = 'open'; t.updatedAt = now(); t.unreadForAdmin = true; save();
  io.to('admins').emit('feed', { kind: 'report', text: '🆘 ' + t.no + ': new message from ' + req.user.name, ts: now() });
  res.json(ticketOut(t));
});
app.post('/api/support/:id/seen', auth, (req, res) => { const t = db.tickets.find((x) => x.id === req.params.id && x.userId === req.user.id); if (t) { t.unreadForUser = false; save(); } res.json({ ok: true }); });
// admin
app.get('/admin/api/tickets', adminAuth, (req, res) => res.json(db.tickets.slice().sort((a, b) => (a.status === 'resolved') - (b.status === 'resolved') || b.updatedAt - a.updatedAt).slice(0, 300)
  .map((t) => { const u = userById(t.userId) || {}; return { ...ticketOut(t), device: t.device, userName: u.name || 'Deleted user', userPhone: u.phone || '', userId: t.userId, unreadForAdmin: !!t.unreadForAdmin }; })));
app.post('/admin/api/tickets/:id', adminAuth, async (req, res) => {
  const t = db.tickets.find((x) => x.id === req.params.id); if (!t) return res.status(404).json({ error: 'Not found.' });
  const text = clip(req.body?.text, 2000), status = ['open', 'in-progress', 'resolved'].includes(req.body?.status) ? req.body.status : t.status;
  if (text) { t.replies.push({ by: 'admin', text, ts: now() }); t.unreadForUser = true; }
  t.status = status; t.updatedAt = now(); t.unreadForAdmin = false; save();
  audit(req.admin, 'ticket-' + status, t.no, text.slice(0, 60));
  if (text || status === 'resolved') {
    const msg = text ? 'Maata Support (' + t.no + '): ' + text.slice(0, 120) : 'Your complaint ' + t.no + ' is resolved ✅';
    io.to('user:' + t.userId).emit('support:reply', { id: t.id, no: t.no, text: msg });
    const u = userById(t.userId); if (u && !isOnline(u.id)) { try { await pushTo(u, { type: 'msg', title: '🆘 Maata Support', body: msg, tag: 'support-' + t.id, url: '/#support' }); } catch {} }
  }
  res.json({ ok: true });
});

// Brevo API keys stop working after 90 days without use: say hello once a week so the key stays active
async function brevoKeepAlive() { if (!BREVO_KEY) return; try { const r = await fetch('https://api.brevo.com/v3/account', { headers: { 'api-key': BREVO_KEY, accept: 'application/json' }, signal: AbortSignal.timeout(15000) }); if (!r.ok) console.error('[mail] Brevo key check failed: ' + r.status); } catch (e) { console.error('[mail] Brevo key check: ' + e.message); } }
setTimeout(brevoKeepAlive, 120000); setInterval(brevoKeepAlive, 7 * 86400000);
app.get('/admin/api/mail', adminAuth, (req, res) => res.json({ on: MAIL_ON, via: MAIL_VIA, from: MAIL_FROM_RAW || '' }));
app.post('/admin/api/mail-test', adminAuth, async (req, res) => {
  const to = cleanEmail(req.body?.to); if (!to) return res.status(400).json({ error: 'Enter a valid email address.' });
  if (!MAIL_ON) return res.status(400).json({ error: 'Email is off. Add BREVO_API_KEY and MAIL_FROM on Render → Environment.' });
  try { await sendMail(to, 'Maata test email ✅', '<div style="font-family:Arial,sans-serif;padding:20px"><h2 style="color:#0F4C5C">🦜 Maata</h2><p>Email works! Password reset codes will reach your customers.</p></div>', 'Maata test email: email works!'); res.json({ ok: true, via: MAIL_VIA }); }
  catch (e) { console.error('[mail test]', e.message); res.status(502).json({ error: 'Sending failed via ' + MAIL_VIA + ': ' + e.message }); }
});
app.get('/admin/api/features', adminAuth, (req, res) => { const f = featureFlags(); res.json(FEATURE_LIST.map(([group, key, label]) => ({ group, key, label, on: f[key] }))); });
app.post('/admin/api/features', adminAuth, (req, res) => {
  let m = db.meta.find((x) => x.id === 'features'); if (!m) { m = { id: 'features', flags: {}, createdAt: now() }; db.meta.push(m); }
  const set = req.body && typeof req.body.flags === 'object' ? req.body.flags : { [String(req.body?.key || '')]: !!req.body?.on };
  for (const [k, v] of Object.entries(set)) if (FEATURE_LIST.some((f) => f[1] === k)) m.flags[k] = !!v;
  save(); const f = featureFlags(); io.emit('features', f); // every open app updates at once
  audit(req.admin, 'features', Object.entries(set).map(([k, v]) => k + (v ? ' on' : ' off')).join(', '), '');
  res.json(f);
});

// Reels moderation
app.get('/admin/api/reels', adminAuth, (req, res) => res.json(db.reels.slice().sort((a, b) => (b.reports || []).length - (a.reports || []).length || b.createdAt - a.createdAt).slice(0, 200).map((r) => { const u = userById(r.by) || {}; return { id: r.id, caption: r.caption, ownerName: u.name || 'Deleted user', ownerPhone: u.phone || '', createdAt: r.createdAt, likes: (r.likes || []).length, views: r.viewCount || 0, reports: (r.reports || []).map((x) => x.reason || 'reported'), removed: !!r.removed, hidden: !!r.hidden, hasThumb: !!r.thumb }; })));
app.get('/admin/api/reels/:id/:what', (req, res) => {
  try { if (jwt.verify(String(req.query.t || ''), JWT_SECRET).typ !== 'admin') throw 0; } catch { return res.status(401).end(); }
  const r = db.reels.find((x) => x.id === req.params.id); if (!r) return res.status(404).end();
  if (req.params.what === 'thumb' && r.thumb) return streamStored(r.thumb.id, 'image/jpeg', r.thumb.size, req, res);
  if (req.params.what === 'video') return streamStored(r.file.id, r.file.mime, r.file.size, req, res);
  res.status(404).end();
});
app.post('/admin/api/reels/:id/:action', adminAuth, (req, res) => {
  const r = db.reels.find((x) => x.id === req.params.id); if (!r) return res.status(404).json({ error: 'Not found.' });
  if (req.params.action === 'remove') { r.removed = true; r.hidden = true; }
  else if (req.params.action === 'restore') { r.removed = false; r.hidden = false; r.reviewed = true; }
  else if (req.params.action === 'approve') { r.hidden = false; r.reviewed = true; r.reports = []; }
  else return res.status(400).json({ error: 'Unknown action.' });
  save(); audit(req.admin, 'reel-' + req.params.action, (r.caption || 'reel').slice(0, 60), ''); res.json({ ok: true });
});

app.get('/admin/api/users', adminAuth, (req, res) => {
  const q = String(req.query.q || '').toLowerCase().trim();
  const filter = String(req.query.filter || 'all');
  const agg = userAggregates();
  const t0 = startOfToday();
  let list = db.users.map((u) => ({
    ...publicUser(u), status: u.status || 'active', createdAt: u.createdAt, lastLoginAt: u.lastLoginAt || null,
    lastSeenAt: isOnline(u.id) ? now() : u.lastSeenAt || null, online: isOnline(u.id), duplicate: !!u.duplicateOf, email: u.email || '',
    ...(agg[u.id] || { logins: 0, calls: 0, talkSec: 0, sent: 0 }),
  }));
  if (q) list = list.filter((u) => u.name.toLowerCase().includes(q) || u.phone.includes(q));
  if (filter === 'online') list = list.filter((u) => u.online);
  if (filter === 'blocked') list = list.filter((u) => u.status === 'blocked');
  if (filter === 'today') list = list.filter((u) => (u.lastLoginAt || 0) >= t0);
  if (filter === 'new') list = list.filter((u) => u.createdAt >= t0 - 6 * 86400000);
  if (filter === 'inactive') list = list.filter((u) => !u.online && (u.lastSeenAt || u.lastLoginAt || 0) < t0 - 29 * 86400000);
  list.sort((a, b) => (b.online - a.online) || ((b.lastLoginAt || 0) - (a.lastLoginAt || 0)));
  if (req.query.format === 'csv') {
    return sendCsv(res, 'maata-customers', [
      ['Name', 'Mobile', 'Status', 'Joined', 'Last login', 'Logins', 'Calls', 'Talk minutes', 'Messages sent'],
      ...list.map((u) => [u.name, u.phone, u.status, fmtLocal(u.createdAt), fmtLocal(u.lastLoginAt), u.logins, u.calls, Math.round(u.talkSec / 60), u.sent]),
    ]);
  }
  res.json(list);
});

app.get('/admin/api/users/:id', adminAuth, (req, res) => {
  const u = userById(req.params.id);
  if (!u) return res.status(404).json({ error: 'Customer not found.' });
  const agg = userAggregates()[u.id] || { logins: 0, calls: 0, talkSec: 0, sent: 0 };
  const partners = new Set();
  let received = 0;
  for (const m of db.messages) {
    if (m.from === u.id) partners.add(m.to);
    if (m.to === u.id) { partners.add(m.from); received++; }
  }
  const calls = db.calls.filter((c) => c.from === u.id || c.to === u.id).slice(-30).reverse()
    .map((c) => ({ ...c, fromName: nameOf(c.from), toName: nameOf(c.to) }));
  const logins = db.logins.filter((l) => l.userId === u.id).slice(-30).reverse();
  const reports = db.reports.filter((r) => r.target === u.id).map((r) => ({ ...r, reporterName: nameOf(r.reporter) }));
  res.json({
    user: { ...publicUser(u), coins: u.coins || 0, referred: db.users.filter((x) => x.referredBy === u.id).length, referredBy: u.referredBy ? (userById(u.referredBy) || {}).name || '' : '', activeDays: (u.activeDays || []).length, status: u.status || 'active', createdAt: u.createdAt, lastLoginAt: u.lastLoginAt, lastSeenAt: u.lastSeenAt, online: isOnline(u.id) },
    stats: { ...agg, received, chatPartners: partners.size },
    calls, logins, reports,
  });
});

function kick(userId, reason) {
  io.to('user:' + userId).emit('force-logout', { reason });
  io.in('user:' + userId).disconnectSockets(true);
}
app.post('/admin/api/users/:id/block', adminAuth, (req, res) => {
  const u = userById(req.params.id);
  if (!u) return res.status(404).json({ error: 'Customer not found.' });
  const block = !!req.body?.blocked;
  u.status = block ? 'blocked' : 'active';
  if (block) { u.tokenVersion = (u.tokenVersion || 0) + 1; kick(u.id, 'This account is blocked. Contact Maata support.'); }
  audit(req.admin, block ? 'block' : 'unblock', u.name + ' (' + u.phone + ')', String(req.body?.note || ''));
  save(); pushLive();
  res.json({ ok: true, status: u.status });
});
// For customers without an email: admin gives them a temporary password (they should change it after logging in)
app.post('/admin/api/users/:id/reset-password', adminAuth, async (req, res) => {
  const u = userById(req.params.id); if (!u) return res.status(404).json({ error: 'Customer not found.' });
  const temp = 'maata' + crypto.randomInt(1000, 10000);
  u.passHash = await bcrypt.hash(temp, 10); u.tokenVersion = (u.tokenVersion || 0) + 1;
  kick(u.id, 'Your password was reset by Maata support. Log in with the new password.'); save();
  audit(req.admin, 'reset-password', u.name + ' (' + u.phone + ')');
  res.json({ ok: true, tempPassword: temp });
});
app.post('/admin/api/users/:id/logout', adminAuth, (req, res) => {
  const u = userById(req.params.id);
  if (!u) return res.status(404).json({ error: 'Customer not found.' });
  u.tokenVersion = (u.tokenVersion || 0) + 1;
  kick(u.id, 'You were logged out. Please log in again.');
  audit(req.admin, 'force-logout', u.name + ' (' + u.phone + ')');
  save();
  res.json({ ok: true });
});
// Removes a customer and everything they own (used by admins, and by customers deleting their own account)
function deleteAccountData(u, why) {
  kick(u.id, why);
  if (u.photo) deleteChatFile(u.photo);
  const removedWatchers = watchersOf(u.id);
  db.users = db.users.filter((x) => x.id !== u.id);
  for (const g of db.groups) g.members = g.members.filter((x) => x.id !== u.id);
  db.groups = db.groups.filter((g) => g.members.length);
  for (const x of db.users) if (x.contacts) x.contacts = x.contacts.filter((c) => c.id !== u.id);
  for (const m of db.messages) if (m.file && (m.from === u.id || m.to === u.id)) deleteChatFile(m.file.id);
  db.messages = db.messages.filter((m) => m.from !== u.id && m.to !== u.id);
  for (const st of db.statuses.filter((x) => x.userId === u.id)) if (st.type !== 'text') deleteMedia(st.id);
  db.statuses = db.statuses.filter((x) => x.userId !== u.id);
  for (const p of db.products.filter((x) => x.ownerId === u.id)) if (p.photo) deleteChatFile(p.photo);
  db.products = db.products.filter((x) => x.ownerId !== u.id);
  db.ads = db.ads.filter((x) => x.ownerId !== u.id);
  db.aichats = db.aichats.filter((x) => x.userId !== u.id);
  for (const c of db.channels) { if (c.ownerId === u.id) c.removed = true; c.followers = (c.followers || []).filter((x) => x !== u.id); }
  db.logins = db.logins.filter((x) => x.userId !== u.id);
  db.tickets = db.tickets.filter((x) => x.userId !== u.id);
  for (const r of db.reels.filter((x) => x.by === u.id)) { deleteChatFile(r.file.id); if (r.thumb) deleteChatFile(r.thumb.id); }
  db.reels = db.reels.filter((x) => x.by !== u.id); db.reelComments = db.reelComments.filter((c) => c.by !== u.id);
  db.activity = db.activity.filter((a) => a.to !== u.id && a.from !== u.id); for (const x of db.users) if (x.following) x.following = x.following.filter((f) => f !== u.id);
  save();
  for (const w of removedWatchers) io.to('user:' + w).emit('user:removed', { id: u.id });
}
// Google Play requires that people can delete their own account (in the app, and from a web page)
app.post('/api/me/delete', auth, rateLimit, async (req, res) => {
  const ok = await bcrypt.compare(String(req.body?.password || ''), req.user.passHash || '');
  if (!ok) return res.status(400).json({ error: 'Wrong password.' });
  audit('customer:' + req.user.phone, 'self-delete', req.user.name + ' (' + req.user.phone + ')');
  deleteAccountData(req.user, 'Your account was deleted.');
  res.json({ ok: true });
});
const PAGE_CSS = 'body{font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;max-width:760px;margin:0 auto;padding:28px 20px 60px;color:#1C2230;line-height:1.65}h1{color:#0F4C5C}h2{color:#0F4C5C;margin-top:28px;font-size:20px}a{color:#11606F}.top{display:flex;align-items:center;gap:12px}.top img{width:56px;height:56px;border-radius:14px}small{color:#5B6474}';
app.get('/privacy', (req, res) => res.send(`<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Privacy Policy · Maata</title><style>${PAGE_CSS}</style></head><body>
<div class="top"><img src="/icons/icon-192.png" alt=""><h1>Maata Privacy Policy</h1></div><small>Last updated: ${new Date().toISOString().slice(0, 10)} · Contact: ${process.env.PUSH_CONTACT || 'support@maataapp.com'}</small>
<p>Maata ("we") provides free voice calls, video calls and messaging. This policy explains what we collect, why, and your choices.</p>
<h2>What we collect</h2><ul>
<li><b>Account:</b> your name, mobile number and a securely hashed password.</li>
<li><b>Contacts:</b> numbers you save in Maata. If you choose to sync or import phone contacts, numbers are only checked against Maata users; the full list stays on your device.</li>
<li><b>Messages and files:</b> chats, photos, videos, voice messages and documents you send are stored on our servers so they can be delivered and shown on your devices. Maata is not end-to-end encrypted. Disappearing and view-once messages are deleted as described in the app.</li>
<li><b>Calls:</b> calls connect directly between devices or through our media server; we keep call history (who, when, how long), not call audio or video. If you use live call translation, speech is sent to Google Cloud to be transcribed and translated.</li>
<li><b>Status, channels, communities, business profile, catalogue and ads</b> you create.</li>
<li><b>Location:</b> only when you choose to share a location or take a GPS photo.</li>
<li><b>Maata AI:</b> questions you send to Maata AI, text you ask it to rewrite, and (if a business turns on AI auto-reply) customer messages to that business are sent to Google Gemini to create answers.</li>
<li><b>Device data:</b> login times, approximate device type, and a notification token so we can alert you about messages and calls.</li></ul>
<h2>How we use it</h2><p>To deliver messages and calls, keep accounts secure, prevent spam and abuse, review reports, and improve Maata. We do not sell your personal data. We do not show ads based on your chats.</p>
<h2>Who we share it with</h2><p>Service providers that run Maata for us: Render (hosting), MongoDB Atlas (database), Cloudflare (domain and file storage), Google (notifications, translation, speech, Gemini AI), and LiveKit (group call media, if enabled). We may share data when required by Indian law.</p>
<h2>Your choices</h2><ul><li>Block or report anyone; control who sees your status.</li><li>Delete messages, chats or your status at any time.</li><li><b>Delete your account:</b> in the app go to ⋮ → Settings → Delete my account, or visit <a href="/delete-account">maataapp.com/delete-account</a>. Your account, messages, statuses, products and files are deleted from our active systems.</li></ul>
<h2>Children</h2><p>Maata is not intended for children under 13.</p>
<h2>Changes</h2><p>We will update this page when the policy changes.</p></body></html>`));
app.get('/delete-account', (req, res) => res.send(`<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Delete your Maata account</title><style>${PAGE_CSS}input,button{font:inherit;padding:12px 14px;border-radius:12px;border:1.5px solid #D5DCE0;width:100%;box-sizing:border-box;margin:6px 0}button{background:#C8413A;color:#fff;border:0;font-weight:700;cursor:pointer}#msg{margin-top:10px;font-weight:600}</style></head><body>
<div class="top"><img src="/icons/icon-192.png" alt=""><h1>Delete your Maata account</h1></div>
<p>This permanently deletes your Maata account, chats, files, statuses, business profile, catalogue and ads. It cannot be undone.</p>
<p>In the app: <b>⋮ → Settings → Delete my account</b>. Or enter your mobile number and password here:</p>
<form id="f"><input id="ph" inputmode="tel" placeholder="Mobile number" required><input id="pw" type="password" placeholder="Password" required><button>Delete my account permanently</button></form><div id="msg" role="status"></div>
<script>document.getElementById('f').onsubmit=async(e)=>{e.preventDefault();const m=document.getElementById('msg');if(!confirm('Delete your Maata account forever?'))return;
try{const l=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({phone:ph.value,password:pw.value})}).then(r=>r.json());
if(l.error)throw new Error(l.error);if(l.needPin)throw new Error('Two-step verification is on: please delete the account from inside the Maata app.');
const d=await fetch('/api/me/delete',{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+l.token},body:JSON.stringify({password:pw.value})}).then(r=>r.json());
if(d.error)throw new Error(d.error);m.style.color='#2E8B57';m.textContent='Your account has been deleted.';f.remove();}catch(er){m.style.color='#C8413A';m.textContent=er.message;}};</script></body></html>`));

app.delete('/admin/api/users/:id', adminAuth, (req, res) => {
  const u = userById(req.params.id);
  if (!u) return res.status(404).json({ error: 'Customer not found.' });
  deleteAccountData(u, 'This account was deleted.');
  audit(req.admin, 'delete', u.name + ' (' + u.phone + ')');
  res.json({ ok: true });
});

app.get('/admin/api/calls', adminAuth, (req, res) => {
  const since = rangeStart(req.query.range);
  const kind = req.query.kind, status = req.query.status, q = String(req.query.q || '').toLowerCase();
  let list = db.calls.filter((c) => c.startedAt >= since);
  if (kind === 'voice' || kind === 'video') list = list.filter((c) => c.kind === kind);
  if (status) list = list.filter((c) => c.status === status);
  list = list.map((c) => ({ ...c, fromName: nameOf(c.from), toName: nameOf(c.to), fromPhone: userById(c.from)?.phone || '', toPhone: userById(c.to)?.phone || '' }));
  if (q) list = list.filter((c) => (c.fromName + c.toName + c.fromPhone + c.toPhone).toLowerCase().includes(q));
  list.reverse();
  if (req.query.format === 'csv') {
    return sendCsv(res, 'maata-calls', [
      ['Started', 'Caller', 'Caller mobile', 'Receiver', 'Receiver mobile', 'Type', 'Status', 'Duration (sec)'],
      ...list.map((c) => [fmtLocal(c.startedAt), c.fromName, c.fromPhone, c.toName, c.toPhone, c.kind, c.status, c.duration || 0]),
    ]);
  }
  const summary = { total: list.length, voice: 0, video: 0, completed: 0, missed: 0, declined: 0, talkSec: 0 };
  for (const c of list) {
    summary[c.kind]++;
    if (c.status === 'completed') summary.completed++;
    if (c.status === 'missed' || c.status === 'unavailable') summary.missed++;
    if (c.status === 'declined' || c.status === 'busy') summary.declined++;
    summary.talkSec += c.duration || 0;
  }
  res.json({ summary, calls: list.slice(0, 500) });
});

app.get('/admin/api/logins', adminAuth, (req, res) => {
  const since = rangeStart(req.query.range);
  const q = String(req.query.q || '').toLowerCase();
  let list = db.logins.filter((l) => l.ts >= since).map((l) => ({ ...l, name: nameOf(l.userId), phone: userById(l.userId)?.phone || '' }));
  if (q) list = list.filter((l) => (l.name + l.phone).toLowerCase().includes(q));
  list.reverse();
  if (req.query.format === 'csv') {
    return sendCsv(res, 'maata-logins', [['Time', 'Name', 'Mobile', 'Type', 'IP', 'Device'], ...list.map((l) => [fmtLocal(l.ts), l.name, l.phone, l.kind, l.ip, l.ua])]);
  }
  res.json({ total: list.length, unique: new Set(list.map((l) => l.userId)).size, logins: list.slice(0, 500) });
});

// Message report = counts only. Message text is never shown to admins (customer privacy).
app.get('/admin/api/messages', adminAuth, (req, res) => {
  const since = rangeStart(req.query.range || '30d');
  const perDay = {}, senders = {}, pairs = new Set();
  let total = 0, read = 0;
  for (const m of db.messages) {
    if (m.ts < since) continue;
    total++; if (m.status === 'read') read++;
    const k = dayKey(m.ts); perDay[k] = (perDay[k] || 0) + 1;
    senders[m.from] = (senders[m.from] || 0) + 1;
    pairs.add([m.from, m.to].sort().join(':'));
  }
  const top = Object.entries(senders).map(([id, n]) => ({ id, name: nameOf(id), phone: userById(id)?.phone || '', sent: n })).sort((a, b) => b.sent - a.sent);
  if (req.query.format === 'csv') {
    return sendCsv(res, 'maata-messages', [['Date', 'Messages'], ...Object.entries(perDay).sort().map(([d, n]) => [d, n]), [], ['Customer', 'Mobile', 'Messages sent'], ...top.map((t) => [t.name, t.phone, t.sent])]);
  }
  res.json({ total, readRate: total ? Math.round((read / total) * 100) : 0, conversations: pairs.size, activeSenders: top.length, perDay, top: top.slice(0, 20) });
});

app.get('/admin/api/reports', adminAuth, (req, res) => {
  res.json(db.reports.slice().reverse().map((r) => ({
    ...r, reporterName: nameOf(r.reporter), targetName: nameOf(r.target),
    targetPhone: userById(r.target)?.phone || '', targetStatus: userById(r.target)?.status || 'deleted',
  })));
});
app.post('/admin/api/reports/:id/resolve', adminAuth, (req, res) => {
  const r = db.reports.find((x) => x.id === req.params.id);
  if (!r) return res.status(404).json({ error: 'Report not found.' });
  r.status = 'resolved'; r.resolvedAt = now(); r.resolvedBy = req.admin; r.note = String(req.body?.note || '').slice(0, 300);
  audit(req.admin, 'resolve-report', nameOf(r.target), r.note);
  save(); res.json({ ok: true });
});

app.get('/admin/api/announcements', adminAuth, (req, res) => res.json(db.announcements.slice().reverse()));
app.post('/admin/api/announcements', adminAuth, (req, res) => {
  const text = String(req.body?.text || '').trim().slice(0, 300);
  if (text.length < 3) return res.status(400).json({ error: 'Write the announcement first.' });
  for (const a of db.announcements) a.active = false;
  const a = { id: newId(), text, ts: now(), by: req.admin, active: true };
  db.announcements.push(a);
  audit(req.admin, 'announce', 'all customers', text);
  save();
  io.emit('announcement', { id: a.id, text: a.text, ts: a.ts });
  res.json(a);
});
app.delete('/admin/api/announcements/active', adminAuth, (req, res) => {
  for (const a of db.announcements) a.active = false;
  audit(req.admin, 'end-announcement', 'all customers');
  save(); io.emit('announcement', null);
  res.json({ ok: true });
});

// 📁 Admin files: keep Maata's own documents (API guide, legal papers, designs, agreements) safe on the server
const ADMIN_FILE_MAX = 25 * 1024 * 1024;
const AF_CATS = ['Developer', 'Legal', 'Business', 'Design', 'Other'];
const afOut = (f) => ({ id: f.id, name: f.name, mime: f.mime, size: f.size, category: f.category, note: f.note || '', createdAt: f.createdAt, by: f.by });
app.get('/admin/api/files', adminAuth, (req, res) => res.json(db.adminFiles.slice().reverse().map(afOut)));
app.post('/admin/api/files', adminAuth, express.raw({ type: () => true, limit: ADMIN_FILE_MAX }), async (req, res) => {
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).json({ error: 'Choose a file.' });
  const name = clip(String(req.query.name || 'file').replace(/[\\/:*?"<>|]+/g, '_'), 120) || 'file';
  const mime = String(req.headers['content-type'] || 'application/octet-stream').split(';')[0].slice(0, 100);
  const f = { id: newId(), fileId: newId(), name, mime, size: req.body.length, category: AF_CATS.includes(req.query.category) ? req.query.category : 'Other', note: clip(req.query.note, 300), createdAt: now(), by: req.admin };
  try { await saveChatFile(f.fileId, req.body, mime); } catch { return res.status(500).json({ error: 'Could not save the file.' }); }
  db.adminFiles.push(f); audit(req.admin, 'file-upload', f.id, name); save(); res.json(afOut(f));
});
app.get('/admin/api/files/:id/download', adminAuth, (req, res) => {
  const f = db.adminFiles.find((x) => x.id === req.params.id); if (!f) return res.status(404).json({ error: 'File not found.' });
  res.setHeader('Content-Type', f.mime); res.setHeader('Content-Length', f.size); res.setHeader('Cache-Control', 'private, no-store');
  res.setHeader('Content-Disposition', (req.query.view ? 'inline' : 'attachment') + '; filename*=UTF-8\'\'' + encodeURIComponent(f.name));
  const st = chatFileStream(f.fileId, 0, f.size - 1); st.on('error', () => res.destroy()); st.pipe(res);
});
app.put('/admin/api/files/:id', adminAuth, (req, res) => { const f = db.adminFiles.find((x) => x.id === req.params.id); if (!f) return res.status(404).json({ error: 'File not found.' }); if (req.body?.note !== undefined) f.note = clip(req.body.note, 300); if (AF_CATS.includes(req.body?.category)) f.category = req.body.category; save(); res.json(afOut(f)); });
app.delete('/admin/api/files/:id', adminAuth, async (req, res) => { const f = db.adminFiles.find((x) => x.id === req.params.id); if (!f) return res.status(404).json({ error: 'File not found.' }); try { await deleteChatFile(f.fileId); } catch {} db.adminFiles = db.adminFiles.filter((x) => x !== f); audit(req.admin, 'file-delete', f.id, f.name); save(); res.json({ ok: true }); });
app.get('/admin/api/audit', adminAuth, (req, res) => res.json(db.audit.slice(-300).reverse()));

// ---------- Real-time ----------
io.use((socket, next) => {
  try {
    const a = socket.handshake.auth || {};
    if (a.adminToken) {
      const p = jwt.verify(a.adminToken, JWT_SECRET);
      if (p.typ !== 'admin') throw new Error('not admin');
      socket.isAdmin = true; return next();
    }
    const u = findUserByToken(a.token || ''); socket.did = tokenDevice(a.token || '');
    if (!u) return next(new Error('unauthorized'));
    socket.user = u; next();
  } catch { next(new Error('unauthorized')); }
});

function finishCall(rec, status) {
  if (!rec || rec.endedAt) return;
  rec.endedAt = now();
  if (rec.answeredAt) { rec.status = 'completed'; rec.duration = Math.round((rec.endedAt - rec.answeredAt) / 1000); }
  else { rec.status = status; rec.duration = 0; }
  activeCalls.delete(rec.id);
  save(); pushLive();
  for (const id of [rec.from, rec.to]) { const u = userById(id); if (u) io.to('user:' + id).emit('call:log', callView(u, rec)); }
  feed('call', `${rec.kind === 'video' ? 'Video' : 'Voice'} call ${nameOf(rec.from)} → ${nameOf(rec.to)}: ${rec.status}${rec.duration ? ' (' + Math.floor(rec.duration / 60) + 'm ' + (rec.duration % 60) + 's)' : ''}`);
}


// ==================== 📍 TEAM TRACKING (field staff: loans, collections, sales, service) ====================
// A company makes a team, adds staff, and admins (manager / general manager) see where on-duty staff are.
// Rules that protect staff: they must ACCEPT the team (consent screen), location is shared ONLY while they are
// "On duty", duty stops automatically at the end of work hours, the phone always shows that it is sharing,
// and points older than 35 days are deleted.
const TEAM_KEEP_DAYS = 35, TEAM_MAX_PTS = 2000;
const tmMin = (ts) => { const d = new Date(ts + TZ * 60000); return d.getUTCHours() * 60 + d.getUTCMinutes(); };
const tmDow = (ts) => new Date(ts + TZ * 60000).getUTCDay();
const hm = (s, def) => { const m = /^(\d{1,2}):(\d{2})$/.exec(String(s || '')); return m && +m[1] < 24 && +m[2] < 60 ? +m[1] * 60 + +m[2] : def; };
const teamOf = (id) => db.teams.find((t) => t.id === id && !t.deleted);
const tmMember = (t, uid) => t.members.find((m) => m.userId === uid && ['invited', 'active'].includes(m.status));
const tmIsAdmin = (t, uid) => { const m = tmMember(t, uid); return !!(m && m.status === 'active' && m.role === 'admin'); };
const tmAdmins = (t) => t.members.filter((m) => m.status === 'active' && m.role === 'admin').map((m) => m.userId);
// who may see whose location: members see nobody else; admins see members (and the group maker);
// an admin's own location is seen only by the group maker (owner).
function tmCanSee(t, viewer, target) {
  if (viewer === target) return true;
  if (!tmIsAdmin(t, viewer)) return false;
  if (viewer === t.ownerId) return true;
  const tm = t.members.find((m) => m.userId === target);
  return !!tm && (target === t.ownerId || tm.role !== 'admin');
}
function tmCtx(req, res, adminOnly) {
  const t = teamOf(req.params.id); if (!t) { res.status(404).json({ error: 'Team not found.' }); return null; }
  const m = tmMember(t, req.user.id); if (!m) { res.status(403).json({ error: 'You are not in this team.' }); return null; }
  if (adminOnly && !(m.status === 'active' && m.role === 'admin')) { res.status(403).json({ error: 'Only team admins can do this.' }); return null; }
  return { t, m, admin: m.status === 'active' && m.role === 'admin' };
}
function tmInHours() { return true; } // no time limits: sharing stays on until the person turns it off
function tmEndAt(t, ts = now()) { return ts + 24 * 3600000; } // rolls forward on every update (the phone service only needs a far end)
const tmSitesFor = (t, uid) => (t.sites || []).filter((s) => s.on !== false && (!s.staff || !s.staff.length || s.staff.includes(uid)));
function tmWhere(t, uid, p) { // which camp / site is this point in?
  const sites = tmSitesFor(t, uid); if (!sites.length) return { state: 'free', site: null };
  for (const s of sites) if (geoDist([s.lng, s.lat], [p.lng, p.lat]) <= s.r + Math.min(p.acc || 0, 60)) return { state: 'site', site: s };
  let best = null, bd = 1e12; for (const s of sites) { const d = geoDist([s.lng, s.lat], [p.lng, p.lat]); if (d < bd) { bd = d; best = s; } }
  return { state: 'away', site: null, nearest: best, dist: Math.round(bd) };
}
function tmEvent(t, uid, kind, text, p, tell) {
  const ev = { id: newId(), teamId: t.id, userId: uid, at: now(), kind, text: clip(text, 200), lat: p ? p.lat : null, lng: p ? p.lng : null };
  db.teamEvents.push(ev);
  const u = userById(uid), name = u ? u.name : 'Staff';
  for (const a of tmAdmins(t)) {
    if (!tmCanSee(t, a, uid)) continue;
    io.to('user:' + a).emit('team:event', { teamId: t.id, event: { ...ev, name } });
    if (tell && a !== uid) { const au = userById(a); if (au) pushTo(au, { type: 'msg', title: '📍 ' + t.name + ': ' + name, body: text, tag: 'team-' + t.id + '-' + uid, url: '/?team=' + t.id }).catch(() => {}); }
  }
  return ev;
}
function tmLiveRow(t, m) {
  const u = userById(m.userId) || {}, d = (t.duty || {})[m.userId] || {}, last = d.last || null;
  const seen = Math.max(last ? last.t : 0, d.ping || 0); // the phone's last signal (a new place, or "still here")
  let state = 'off'; if (d.on) state = !last ? 'waiting' : now() - seen > (t.offlineMin || 15) * 60000 ? 'offline' : d.gpsOff ? 'gps_off' : d.mock ? 'mock' : d.state || 'free';
  return { userId: m.userId, name: u.name || 'Staff', phone: u.phone || '', role: m.role, status: m.status, onDuty: !!d.on, since: d.since || null, state, site: d.siteName || '', awaySince: d.awaySince || null, last, seen: seen || null, bat: d.bat ?? (last ? last.bat : null), km: d.km || 0,
    diag: d.diag || null, wokeAt: d.wokeAt || null, wakeOk: d.wakeOk || null, appv: u.androidVer || '', appLatest: latestApk(), via: d.via || '' };
}
let apkVer = { v: 0, at: 0 }; // newest Android app build (from app-version.json, read at most once a minute)
function latestApk() { if (now() - apkVer.at > 60000) { apkVer.at = now(); for (const f of [path.join(PAGES, 'app-version.json'), path.join(__dirname, 'app-version.json')]) { try { apkVer.v = Number(JSON.parse(fs.readFileSync(f, 'utf8')).version) || 0; break; } catch { /* next */ } } } return apkVer.v; }
// 📲 the phone is quiet (Android paused Maata while it is closed): a high-priority push wakes it, and it sends its place at once
function tmWake(t, uid, gap) {
  const d = (t.duty || {})[uid], u = userById(uid); if (!d || !d.on || !u || !(u.fcmTokens || []).length) return false;
  if (now() - (d.wokeAt || 0) < gap) return false;
  d.wokeAt = now(); fcmTo(u, { type: 'track_wake', teamId: t.id }, 300).catch(() => {}); return true;
}
function tmOut(t, uid) {
  const admin = tmIsAdmin(t, uid), me = tmMember(t, uid);
  const owner = userById(t.ownerId) || {}, seenBy = (target) => tmAdmins(t).filter((a) => a !== target && tmCanSee(t, a, target)).map((a) => (userById(a) || {}).name || 'Admin');
  const base = { id: t.id, name: t.name, type: t.type || 'work', isOwner: t.ownerId === uid, ownerName: owner.name || '', seenBy: seenBy(uid), hours: t.hours, interval: t.interval, awayMin: t.awayMin, offlineMin: t.offlineMin, myRole: me ? me.role : null, myStatus: me ? me.status : null, admin, inHours: tmInHours(t),
    admins: seenBy(uid), mySites: tmSitesFor(t, uid).map((s) => ({ id: s.id, name: s.name, lat: s.lat, lng: s.lng, r: s.r })), me: me ? tmLiveRow(t, me) : null, endAt: tmEndAt(t) };
  if (!admin) return base;
  return { ...base, sites: t.sites || [], members: t.members.filter((m) => ['invited', 'active'].includes(m.status) && (tmCanSee(t, uid, m.userId) || m.status === 'invited')).map((m) => ({ ...tmLiveRow(t, m), ...(tmCanSee(t, uid, m.userId) ? {} : { onDuty: false, last: null, state: 'off', km: 0 }), owner: m.userId === t.ownerId })) };
}
function tmTrackDoc(t, uid, ts) { const id = t.id + ':' + uid + ':' + dayKey(ts); let d = db.teamTracks.find((x) => x.id === id); if (!d) { d = { id, teamId: t.id, userId: uid, day: dayKey(ts), createdAt: now(), pts: [] }; db.teamTracks.push(d); } return d; }
function tmStopDuty(t, uid, why, auto) {
  const d = (t.duty || {})[uid]; if (!d || !d.on) return false;
  d.on = false; d.offAt = now(); tmEvent(t, uid, auto ? 'auto_off' : 'duty_off', why, d.last, auto);
  io.to('user:' + uid).emit('team:duty', { teamId: t.id, on: false, why });
  return true;
}
// ---- teams ----
app.get('/api/teams', auth, (req, res) => res.json(db.teams.filter((t) => !t.deleted && tmMember(t, req.user.id)).map((t) => tmOut(t, req.user.id))));
app.post('/api/teams', auth, (req, res) => {
  const name = clip(req.body?.name, 60); if (!name) return res.status(400).json({ error: 'Write the group name.' });
  const type = req.body?.type === 'family' ? 'family' : 'work';
  if (db.teams.filter((t) => t.ownerId === req.user.id && !t.deleted).length >= 10) return res.status(400).json({ error: 'You can make up to 10 teams.' });
  const t = { id: newId(), name, type, ownerId: req.user.id, createdAt: now(), members: [{ userId: req.user.id, role: 'admin', status: 'active', joinedAt: now(), consentAt: now() }],
    hours: { start: '09:00', end: '18:00', days: [1, 2, 3, 4, 5, 6] }, interval: 60, awayMin: 20, offlineMin: 15, sites: [], duty: {} };
  db.teams.push(t); save(); res.json(tmOut(t, req.user.id));
});
app.get('/api/teams/:id', auth, (req, res) => { const c = tmCtx(req, res); if (!c) return;
  if (tmIsAdmin(c.t, req.user.id)) c.t.watchAt = now(); // an admin is looking at the map: phones send every 30 s for a while
  if (tmIsAdmin(c.t, req.user.id)) for (const [uid, d] of Object.entries(c.t.duty || {})) // the admin is looking: wake quiet phones now
    if (uid !== req.user.id && d.on && tmCanSee(c.t, req.user.id, uid) && now() - Math.max(d.last ? d.last.t : 0, d.ping || 0) > 2 * 60000) tmWake(c.t, uid, 2 * 60000);
  res.json(tmOut(c.t, req.user.id)); });
app.post('/api/teams/:id/wake/:uid', auth, (req, res) => { const c = tmCtx(req, res, true); if (!c) return; const uid = String(req.params.uid);
  if (!tmCanSee(c.t, req.user.id, uid)) return res.status(403).json({ error: 'You cannot see this person\'s location.' });
  const d = (c.t.duty || {})[uid]; if (!d || !d.on) return res.status(400).json({ error: 'Location sharing is OFF for this person.' });
  const u = userById(uid); if (!u || !(u.fcmTokens || []).length) return res.status(400).json({ error: 'This phone has no Maata app notifications. Ask them to open the Maata app once.' });
  tmWake(c.t, uid, 15000); save(); res.json({ ok: true }); });
// 📲 ask a member to update the Maata app on their phone (old app = location stops when Maata is closed)
app.post('/api/teams/:id/ask-update/:uid', auth, (req, res) => { const c = tmCtx(req, res, true); if (!c) return; const uid = String(req.params.uid);
  if (!tmCanSee(c.t, req.user.id, uid)) return res.status(403).json({ error: 'Not allowed.' }); const u = userById(uid); if (!u) return res.status(404).json({ error: 'Not found.' });
  const d = (c.t.duty || {})[uid] || {}; if (now() - (d.askedUpd || 0) < 60000) return res.json({ ok: true }); if (c.t.duty && c.t.duty[uid]) c.t.duty[uid].askedUpd = now();
  pushTo(u, { type: 'msg', title: '📲 Please update Maata', body: (req.user.name || 'Your group admin') + ' asks you to update the Maata app, so your live location keeps working when Maata is closed. Tap here.', tag: 'maata-update', url: '/?appupdate=1' }).catch(() => {});
  save(); res.json({ ok: true }); });
app.put('/api/teams/:id', auth, (req, res) => {
  const c = tmCtx(req, res, true); if (!c) return; const t = c.t, b = req.body || {};
  if (b.name !== undefined) t.name = clip(b.name, 60) || t.name;
  if (b.hours) { const st = hm(b.hours.start, -1), en = hm(b.hours.end, -1); if (st < 0 || en < 0 || en <= st) return res.status(400).json({ error: 'Check work hours (end after start).' }); const days = (Array.isArray(b.hours.days) ? b.hours.days : []).map(Number).filter((x) => x >= 0 && x <= 6); t.hours = { start: b.hours.start, end: b.hours.end, days: days.length ? [...new Set(days)] : [1, 2, 3, 4, 5, 6] }; }
  if (b.interval !== undefined) t.interval = [30, 60, 120, 300].includes(Number(b.interval)) ? Number(b.interval) : 60;
  if (b.awayMin !== undefined) t.awayMin = Math.max(5, Math.min(240, Math.round(Number(b.awayMin) || 20)));
  if (b.offlineMin !== undefined) t.offlineMin = Math.max(5, Math.min(240, Math.round(Number(b.offlineMin) || 15)));
  save(); res.json(tmOut(t, req.user.id));
});
app.delete('/api/teams/:id', auth, (req, res) => { const c = tmCtx(req, res, true); if (!c) return; if (c.t.ownerId !== req.user.id) return res.status(403).json({ error: 'Only the person who made the team can delete it.' }); c.t.deleted = true; for (const uid of Object.keys(c.t.duty || {})) tmStopDuty(c.t, uid, 'Team deleted'); db.teamTracks = db.teamTracks.filter((x) => x.teamId !== c.t.id); save(); res.json({ ok: true }); });
// members
app.post('/api/teams/:id/members', auth, (req, res) => {
  const c = tmCtx(req, res, true); if (!c) return; const t = c.t;
  const u = findByMobile(cleanMobile(req.body?.phone || '')); if (!u) return res.status(404).json({ error: 'This number is not on Maata. Ask them to install Maata first.' });
  if (t.members.filter((m) => ['invited', 'active'].includes(m.status)).length >= 200) return res.status(400).json({ error: 'A team can have up to 200 members.' });
  const role = req.body?.role === 'admin' && t.ownerId === req.user.id ? 'admin' : 'staff'; // only the group maker gives admin
  let m = t.members.find((x) => x.userId === u.id);
  if (m && ['invited', 'active'].includes(m.status)) { if (t.ownerId === req.user.id) m.role = role; save(); return res.json(tmOut(t, req.user.id)); }
  if (m) Object.assign(m, { role, status: 'invited', invitedAt: now(), invitedBy: req.user.id }); else t.members.push({ userId: u.id, role, status: 'invited', invitedAt: now(), invitedBy: req.user.id });
  save(); io.to('user:' + u.id).emit('team:invite', { teamId: t.id });
  pushTo(u, { type: 'msg', title: '📍 ' + t.name, body: req.user.name + ' added you to the group. Open to read and accept.', tag: 'team-inv-' + t.id, url: '/?team=' + t.id }).catch(() => {});
  res.json(tmOut(t, req.user.id));
});
app.put('/api/teams/:id/members/:uid', auth, (req, res) => { const c = tmCtx(req, res, true); if (!c) return; if (c.t.ownerId !== req.user.id) return res.status(403).json({ error: 'Only the group maker can give or remove admin.' }); const m = tmMember(c.t, req.params.uid); if (!m) return res.status(404).json({ error: 'Not in this team.' }); if (m.userId === c.t.ownerId && req.body?.role !== 'admin') return res.status(400).json({ error: 'The team maker stays admin.' }); m.role = req.body?.role === 'admin' ? 'admin' : 'staff'; save(); res.json(tmOut(c.t, req.user.id)); });
app.delete('/api/teams/:id/members/:uid', auth, (req, res) => { const c = tmCtx(req, res, true); if (!c) return; const m = tmMember(c.t, req.params.uid); if (!m) return res.status(404).json({ error: 'Not in this team.' }); if (m.role === 'admin' && c.t.ownerId !== req.user.id) return res.status(403).json({ error: 'Only the group maker can remove an admin.' }); if (m.userId === c.t.ownerId) return res.status(400).json({ error: 'The team maker cannot be removed.' }); tmStopDuty(c.t, m.userId, 'Removed from team'); m.status = 'removed'; save(); res.json(tmOut(c.t, req.user.id)); });
app.post('/api/teams/:id/accept', auth, (req, res) => { const c = tmCtx(req, res); if (!c) return; if (c.m.status !== 'invited') return res.json(tmOut(c.t, req.user.id)); if (!req.body?.agree) return res.status(400).json({ error: 'Please read and agree first.' }); c.m.status = 'active'; c.m.joinedAt = now(); c.m.consentAt = now(); tmEvent(c.t, req.user.id, 'joined', 'Joined the team', null); save(); res.json(tmOut(c.t, req.user.id)); });
app.post('/api/teams/:id/leave', auth, (req, res) => { const c = tmCtx(req, res); if (!c) return; if (req.user.id === c.t.ownerId) return res.status(400).json({ error: 'You made this team. Delete it instead.' }); tmStopDuty(c.t, req.user.id, 'Left the team'); c.m.status = c.m.status === 'invited' ? 'declined' : 'left'; tmEvent(c.t, req.user.id, 'left', c.m.status === 'declined' ? 'Did not accept the invite' : 'Left the team', null, true); save(); res.json({ ok: true }); });
// camps / work sites (geofences)
app.post('/api/teams/:id/sites', auth, (req, res) => {
  const c = tmCtx(req, res, true); if (!c) return; const b = req.body || {}, la = Number(b.lat), lo = Number(b.lng);
  if (!(Math.abs(la) <= 90 && Math.abs(lo) <= 180) || (!la && !lo)) return res.status(400).json({ error: 'Choose the place on the map.' });
  const name = clip(b.name, 60); if (!name) return res.status(400).json({ error: 'Write the camp / site name.' });
  c.t.sites = c.t.sites || []; if (c.t.sites.length >= 100) return res.status(400).json({ error: 'Up to 100 sites.' });
  const ids = new Set(c.t.members.map((m) => m.userId)), s = { id: newId(), name, lat: +la.toFixed(6), lng: +lo.toFixed(6), r: Math.max(50, Math.min(5000, Math.round(Number(b.r) || 300))), staff: (Array.isArray(b.staff) ? b.staff : []).filter((x) => ids.has(x)), on: true, createdAt: now() };
  if (b.id) { const old = c.t.sites.find((x) => x.id === b.id); if (!old) return res.status(404).json({ error: 'Site not found.' }); Object.assign(old, { ...s, id: old.id, createdAt: old.createdAt, on: b.on !== false }); } else c.t.sites.push(s);
  save(); res.json(tmOut(c.t, req.user.id));
});
app.delete('/api/teams/:id/sites/:sid', auth, (req, res) => { const c = tmCtx(req, res, true); if (!c) return; c.t.sites = (c.t.sites || []).filter((s) => s.id !== req.params.sid); save(); res.json(tmOut(c.t, req.user.id)); });
// duty on / off (staff)
app.post('/api/teams/:id/duty', auth, (req, res) => {
  const c = tmCtx(req, res); if (!c) return; const t = c.t;
  if (c.m.status !== 'active') return res.status(400).json({ error: 'Accept the team first.' });
  t.duty = t.duty || {}; const d = t.duty[req.user.id] = t.duty[req.user.id] || {};
  if (req.body?.on) {
    if (!tmInHours(t)) return res.status(400).json({ error: 'Duty can start only in work hours (' + t.hours.start + '–' + t.hours.end + ').' });
    if (!d.on) { Object.assign(d, { on: true, since: now(), state: null, awaySince: null, awayTold: false, offlineTold: false, gpsOff: false, mock: false, km: d.day === dayKey(now()) ? d.km || 0 : 0, day: dayKey(now()), last: null }); tmEvent(t, req.user.id, 'duty_on', 'Started duty', null); }
  } else tmStopDuty(t, req.user.id, 'Ended duty');
  save(); res.json({ ...tmOut(t, req.user.id), interval: t.interval });
});
// location points (from the Android service or the open app); batch allowed so nothing is lost offline
app.post('/api/teams/:id/loc', auth, (req, res) => {
  const t = teamOf(req.params.id); if (!t) return res.status(404).json({ on: false, error: 'Team not found.' });
  const m = tmMember(t, req.user.id), d = (t.duty || {})[req.user.id];
  if (!m || m.status !== 'active' || !d || !d.on) return res.json({ on: false });
  if (!tmInHours(t)) { tmStopDuty(t, req.user.id, 'Work hours are over', true); save(); return res.json({ on: false }); }
  const raw = Array.isArray(req.body?.pts) ? req.body.pts.slice(-300) : [];
  const pts = raw.map((p) => ({ t: Math.min(now(), Math.round(Number(p.t) || now())), lat: Number(p.lat), lng: Number(p.lng), acc: Math.round(Number(p.acc) || 0), bat: p.bat == null ? null : Math.max(0, Math.min(100, Math.round(Number(p.bat)))), mock: !!p.mock, spd: Math.max(0, Number(p.spd) || 0) }))
    .filter((p) => Math.abs(p.lat) <= 90 && Math.abs(p.lng) <= 180 && (p.lat || p.lng) && p.t > (d.since || 0) - 60000 && p.acc < 2000).sort((a, b) => a.t - b.t);
  const dg = req.body?.diag; if (dg && typeof dg === 'object') d.diag = { bgLoc: dg.bgLoc !== false, battery: dg.battery !== false, notif: dg.notif !== false, autostart: !!dg.autostart, maker: clip(dg.maker, 30), perm: dg.perm !== false, gps: dg.gps !== false, app: clip(dg.app, 20), at: now() };
  if (req.body?.via === 'wake') d.wakeOk = now();
  if (['web', 'service', 'wake'].includes(req.body?.via)) d.via = req.body.via;
  if (req.body?.alive) { // phone heartbeat: Maata is running (also when closed / screen locked), even if the person has not moved
    d.ping = now(); if (req.body.bat != null && Number(req.body.bat) >= 0) d.bat = Math.max(0, Math.min(100, Math.round(Number(req.body.bat))));
    if (d.offlineTold && d.last) { d.offlineTold = false; tmEvent(t, req.user.id, 'back', 'Phone is online again', d.last); }
  }
  if (req.body?.gps === 'off' && !d.gpsOff) { d.gpsOff = true; tmEvent(t, req.user.id, 'gps_off', 'Switched off location (GPS) on the phone', d.last, true); }
  for (const p of pts) {
    if (d.last && p.t <= d.last.t) continue;
    const doc = tmTrackDoc(t, req.user.id, p.t), prev = d.last;
    if (doc.pts.length && p.t <= doc.pts[doc.pts.length - 1][0]) continue; // phone clock went back: keep the day in time order
    if (prev && p.acc < 100 && (prev.acc || 0) < 100) { const step = geoDist([prev.lng, prev.lat], [p.lng, p.lat]); if (step > 25 && step < 50000) d.km = Math.round(((d.km || 0) + step / 1000) * 100) / 100; }
    doc.pts.push([p.t, +p.lat.toFixed(6), +p.lng.toFixed(6), p.acc, p.bat, p.mock ? 1 : 0]); if (doc.pts.length > TEAM_MAX_PTS) doc.pts.splice(0, doc.pts.length - TEAM_MAX_PTS);
    if (d.gpsOff) { d.gpsOff = false; tmEvent(t, req.user.id, 'gps_on', 'Location is on again', p); }
    if (p.mock && !d.mock) { d.mock = true; tmEvent(t, req.user.id, 'mock', '⚠️ FAKE GPS app detected. The location may be false.', p, true); }
    if (d.offlineTold) { d.offlineTold = false; tmEvent(t, req.user.id, 'back', 'Location is coming again', p); }
    const w = tmWhere(t, req.user.id, p);
    if (w.state === 'site') {
      if (d.state !== 'site' || d.siteId !== w.site.id) tmEvent(t, req.user.id, 'enter', 'Reached ' + w.site.name, p, d.awayTold);
      Object.assign(d, { state: 'site', siteId: w.site.id, siteName: w.site.name, awaySince: null, awayTold: false });
    } else if (w.state === 'away') {
      if (d.state === 'site') tmEvent(t, req.user.id, 'leave', 'Left ' + (d.siteName || 'the camp'), p);
      if (d.state !== 'away') d.awaySince = p.t;
      Object.assign(d, { state: 'away', siteId: null, siteName: '', awayDist: w.dist, awayNear: w.nearest ? w.nearest.name : '' });
      if (!d.awayTold && p.t - d.awaySince >= (t.awayMin || 20) * 60000) { d.awayTold = true; tmEvent(t, req.user.id, 'away', 'Away from ' + (w.nearest ? w.nearest.name : 'the camp') + ' for ' + Math.round((p.t - d.awaySince) / 60000) + ' min · ' + (w.dist >= 1000 ? (w.dist / 1000).toFixed(1) + ' km' : w.dist + ' m') + ' away', p, true); }
    } else Object.assign(d, { state: 'free', siteId: null, siteName: '' });
    d.last = p;
  }
  if (pts.length || req.body?.alive) { const row = tmLiveRow(t, m); for (const a of tmAdmins(t)) if (tmCanSee(t, a, req.user.id)) io.to('user:' + a).emit('team:live', { teamId: t.id, row }); }
  save(); res.json({ on: true, interval: now() - (t.watchAt || 0) < 3 * 60000 ? Math.min(t.interval || 60, 30) : t.interval, endAt: tmEndAt(t), state: d.state || 'free', site: d.siteName || '' });
});
// admin: one person's day (route, stops, events, numbers)
function tmStops(pts) { // places where the person stayed 10+ minutes within ~150 m
  const out = []; let i = 0;
  while (i < pts.length) { let j = i + 1; while (j < pts.length && geoDist([pts[i][2], pts[i][1]], [pts[j][2], pts[j][1]]) < 150) j++; const dur = pts[j - 1][0] - pts[i][0]; if (dur >= 600000) { const seg = pts.slice(i, j); out.push({ from: pts[i][0], to: pts[j - 1][0], min: Math.round(dur / 60000), lat: seg.reduce((a, p) => a + p[1], 0) / seg.length, lng: seg.reduce((a, p) => a + p[2], 0) / seg.length }); } i = j; }
  return out;
}
function tmDay(t, uid, day) {
  const doc = db.teamTracks.find((x) => x.id === t.id + ':' + uid + ':' + day), pts = doc ? [...doc.pts].sort((a, b) => a[0] - b[0]) : [];
  const evs = db.teamEvents.filter((e) => e.teamId === t.id && e.userId === uid && dayKey(e.at) === day);
  let km = 0, siteMs = 0, awayMs = 0; const sites = tmSitesFor(t, uid);
  for (let k = 1; k < pts.length; k++) { const a = pts[k - 1], b = pts[k]; const g = Math.max(0, Math.min(b[0] - a[0], 15 * 60000)); if (a[3] < 100 && b[3] < 100) { const s = geoDist([a[2], a[1]], [b[2], b[1]]); if (s > 25 && s < 50000) km += s / 1000; }
    if (sites.length) { const w = tmWhere(t, uid, { lat: a[1], lng: a[2], acc: a[3] }); if (w.state === 'site') siteMs += g; else awayMs += g; } }
  const stops = tmStops(pts).map((s) => { const w = sites.length ? tmWhere(t, uid, { lat: s.lat, lng: s.lng, acc: 0 }) : { state: 'free' }; return { ...s, site: w.state === 'site' ? w.site.name : '', away: w.state === 'away' ? w.dist : 0 }; });
  return { day, pts: pts.map((p) => ({ t: p[0], lat: p[1], lng: p[2], acc: p[3], bat: p[4], mock: !!p[5] })), events: evs, stops, km: Math.round(km * 10) / 10, siteMin: Math.round(siteMs / 60000), awayMin: Math.round(awayMs / 60000), first: pts[0] ? pts[0][0] : null, lastAt: pts.length ? pts[pts.length - 1][0] : null, mock: pts.some((p) => p[5]) };
}
app.get('/api/teams/:id/day', auth, (req, res) => {
  const c = tmCtx(req, res); if (!c) return; const uid = String(req.query.uid || req.user.id);
  if (!tmCanSee(c.t, req.user.id, uid)) return res.status(403).json({ error: 'You cannot see this person\'s location.' });
  if (!tmMember(c.t, uid)) return res.status(404).json({ error: 'Not in this team.' });
  const day = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.day || '')) ? req.query.day : dayKey(now());
  res.json({ ...tmDay(c.t, uid, day), name: (userById(uid) || {}).name || 'Staff' });
});
app.get('/api/teams/:id/events', auth, (req, res) => { const c = tmCtx(req, res, true); if (!c) return; const day = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.day || '')) ? req.query.day : dayKey(now()); res.json(db.teamEvents.filter((e) => e.teamId === c.t.id && dayKey(e.at) === day && tmCanSee(c.t, req.user.id, e.userId)).slice(-400).reverse().map((e) => ({ ...e, name: (userById(e.userId) || {}).name || 'Staff' }))); });
app.post('/api/teams/:id/ai', auth, async (req, res) => {
  const c = tmCtx(req, res, true); if (!c) return; const day = /^\d{4}-\d{2}-\d{2}$/.test(String(req.body?.day || '')) ? req.body.day : dayKey(now());
  const hhmm = (ts) => ts ? new Date(ts + TZ * 60000).toISOString().slice(11, 16) : '-';
  const rows = c.t.members.filter((m) => m.status === 'active' && m.userId !== req.user.id && tmCanSee(c.t, req.user.id, m.userId)).map((m) => { const d = tmDay(c.t, m.userId, day); return { name: (userById(m.userId) || {}).name, firstSeen: hhmm(d.first), lastSeen: hhmm(d.lastAt), km: d.km, minutesAtCamp: d.siteMin, minutesAway: d.awayMin, longStopsAwayFromCamp: d.stops.filter((s) => s.away).map((s) => hhmm(s.from) + '-' + hhmm(s.to) + ' (' + s.min + ' min, ' + (s.away / 1000).toFixed(1) + ' km from camp)'), alerts: d.events.filter((e) => ['away', 'offline', 'gps_off', 'mock', 'auto_off'].includes(e.kind)).map((e) => hhmm(e.at) + ' ' + e.text), fakeGps: d.mock }; });
  let text = '';
  if (GEMINI_KEY && rows.length) { const lang = AI_LANG_NAME[req.user.lang] || 'English'; const g = await gemini('You help a field-team manager in India review staff attendance from location data. Using ONLY these facts, write a short report in ' + lang + ': first one line per staff member (emoji ✅ good / ⚠️ check / ❌ problem), then 2-3 practical suggestions. Be fair: phones can lose signal, so say "check with them" rather than accusing. Do not invent facts.', [{ role: 'user', parts: [{ text: 'Team: ' + c.t.name + '. Day: ' + day + '. Work hours ' + c.t.hours.start + '-' + c.t.hours.end + '.\n' + JSON.stringify(rows) }] }], { maxTokens: 700, temperature: 0.3 }); text = String(g.text || '').trim(); }
  if (!text) text = rows.length ? rows.map((r) => (r.alerts.length || r.fakeGps ? '⚠️ ' : r.firstSeen === '-' ? '❌ ' : '✅ ') + r.name + ': ' + (r.firstSeen === '-' ? 'no location today' : r.firstSeen + '–' + r.lastSeen + ', ' + r.km + ' km, at camp ' + r.minutesAtCamp + ' min, away ' + r.minutesAway + ' min' + (r.alerts.length ? '. Alerts: ' + r.alerts.join('; ') : ''))).join('\n') : 'No staff in this team yet.';
  res.json({ text: clip(text, 4000), rows });
});
// every minute: phones that stopped sending, and duty that should end with work hours
setInterval(() => {
  let changed = false;
  for (const t of db.teams) { if (t.deleted) continue;
    for (const [uid, d] of Object.entries(t.duty || {})) { if (!d.on) continue;
      if (!tmInHours(t)) { tmStopDuty(t, uid, 'Work hours are over', true); changed = true; continue; }
      const ref = Math.max(d.last ? d.last.t : d.since, d.ping || 0);
      if (now() - ref > 3 * 60000) tmWake(t, uid, 3 * 60000);
      { const u = userById(uid), lv = latestApk(), v = u && u.androidVer ? Number(String(u.androidVer).split('.').pop()) || 0 : 0; // old Maata app while sharing: ask it to update itself, once a day
        if (u && lv && v && v < lv && now() - (u.updAskedAt || 0) > 20 * 3600000) { u.updAskedAt = now(); changed = true;
          pushTo(u, { type: 'msg', title: '📲 Update Maata', body: 'A new Maata app is ready. Tap to update — your live location keeps working when Maata is closed.', tag: 'maata-update', url: '/?appupdate=1' }).catch(() => {}); } } // a quiet phone is woken every 3 min → a fresh place at least every ~3–5 min
      if (!d.offlineTold && now() - ref > (t.offlineMin || 15) * 60000) { d.offlineTold = true; tmEvent(t, uid, 'offline', 'No signal for ' + (t.offlineMin || 15) + ' min (phone switched off, no internet, or the phone stopped Maata — see “Keep sharing when Maata is closed”)', d.last, true); changed = true; }
    } }
  const cut = now() - TEAM_KEEP_DAYS * 86400000; const a = db.teamTracks.length, b = db.teamEvents.length;
  db.teamTracks = db.teamTracks.filter((x) => x.createdAt > cut); db.teamEvents = db.teamEvents.filter((x) => x.at > cut);
  if (changed || a !== db.teamTracks.length || b !== db.teamEvents.length) save();
}, 60000);


// ==================== 🏢 MAATA BUSINESS MESSAGING (API for banks, schools, hospitals, offices, shops) ====================
// An organisation's own software sends messages to its customers in Maata through a REST API (/v1).
// Trust rules: only VERIFIED organisations can send live messages; customers control each business
// (All / Important only / None), can report & block; marketing needs the customer's opt-in; every message has a
// state (accepted → delivered → read / failed); webhooks are signed; keys are stored only as hashes.
const BM_KEEP_DAYS = 90;
const BM_LIMITS = { test: { perMin: 30, perDay: 200 }, live: { perMin: 300, perDay: 20000 } };
const BM_BAD = /\b(otp|one[\s-]?time password|password|passcode|pin|cvv|card number)\b/i; // never in templates
const bmApi = (b) => { b.api = { status: 'none', webhook: null, autoTranslate: true, ...(b.api || {}) }; return b.api; };
const bmSha = (s) => crypto.createHash('sha256').update(String(s)).digest('hex');
const bmBiz = (bizId) => { const u = userById(bizId); return u && u.business && u.status !== 'blocked' ? u : null; };
const bmBizCard = (u) => ({ bizId: u.id, name: u.business.name, category: u.business.category || '', verified: bmApi(u.business).status === 'verified', photo: !!u.photo });
const bmPref = (uid, bizId) => db.bmPrefs.find((p) => p.id === uid + ':' + bizId);
function bmSetPref(uid, bizId, patch) { let p = bmPref(uid, bizId); if (!p) { p = { id: uid + ':' + bizId, userId: uid, bizId, level: 'important', createdAt: now() }; db.bmPrefs.push(p); } Object.assign(p, patch, { at: now() }); return p; }
function bmRelation(uid, owner) { // the customer already deals with this business
  if (db.invoices.some((i) => i.bizId === owner.id && cleanMobile(i.customer && i.customer.phone) === cleanMobile((userById(uid) || {}).phone))) return true;
  if (db.orders.some((o) => o.bizId === owner.id && o.customerId === uid)) return true;
  return db.bmMsgs.some((m) => m.bizId === owner.id && m.to === uid && m.dir === 'in');
}
function bmCanSend(owner, user, category) { // → null if allowed, else an error code
  const p = bmPref(user.id, owner.id);
  if (p && (p.blocked || p.level === 'none')) return 'opted_out';
  if (category === 'marketing') return p && p.level === 'all' && p.optInAt ? null : 'no_marketing_consent';
  if (p && p.optInAt) return null;
  return bmRelation(user.id, owner) ? null : 'no_consent';
}
const BM_ERR = { not_on_maata: 'This number is not on Maata.', opted_out: 'The customer turned off messages from you.', no_consent: 'The customer has not opted in. Share your opt-in link or QR first.', no_marketing_consent: 'Marketing needs the customer to choose "All messages" for your business.', not_verified: 'Your business is not verified yet. Use a test key (mk_test_) until Maata verifies you.', test_only_team: 'Test keys can send only to your own team (owner and staff).', rate_limited: 'Too many messages. Slow down.', daily_limit: 'Daily message limit reached.', bad_template: 'Template not found or not approved.', missing_variable: 'A template variable is missing.', window_closed: 'Free text is allowed only within 24 hours after the customer messaged you. Use a template.', suspended: 'Messaging for this business is suspended.' };
function bmRender(body, vars) { const miss = []; const out = String(body).replace(/\{\{\s*([a-z0-9_]{1,40})\s*\}\}/gi, (m0, k) => { const v = vars && vars[k]; if (v == null || v === '') { miss.push(k); return m0; } return clip(String(v), 300); }); return { text: out, miss }; }
const bmVars = (body) => [...new Set([...String(body).matchAll(/\{\{\s*([a-z0-9_]{1,40})\s*\}\}/gi)].map((m) => m[1]))];
function bmMsgOut(m) { return { id: m.id, to: m.toPhone, status: m.state, reason: m.reason || null, template: m.templateName || null, created_at: new Date(m.createdAt).toISOString(), delivered_at: m.deliveredAt ? new Date(m.deliveredAt).toISOString() : null, read_at: m.readAt ? new Date(m.readAt).toISOString() : null, send_at: m.sendAt ? new Date(m.sendAt).toISOString() : null }; }
const bmHits = new Map();
function bmRate(keyId, env) { const lim = BM_LIMITS[env]; const t = now(); const arr = (bmHits.get(keyId) || []).filter((x) => t - x < 60000); if (arr.length >= lim.perMin) return false; arr.push(t); bmHits.set(keyId, arr); return true; }
const bmToday = (bizId, env) => db.bmMsgs.filter((m) => m.bizId === bizId && m.dir === 'out' && m.env === env && m.createdAt >= startOfToday()).length;
// ---- webhooks (signed, retried with backoff) ----
const BM_BACKOFF = [10e3, 60e3, 5 * 60e3, 30 * 60e3, 2 * 3600e3];
function bmEvent(owner, type, data) {
  const api = bmApi(owner.business); if (!api.webhook || !api.webhook.url || api.webhook.off) return;
  db.bmJobs.push({ id: newId(), bizId: owner.id, createdAt: now(), next: now(), tries: 0, body: JSON.stringify({ id: 'evt_' + crypto.randomBytes(8).toString('hex'), type, created_at: new Date().toISOString(), data }) });
}
let bmWorking = false;
async function bmWork() {
  if (bmWorking) return; bmWorking = true;
  try {
    // 1) scheduled / accepted messages → deliver
    for (const m of db.bmMsgs.filter((x) => x.dir === 'out' && (x.state === 'accepted' || (x.state === 'queued' && x.sendAt <= now()))).slice(0, 200)) await bmDeliver(m);
    // 2) webhooks
    for (const j of db.bmJobs.filter((x) => x.next <= now()).slice(0, 50)) {
      const owner = bmBiz(j.bizId), hook = owner && bmApi(owner.business).webhook;
      if (!hook || !hook.url) { j.dead = true; continue; }
      const ts = Math.floor(now() / 1000), sig = crypto.createHmac('sha256', hook.secret).update(ts + '.' + j.body).digest('hex');
      let ok = false, code = 0;
      try { const r = await fetch(hook.url, { method: 'POST', headers: { 'Content-Type': 'application/json', 'User-Agent': 'Maata-Webhooks/1', 'X-Maata-Signature': 't=' + ts + ',v1=' + sig }, body: j.body, signal: AbortSignal.timeout(8000) }); code = r.status; ok = r.ok; } catch { code = 0; }
      hook.lastCode = code; hook.lastAt = now();
      if (ok) j.dead = 'done'; else { j.tries++; if (j.tries > BM_BACKOFF.length) j.dead = 'failed'; else j.next = now() + BM_BACKOFF[j.tries - 1] * (0.8 + Math.random() * 0.4); }
    }
    const before = db.bmJobs.length; db.bmJobs = db.bmJobs.filter((j) => !j.dead || (j.dead === 'failed' && now() - j.createdAt < 3 * 86400000));
    const cut = now() - BM_KEEP_DAYS * 86400000; db.bmMsgs = db.bmMsgs.filter((m) => m.createdAt > cut);
    if (before !== db.bmJobs.length) save();
  } catch (e) { console.error('[bm] worker', e.message); }
  bmWorking = false;
}
setInterval(bmWork, 5000);
async function bmDeliver(m) {
  const owner = bmBiz(m.bizId), user = userById(m.to);
  if (!owner || !user) { m.state = 'failed'; m.reason = 'not_on_maata'; save(); return; }
  // AI translation into the customer's language (original kept and shown on tap)
  const api = bmApi(owner.business), want = user.lang && user.lang !== 'en' ? user.lang : null;
  if (api.autoTranslate && want && GEMINI_KEY && !m.lang && AI_LANG_NAME[want]) {
    try { const g = await gemini('Translate this business message into ' + AI_LANG_NAME[want] + '. Keep numbers, dates, amounts, names, links and codes exactly the same. Return only the translation.', [{ role: 'user', parts: [{ text: m.text }] }], { maxTokens: 600, temperature: 0.1 }); const tr = String(g.text || '').trim(); if (tr && tr.length < 4000) { m.original = m.text; m.text = tr; m.lang = want; } } catch { /* send the original */ }
  }
  m.state = 'sent'; m.sentAt = now();
  io.to('user:' + user.id).emit('bm:msg', { bizId: owner.id, msg: bmCustOut(m), biz: bmBizCard(owner) });
  const v = bmApi(owner.business).status === 'verified' ? ' ✔' : '';
  pushTo(user, { type: 'msg', title: '🏢 ' + owner.business.name + v, body: m.sensitive ? 'You have a new message. Open Maata to read it.' : clip(m.title ? m.title + ': ' + m.text : m.text, 140), tag: 'bm-' + owner.id, url: '/?bm=' + owner.id }).catch(() => {});
  save();
}
function bmMark(m, state) { // delivered / read (from the customer's app)
  if (m.dir !== 'out') return false;
  const order = { accepted: 0, queued: 0, sent: 1, delivered: 2, read: 3, failed: -1 };
  if ((order[state] ?? -1) <= (order[m.state] ?? -1)) return false;
  if (state === 'delivered' || state === 'read') { if (!m.deliveredAt) m.deliveredAt = now(); }
  if (state === 'read') m.readAt = now();
  m.state = state; const owner = bmBiz(m.bizId);
  if (owner && m.env === 'live') bmEvent(owner, 'message.' + state, bmMsgOut(m));
  return true;
}
const bmCustOut = (m) => ({ id: m.id, dir: m.dir, title: m.title || '', text: m.text, original: m.original || '', lang: m.lang || '', buttons: m.buttons || [], category: m.category, at: m.sentAt || m.createdAt, state: m.state, test: m.env === 'test' });
// ---- create a message (shared by the API, the dashboard test console and replies) ----
function bmCreate(owner, key, b, actorEnv) {
  const api = bmApi(owner.business), env = key ? key.env : actorEnv;
  if (api.status === 'suspended') return { code: 403, err: 'suspended' };
  if (env === 'live' && api.status !== 'verified') return { code: 403, err: 'not_verified' };
  const idem = clip(b.idempotency_key, 100);
  if (idem) { const dup = db.bmMsgs.find((m) => m.bizId === owner.id && m.idem === idem); if (dup) return { msg: dup, dup: true }; }
  if (key && !bmRate(key.id, env)) return { code: 429, err: 'rate_limited' };
  if (bmToday(owner.id, env) >= (api.dailyLimit || BM_LIMITS[env].perDay)) return { code: 429, err: 'daily_limit' };
  const raw = String((b.to && (b.to.phone || b.to)) || ''), user = raw.startsWith('user_') ? userById(raw.slice(5)) : findByMobile(cleanMobile(raw));
  if (!user || user.status === 'blocked') return { code: 404, err: 'not_on_maata' };
  if (env === 'test' && !(user.id === owner.id || (owner.business.members || []).some((x) => x.userId === user.id && x.status !== 'removed'))) return { code: 403, err: 'test_only_team' };
  let text, title = '', buttons = [], category = 'transactional', tpl = null, sensitive = false;
  if (b.template) {
    tpl = db.bmTemplates.find((t) => t.bizId === owner.id && (t.id === b.template || t.name === b.template) && !t.deleted);
    if (!tpl || tpl.status !== 'approved') return { code: 400, err: 'bad_template' };
    const r = bmRender(tpl.body, b.variables || {}); if (r.miss.length) return { code: 400, err: 'missing_variable', detail: r.miss };
    text = r.text; title = tpl.title ? bmRender(tpl.title, b.variables || {}).text : ''; buttons = (tpl.buttons || []).map((x) => ({ label: x.label, url: bmRender(x.url, b.variables || {}).text })); category = tpl.category; sensitive = !!tpl.sensitive;
  } else {
    text = clip(b.text, 2000); if (!text) return { code: 400, err: 'bad_template' };
    const lastIn = db.bmMsgs.filter((m) => m.bizId === owner.id && m.to === user.id && m.dir === 'in').pop();
    if (!lastIn || now() - lastIn.createdAt > 24 * 3600000) return { code: 400, err: 'window_closed' };
  }
  if (env === 'live') { const why = bmCanSend(owner, user, category); if (why) return { code: 403, err: why }; }
  const sendAt = b.send_at ? Date.parse(b.send_at) : 0;
  const m = { id: 'msg_' + crypto.randomBytes(10).toString('hex'), bizId: owner.id, keyId: key ? key.id : null, env, dir: 'out', to: user.id, toPhone: user.phone, templateId: tpl ? tpl.id : null, templateName: tpl ? tpl.name : null, title, text, buttons, category, sensitive, idem: idem || null,
    state: sendAt > now() + 30000 && sendAt < now() + 30 * 86400000 ? 'queued' : 'accepted', sendAt: sendAt > now() + 30000 ? sendAt : null, createdAt: now() };
  db.bmMsgs.push(m); save(); setImmediate(bmWork);
  return { msg: m };
}
// ---- public REST API for the organisation's software ----
function bmKeyAuth(req, res, next) {
  const h = String(req.headers.authorization || ''), tok = h.startsWith('Bearer ') ? h.slice(7).trim() : '';
  const key = tok && db.bmKeys.find((k) => k.hash === bmSha(tok) && !k.revokedAt);
  if (!key) return res.status(401).json({ error: { code: 'unauthorized', message: 'Missing or wrong API key. Use: Authorization: Bearer mk_live_… or mk_test_…' } });
  const owner = bmBiz(key.bizId); if (!owner) return res.status(401).json({ error: { code: 'unauthorized', message: 'Business not found.' } });
  key.lastUsedAt = now(); req.bmKey = key; req.bmOwner = owner; next();
}
const bmErr = (res, r) => res.status(r.code || 400).json({ error: { code: r.err, message: BM_ERR[r.err] || r.err, detail: r.detail || undefined } });
app.post('/v1/messages', bmKeyAuth, (req, res) => { const r = bmCreate(req.bmOwner, req.bmKey, req.body || {}); if (r.err) return bmErr(res, r); res.status(r.dup ? 200 : 202).json(bmMsgOut(r.msg)); });
app.get('/v1/messages/:id', bmKeyAuth, (req, res) => { const m = db.bmMsgs.find((x) => x.id === req.params.id && x.bizId === req.bmOwner.id); if (!m) return res.status(404).json({ error: { code: 'not_found', message: 'Message not found.' } }); res.json(bmMsgOut(m)); });
app.get('/v1/templates', bmKeyAuth, (req, res) => res.json({ data: db.bmTemplates.filter((t) => t.bizId === req.bmOwner.id && !t.deleted).map((t) => ({ id: t.id, name: t.name, category: t.category, language: t.lang, status: t.status, variables: bmVars(t.body + ' ' + (t.title || '') + ' ' + (t.buttons || []).map((x) => x.url).join(' ')) })) }));
app.get('/v1/usage', bmKeyAuth, (req, res) => { const env = req.bmKey.env, api = bmApi(req.bmOwner.business); res.json({ environment: env, verified: api.status === 'verified', today: bmToday(req.bmOwner.id, env), daily_limit: api.dailyLimit || BM_LIMITS[env].perDay, per_minute_limit: BM_LIMITS[env].perMin }); });
app.get('/v1/customers/:phone', bmKeyAuth, (req, res) => { const u = findByMobile(cleanMobile(req.params.phone)); if (!u) return res.json({ on_maata: false, can_send: false }); const why = bmCanSend(req.bmOwner, u, 'transactional'), whyM = bmCanSend(req.bmOwner, u, 'marketing'); res.json({ on_maata: true, can_send: !why, can_send_marketing: !whyM, reason: why || null }); });
// ---- customer side (Maata app) ----
app.get('/api/bm/inbox', auth, (req, res) => {
  const mine = db.bmMsgs.filter((m) => m.to === req.user.id && ['sent', 'delivered', 'read'].includes(m.state) || (m.to === req.user.id && m.dir === 'in'));
  const by = new Map(); for (const m of mine) { const e = by.get(m.bizId) || { last: null, unread: 0 }; e.last = m; if (m.dir === 'out' && m.state !== 'read') e.unread++; by.set(m.bizId, e); }
  const out = []; for (const [bizId, e] of by) { const o = bmBiz(bizId); if (!o) continue; const p = bmPref(req.user.id, bizId); out.push({ ...bmBizCard(o), last: { text: e.last.text.slice(0, 120), at: e.last.sentAt || e.last.createdAt, dir: e.last.dir }, unread: e.unread, level: p ? p.level : 'important', blocked: !!(p && p.blocked) }); }
  out.sort((a, b) => b.last.at - a.last.at); res.json(out);
});
app.get('/api/bm/thread/:bizId', auth, (req, res) => {
  const o = bmBiz(req.params.bizId); if (!o) return res.status(404).json({ error: 'Business not found.' });
  const list = db.bmMsgs.filter((m) => m.bizId === o.id && m.to === req.user.id && (m.dir === 'in' || ['sent', 'delivered', 'read'].includes(m.state))).slice(-200);
  let ch = false; for (const m of list) if (m.dir === 'out' && m.state !== 'read') ch = bmMark(m, 'read') || ch; if (ch) save();
  const p = bmPref(req.user.id, o.id);
  res.json({ biz: { ...bmBizCard(o), address: o.business.address || '', phone: o.phone }, pref: { level: p ? p.level : 'important', optIn: !!(p && p.optInAt), blocked: !!(p && p.blocked) }, msgs: list.map(bmCustOut) });
});
app.post('/api/bm/delivered', auth, (req, res) => { const ids = Array.isArray(req.body?.ids) ? req.body.ids.slice(0, 100) : []; let ch = false; for (const id of ids) { const m = db.bmMsgs.find((x) => x.id === id && x.to === req.user.id); if (m) ch = bmMark(m, 'delivered') || ch; } if (ch) save(); res.json({ ok: true }); });
app.post('/api/bm/thread/:bizId/reply', auth, (req, res) => {
  const o = bmBiz(req.params.bizId); if (!o) return res.status(404).json({ error: 'Business not found.' });
  const text = clip(req.body?.text, 1000); if (!text) return res.status(400).json({ error: 'Write a message.' });
  if (!db.bmMsgs.some((m) => m.bizId === o.id && m.to === req.user.id && m.dir === 'out')) return res.status(400).json({ error: 'You can reply after this business messages you.' });
  const m = { id: 'msg_' + crypto.randomBytes(10).toString('hex'), bizId: o.id, env: 'live', dir: 'in', to: req.user.id, toPhone: req.user.phone, text, state: 'received', createdAt: now() };
  db.bmMsgs.push(m);
  if (/^\s*(stop|unsubscribe|aapu|aapandi|ఆపు)\s*$/i.test(text)) { bmSetPref(req.user.id, o.id, { level: 'none' }); bmEvent(o, 'customer.opted_out', { phone: req.user.phone, source: 'reply' }); }
  bmEvent(o, 'message.received', { id: m.id, from: req.user.phone, name: req.user.name, text, received_at: new Date().toISOString() });
  for (const id of [o.id, ...((o.business.members || []).filter((x) => x.status !== 'removed').map((x) => x.userId))]) io.to('user:' + id).emit('bm:reply', { bizId: o.id });
  save(); res.json(bmCustOut(m));
});
app.put('/api/bm/prefs/:bizId', auth, (req, res) => {
  const o = bmBiz(req.params.bizId); if (!o) return res.status(404).json({ error: 'Business not found.' });
  const level = ['all', 'important', 'none'].includes(req.body?.level) ? req.body.level : 'important', prev = bmPref(req.user.id, o.id);
  const p = bmSetPref(req.user.id, o.id, { level, blocked: false, ...(req.body?.optIn ? { optInAt: now(), source: clip(req.body.source, 40) || 'app' } : {}) });
  if (level === 'none' && (!prev || prev.level !== 'none')) bmEvent(o, 'customer.opted_out', { phone: req.user.phone, source: 'app' });
  if (req.body?.optIn) bmEvent(o, 'customer.opted_in', { phone: req.user.phone, level, source: p.source });
  save(); res.json({ level: p.level, optIn: !!p.optInAt });
});
app.get('/api/bm/biz/:bizId', auth, (req, res) => { const o = bmBiz(req.params.bizId); if (!o) return res.status(404).json({ error: 'Business not found.' }); const p = bmPref(req.user.id, o.id); res.json({ ...bmBizCard(o), address: o.business.address || '', level: p ? p.level : null, optIn: !!(p && p.optInAt) }); });
app.post('/api/bm/report/:bizId', auth, (req, res) => {
  const o = bmBiz(req.params.bizId); if (!o) return res.status(404).json({ error: 'Business not found.' });
  bmSetPref(req.user.id, o.id, { level: 'none', blocked: true }); const api = bmApi(o.business); api.reports = (api.reports || 0) + 1;
  db.reports.push({ id: newId(), by: req.user.id, reported: o.id, reason: 'Business messages: ' + (clip(req.body?.reason, 300) || 'spam'), ts: now(), status: 'open' });
  bmEvent(o, 'customer.opted_out', { phone: req.user.phone, source: 'report' }); save(); res.json({ ok: true });
});
// ---- organisation dashboard (Maata Business → Business messaging API) ----
const bmTplOut = (t) => ({ id: t.id, name: t.name, title: t.title || '', body: t.body, category: t.category, lang: t.lang, status: t.status, reason: t.reason || '', sensitive: !!t.sensitive, buttons: t.buttons || [], variables: bmVars(t.body + ' ' + (t.title || '') + ' ' + (t.buttons || []).map((x) => x.url).join(' ')), createdAt: t.createdAt });
function bmDash(c) {
  const api = bmApi(c.biz), mine = db.bmMsgs.filter((m) => m.bizId === c.bizId), since = now() - 30 * 86400000, out = mine.filter((m) => m.dir === 'out' && m.createdAt > since);
  const cnt = (f) => out.filter(f).length;
  return { status: api.status, apply: api.apply || null, reason: api.reason || '', autoTranslate: api.autoTranslate !== false, webhook: api.webhook ? { url: api.webhook.url, lastCode: api.webhook.lastCode ?? null, lastAt: api.webhook.lastAt || null, secretHint: api.webhook.secret ? api.webhook.secret.slice(0, 6) + '…' : '' } : null,
    stats: { sent30: out.length, delivered: cnt((m) => ['delivered', 'read'].includes(m.state)), read: cnt((m) => m.state === 'read'), failed: cnt((m) => m.state === 'failed'), today: bmToday(c.bizId, 'live'), testToday: bmToday(c.bizId, 'test'), optedIn: db.bmPrefs.filter((p) => p.bizId === c.bizId && p.optInAt && p.level !== 'none').length, optedOut: db.bmPrefs.filter((p) => p.bizId === c.bizId && p.level === 'none').length, replies: mine.filter((m) => m.dir === 'in' && m.createdAt > since).length },
    limits: { live: { ...BM_LIMITS.live, perDay: api.dailyLimit || BM_LIMITS.live.perDay }, test: BM_LIMITS.test },
    keys: db.bmKeys.filter((k) => k.bizId === c.bizId && !k.revokedAt).map((k) => ({ id: k.id, name: k.name, env: k.env, hint: k.prefix + '…' + k.last4, createdAt: k.createdAt, lastUsedAt: k.lastUsedAt || null })),
    templates: db.bmTemplates.filter((t) => t.bizId === c.bizId && !t.deleted).map(bmTplOut), optinLink: (process.env.PUBLIC_URL || 'https://maataapp.com').replace(/\/$/, '') + '/?optin=' + c.bizId };
}
app.get('/api/biz/:bizId/bm', auth, (req, res) => { const c = bizCtx(req, res, 'settings'); if (c) res.json(bmDash(c)); });
app.get('/api/biz/:bizId/bm/optin-qr', auth, async (req, res) => { const c = bizCtx(req, res); if (!c) return; res.json({ svg: await QRCode.toString(bmDash(c).optinLink, { type: 'svg', margin: 1, errorCorrectionLevel: 'M' }) }); });
app.post('/api/biz/:bizId/bm/apply', auth, (req, res) => {
  const c = bizCtx(req, res, 'settings'); if (!c) return; const api = bmApi(c.biz), b = req.body || {};
  if (api.status === 'verified') return res.json(bmDash(c)); if (api.status === 'suspended') return res.status(403).json({ error: BM_ERR.suspended });
  const legal = clip(b.legalName, 100), kind = clip(b.orgType, 40), use = clip(b.useCase, 500); if (!legal || !kind || use.length < 15) return res.status(400).json({ error: 'Fill the legal name, organisation type and what messages you will send.' });
  api.apply = { legalName: legal, orgType: kind, regNo: clip(b.regNo, 40), website: clip(b.website, 120), contact: clip(b.contact, 80), useCase: use, at: now(), by: req.user.id }; api.status = 'pending'; api.reason = '';
  bizLog(c, 'bm-apply', '', legal); save(); res.json(bmDash(c));
});
app.post('/api/biz/:bizId/bm/keys', auth, (req, res) => {
  const c = bizCtx(req, res, 'settings'); if (!c) return; const env = req.body?.env === 'live' ? 'live' : 'test';
  if (env === 'live' && bmApi(c.biz).status !== 'verified') return res.status(403).json({ error: 'Live keys are given after Maata verifies your organisation. Use a test key now.' });
  if (db.bmKeys.filter((k) => k.bizId === c.bizId && !k.revokedAt).length >= 10) return res.status(400).json({ error: 'Up to 10 active keys. Revoke an old one.' });
  const secret = 'mk_' + env + '_' + crypto.randomBytes(24).toString('hex');
  const k = { id: newId(), bizId: c.bizId, name: clip(req.body?.name, 40) || (env === 'live' ? 'Live key' : 'Test key'), env, hash: bmSha(secret), prefix: secret.slice(0, 12), last4: secret.slice(-4), createdAt: now(), by: req.user.id };
  db.bmKeys.push(k); bizLog(c, 'bm-key', k.id, k.name + ' (' + env + ')'); save(); res.json({ key: secret, ...bmDash(c) }); // the full key is shown ONLY now
});
app.delete('/api/biz/:bizId/bm/keys/:id', auth, (req, res) => { const c = bizCtx(req, res, 'settings'); if (!c) return; const k = db.bmKeys.find((x) => x.id === req.params.id && x.bizId === c.bizId); if (!k) return res.status(404).json({ error: 'Key not found.' }); k.revokedAt = now(); bizLog(c, 'bm-key-revoke', k.id, k.name); save(); res.json(bmDash(c)); });
app.post('/api/biz/:bizId/bm/templates', auth, (req, res) => {
  const c = bizCtx(req, res, 'settings'); if (!c) return; const b = req.body || {};
  const name = String(b.name || '').toLowerCase().replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, '').slice(0, 40); if (!name) return res.status(400).json({ error: 'Template name: letters, numbers and _ (e.g. fee_reminder).' });
  const body = clip(b.body, 1500), title = clip(b.title, 80); if (body.length < 5) return res.status(400).json({ error: 'Write the message.' });
  if (BM_BAD.test(body + ' ' + title)) return res.status(400).json({ error: 'For safety, templates cannot carry OTPs, passwords, PINs or card details.' });
  const buttons = (Array.isArray(b.buttons) ? b.buttons : []).slice(0, 2).map((x) => ({ label: clip(x.label, 25), url: String(x.url || '').trim().slice(0, 300) })).filter((x) => x.label && /^https:\/\/\S+$/.test(x.url.replace(/\{\{[^}]+\}\}/g, 'x')));
  const category = b.category === 'marketing' ? 'marketing' : 'transactional';
  if (db.bmTemplates.some((t) => t.bizId === c.bizId && t.name === name && !t.deleted && t.id !== b.id)) return res.status(400).json({ error: 'A template with this name exists.' });
  const t = { id: newId(), bizId: c.bizId, name, title, body, category, lang: clip(b.lang, 5) || 'en', buttons, sensitive: !!b.sensitive, status: category === 'marketing' ? 'pending' : 'approved', createdAt: now(), by: req.user.id };
  if (category === 'marketing') t.reason = 'Marketing templates are checked by Maata before use.';
  db.bmTemplates.push(t); bizLog(c, 'bm-template', t.id, name); save(); res.json(bmDash(c));
});
app.delete('/api/biz/:bizId/bm/templates/:id', auth, (req, res) => { const c = bizCtx(req, res, 'settings'); if (!c) return; const t = db.bmTemplates.find((x) => x.id === req.params.id && x.bizId === c.bizId); if (!t) return res.status(404).json({ error: 'Template not found.' }); t.deleted = true; save(); res.json(bmDash(c)); });
app.put('/api/biz/:bizId/bm/settings', auth, (req, res) => {
  const c = bizCtx(req, res, 'settings'); if (!c) return; const api = bmApi(c.biz), b = req.body || {};
  if (b.autoTranslate !== undefined) api.autoTranslate = !!b.autoTranslate;
  if (b.webhookUrl !== undefined) { const u = String(b.webhookUrl || '').trim(); if (!u) api.webhook = null; else { if (!process.env.BM_ALLOW_LOCAL && (!/^https:\/\/[^\s]+$/.test(u) || /localhost|127\.0\.0\.1|0\.0\.0\.0|\.local\b|169\.254\.|\b10\.\d+\.|192\.168\./i.test(u))) return res.status(400).json({ error: 'Webhook must be a public https:// address.' }); api.webhook = { url: u, secret: (api.webhook && api.webhook.secret && !b.newSecret) ? api.webhook.secret : 'whsec_' + crypto.randomBytes(20).toString('hex') }; } }
  save(); const out = bmDash(c); if (b.webhookUrl && api.webhook) out.webhookSecret = api.webhook.secret; res.json(out);
});
app.post('/api/biz/:bizId/bm/webhook-test', auth, (req, res) => { const c = bizCtx(req, res, 'settings'); if (!c) return; if (!bmApi(c.biz).webhook) return res.status(400).json({ error: 'Add a webhook address first.' }); bmEvent(c.owner, 'test.ping', { hello: 'from Maata' }); save(); setImmediate(bmWork); res.json({ ok: true }); });
app.post('/api/biz/:bizId/bm/test-send', auth, (req, res) => { const c = bizCtx(req, res, 'settings'); if (!c) return; const r = bmCreate(c.owner, null, { ...(req.body || {}), idempotency_key: null }, 'test'); if (r.err) return res.status(r.code || 400).json({ error: BM_ERR[r.err] || r.err }); res.json(bmMsgOut(r.msg)); });
app.get('/api/biz/:bizId/bm/logs', auth, (req, res) => { const c = bizCtx(req, res, 'settings'); if (!c) return; res.json(db.bmMsgs.filter((m) => m.bizId === c.bizId).slice(-150).reverse().map((m) => ({ ...bmMsgOut(m), dir: m.dir, env: m.env, text: m.text.slice(0, 160), name: (userById(m.to) || {}).name || '' }))); });
app.post('/api/biz/:bizId/bm/reply', auth, (req, res) => { const c = bizCtx(req, res, 'bill'); if (!c) return; const r = bmCreate(c.owner, null, { to: req.body?.phone, text: req.body?.text }, bmApi(c.biz).status === 'verified' ? 'live' : 'test'); if (r.err) return res.status(r.code || 400).json({ error: BM_ERR[r.err] || r.err }); res.json(bmMsgOut(r.msg)); });
app.post('/api/biz/:bizId/bm/ai-template', auth, async (req, res) => {
  const c = bizCtx(req, res, 'settings'); if (!c) return; const goal = clip(req.body?.goal, 400); if (!goal) return res.status(400).json({ error: 'Say what the message is for.' });
  if (!GEMINI_KEY) return res.status(503).json({ error: 'Maata AI is not switched on.' });
  const g = await gemini('You write short, clear message templates for an Indian organisation (' + (c.biz.category || 'business') + ': ' + c.biz.name + ') to send to its customers in the Maata app. Use {{variable_name}} (lowercase, underscores) for every changing value such as names, dates, amounts, ids. Never include OTPs, passwords or card numbers. Language: ' + (AI_LANG_NAME[req.body?.lang] || 'English') + '. Return ONLY JSON: {"name":"short_snake_case","title":"short title","body":"message text (max 400 chars)","category":"transactional|marketing"}',
    [{ role: 'user', parts: [{ text: goal }] }], { json: true, maxTokens: 500, temperature: 0.4 });
  let d = null; try { d = JSON.parse(g.text || ''); } catch {}
  if (!d || !d.body) return res.status(502).json({ error: 'AI could not write it. Try again with more detail.' });
  res.json({ name: clip(d.name, 40), title: clip(d.title, 80), body: clip(d.body, 1500), category: d.category === 'marketing' ? 'marketing' : 'transactional' });
});
// ---- platform admin: verify organisations ----
app.get('/admin/api/bm', adminAuth, (req, res) => res.json(db.users.filter((u) => u.business && u.business.api && u.business.api.status && u.business.api.status !== 'none').map((u) => { const a = u.business.api, out = db.bmMsgs.filter((m) => m.bizId === u.id && m.dir === 'out' && m.env === 'live'); return { bizId: u.id, name: u.business.name, phone: u.phone, owner: u.name, status: a.status, apply: a.apply || null, reason: a.reason || '', reports: a.reports || 0, sent30: out.filter((m) => m.createdAt > now() - 30 * 86400000).length, dailyLimit: a.dailyLimit || BM_LIMITS.live.perDay, pendingTemplates: db.bmTemplates.filter((t) => t.bizId === u.id && t.status === 'pending' && !t.deleted).map(bmTplOut) }; }).sort((a, b) => (a.status === 'pending' ? 0 : 1) - (b.status === 'pending' ? 0 : 1))));
app.post('/admin/api/bm/:bizId/:action', adminAuth, (req, res) => {
  const o = bmBiz(req.params.bizId); if (!o) return res.status(404).json({ error: 'Business not found.' }); const a = bmApi(o.business), act = req.params.action, why = clip(req.body?.reason, 200);
  if (act === 'verify') { a.status = 'verified'; a.verifiedAt = now(); a.reason = ''; }
  else if (act === 'reject') { a.status = 'rejected'; a.reason = why || 'Details could not be verified.'; }
  else if (act === 'suspend') { a.status = 'suspended'; a.reason = why || 'Suspended after complaints.'; }
  else if (act === 'unsuspend') { a.status = 'verified'; a.reason = ''; }
  else if (act === 'limit') { a.dailyLimit = Math.max(10, Math.min(1e6, Math.round(Number(req.body?.limit) || BM_LIMITS.live.perDay))); }
  else if (act === 'template') { const t = db.bmTemplates.find((x) => x.id === req.body?.templateId && x.bizId === o.id); if (!t) return res.status(404).json({ error: 'Template not found.' }); t.status = req.body?.ok ? 'approved' : 'rejected'; t.reason = req.body?.ok ? '' : why || 'Not allowed.'; }
  else return res.status(400).json({ error: 'Unknown action.' });
  audit(req.admin, 'bm-' + act, o.id, o.business.name + (why ? ' – ' + why : '')); save();
  pushTo(o, { type: 'msg', title: '🏢 Business messaging', body: act === 'verify' || act === 'unsuspend' ? '✅ ' + o.business.name + ' is verified. You can now create live API keys.' : act === 'template' ? 'A template was ' + (req.body?.ok ? 'approved' : 'rejected') + '.' : 'Status: ' + a.status + (a.reason ? ' – ' + a.reason : ''), tag: 'bm-admin', url: '/?biz=' + o.id }).catch(() => {});
  res.json({ ok: true, status: a.status });
});
// ---- developer docs ----
app.get('/developers', (req, res) => {
  const base = (process.env.PUBLIC_URL || 'https://maataapp.com').replace(/\/$/, '');
  res.type('html').send(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Maata Business Messaging API</title><style>body{margin:0;font-family:system-ui,-apple-system,'Segoe UI',sans-serif;color:#13201E;background:#F6F8F7;line-height:1.55}main{max-width:820px;margin:0 auto;padding:24px 18px 60px}h1{color:#0F7A6E;margin:0 0 6px}h2{margin-top:30px;border-top:1px solid #DDE4E2;padding-top:18px}pre{background:#0E1B1A;color:#D7F5EE;padding:14px;border-radius:12px;overflow:auto;font-size:13px}code{background:#E6EFEC;padding:1px 5px;border-radius:5px}table{border-collapse:collapse;width:100%;font-size:14px}td,th{border-bottom:1px solid #DDE4E2;padding:7px 6px;text-align:left;vertical-align:top}.tip{background:#E6F4F1;border-left:4px solid #0F7A6E;padding:10px 14px;border-radius:8px}</style></head><body><main>
<h1>🏢 Maata Business Messaging API</h1><p>Send messages from your software (school, bank, hospital, office, shop) to customers in the Maata app. Version 1.</p>
<div class="tip">1) Maata Business → More → <b>Business messaging API</b> → apply for verification. 2) Create a <b>test key</b> (<code>mk_test_…</code>) and try it on your own phone. 3) After verification, create a <b>live key</b> (<code>mk_live_…</code>). Keep keys on your server only — never in an app or web page.</div>
<h2>Send a message</h2><pre>curl -X POST ${base}/v1/messages \\
  -H "Authorization: Bearer mk_live_xxx" -H "Content-Type: application/json" \\
  -d '{"to":"9876543210","template":"fee_reminder","variables":{"student_name":"Ravi","amount":"₹2,500","due_date":"20 Oct"},"idempotency_key":"fee-2026-10-ravi"}'</pre>
<p>Response <code>202</code>: <code>{"id":"msg_…","status":"accepted",…}</code>. Optional: <code>"send_at":"2026-10-20T09:00:00+05:30"</code> to schedule. Free text (<code>"text":"…"</code> instead of a template) is allowed only within 24 hours after the customer replied.</p>
<h2>Message states</h2><p><code>accepted</code> / <code>queued</code> → <code>sent</code> → <code>delivered</code> (reached the phone) → <code>read</code>, or <code>failed</code>. Check with <code>GET /v1/messages/{id}</code>.</p>
<h2>Other endpoints</h2><table><tr><th>GET /v1/templates</th><td>Your templates, status and variables.</td></tr><tr><th>GET /v1/usage</th><td>Today's count and limits.</td></tr><tr><th>GET /v1/customers/{phone}</th><td>Is the person on Maata, and can you message them?</td></tr></table>
<h2>Consent rules</h2><ul><li><b>Transactional</b> (fees, bills, appointments, orders): the customer has a relationship with you or opted in.</li><li><b>Marketing</b>: only customers who chose "All messages" for you.</li><li>Customers can choose All / Important only / None, reply STOP, or report you. Share your opt-in link: <code>${base}/?optin=YOUR_BUSINESS_ID</code></li><li>Templates must not contain OTPs, passwords, PINs or card numbers.</li></ul>
<h2>Webhooks</h2><p>Add an https address in the dashboard. Maata POSTs JSON events: <code>message.delivered</code>, <code>message.read</code>, <code>message.received</code> (customer reply), <code>customer.opted_in</code>, <code>customer.opted_out</code>, <code>test.ping</code>. Failed deliveries are retried (10 s, 1 min, 5 min, 30 min, 2 h).</p>
<p>Check the signature: header <code>X-Maata-Signature: t=TIMESTAMP,v1=HEX</code> where HEX = HMAC-SHA256(your <code>whsec_…</code> secret, <code>TIMESTAMP + "." + raw body</code>). Reject if older than 5 minutes.</p>
<pre>// Node.js
const [t, v1] = sig.split(',').map(s => s.split('=')[1]);
const ok = crypto.createHmac('sha256', SECRET).update(t + '.' + rawBody).digest('hex') === v1;</pre>
<h2>Errors</h2><table>${Object.entries(BM_ERR).map(([k, v]) => '<tr><th>' + k + '</th><td>' + v + '</td></tr>').join('')}</table>
<h2>Limits</h2><p>Test keys: ${BM_LIMITS.test.perMin}/min, ${BM_LIMITS.test.perDay}/day, only to your own team. Live keys: ${BM_LIMITS.live.perMin}/min, ${BM_LIMITS.live.perDay}/day (can be raised).</p>
</main></body></html>`);
});

io.on('connection', (socket) => {
  if (socket.isAdmin) {
    socket.join('admins');
    socket.emit('live', liveNow());
    return;
  }
  const me = socket.user.id;
  const room = (id) => 'user:' + id;
  socket.join(room(me));
  markActive(socket.user); // counts the days people use Maata (for referral coins)
  socket.on('secure:hello', (p) => { if (secDevOk(p?.dev)) socket.secDev = p.dev; });
  for (const g of db.groups) if (gMember(g, me)) socket.join('grp:' + g.id);
  for (const c of db.channels) if (!c.removed && (c.followers || []).includes(me)) socket.join('ch:' + c.id);

  const wasOnline = isOnline(me);
  online.set(me, (online.get(me) || 0) + 1);
  const waiting = pendingRings.get(me);
  if (waiting) {
    clearInterval(waiting.timer);
    fcmTo(socket.user, { type: 'call_end', callId: waiting.callId, answered: '1' }, 30).catch(() => {});
    if (waiting.until > now() && activeCalls.has(waiting.callId)) {
      setTimeout(() => { socket.emit('call:incoming', waiting.offer); for (const c of waiting.ice) socket.emit('call:ice', c); io.to('user:' + waiting.from).emit('call:ringing', { callId: waiting.callId }); }, 1200);
    }
    pendingRings.delete(me);
  }
  if (!wasOnline) toWatchers(me, 'presence', { userId: me, online: true });
  pushLive();

  const bySender = {};
  for (const m of db.messages) {
    if (m.to === me && m.status === 'sent') { m.status = 'delivered'; m.deliveredAt = now(); (bySender[m.from] = bySender[m.from] || []).push(m.id); }
  }
  if (Object.keys(bySender).length) {
    save();
    for (const [sender, ids] of Object.entries(bySender)) io.to(room(sender)).emit('msg:status', { ids, status: 'delivered' });
  }

  socket.on('msg:send', (p, ack) => {
    const reply = (x) => typeof ack === 'function' && ack(x);
    const ok = canMessage(socket.user, String(p?.to || ''));
    if (ok.error) return reply({ error: ok.error });
    const type = ['contact', 'poll', 'event', 'location', 'gif', 'sticker', 'product', 'reel'].includes(p?.type) ? p.type : 'text';
    const str = (v, n) => String(v ?? '').trim().slice(0, n);
    let fields;
    if (type === 'text') {
      const text = str(p?.text, 4000); if (!text) return reply({ error: 'Message not sent.' });
      fields = { type, text };
    } else if (type === 'reel') {
      const rl = db.reels.find((x) => x.id === String(p?.reel?.id || '') && !x.removed);
      if (!rl) return reply({ error: 'Reel not found.' });
      fields = { type, reel: { id: rl.id, caption: (rl.caption || '').slice(0, 120), byName: (userById(rl.by) || {}).name || 'Maata user', hasThumb: !!rl.thumb }, text: clip(p?.text, 500) };
    } else if (type === 'product') {
      const pr = db.products.find((x) => x.id === String(p?.product?.id || ''));
      if (!pr) return reply({ error: 'Product not found.' });
      fields = { type, product: { id: pr.id, ownerId: pr.ownerId, name: pr.name, price: pr.price, hasPhoto: !!pr.photo, photoV: pr.photoV || 0 }, text: clip(p?.text, 1000) };
    } else if (type === 'gif') {
      const g = p?.gif || {}, okUrl = (u) => typeof u === 'string' && /^https:\/\/media\d*\.tenor\.com\/[\w\-./%]+$/.test(u) && u.length < 400;
      if (!okUrl(g.url) || (g.preview && !okUrl(g.preview))) return reply({ error: 'GIF not sent.' });
      fields = { type, gif: { url: g.url, preview: g.preview || g.url, w: Math.min(2000, Number(g.w) || 0), h: Math.min(2000, Number(g.h) || 0) } };
    } else if (type === 'sticker') {
      const id = String(p?.sticker?.id || '');
      if (!/^[a-z0-9-]{1,30}$/.test(id)) return reply({ error: 'Sticker not sent.' });
      fields = { type, sticker: { id } };
    } else if (type === 'contact') {
      const name = str(p?.contact?.name, 60), phone = str(p?.contact?.phone, 20).replace(/[^\d+ ]/g, '');
      if (!name || phone.replace(/\D/g, '').length < 6) return reply({ error: 'Choose a contact with a phone number.' });
      fields = { type, contact: { name, phone } };
    } else if (type === 'poll') {
      const question = str(p?.poll?.question, 200);
      const options = (Array.isArray(p?.poll?.options) ? p.poll.options : []).map((o) => str(o, 100)).filter(Boolean).slice(0, 12);
      if (!question || options.length < 2) return reply({ error: 'A poll needs a question and at least 2 options.' });
      fields = { type, poll: { question, options, multi: !!p.poll.multi, votes: {} } };
    } else if (type === 'event') {
      const title = str(p?.event?.title, 120), date = str(p?.event?.date, 10), time = str(p?.event?.time, 5);
      if (!title || !/^\d{4}-\d{2}-\d{2}$/.test(date)) return reply({ error: 'An event needs a name and a date.' });
      fields = { type, event: { title, date, time: /^\d{2}:\d{2}$/.test(time) ? time : '', place: str(p.event.place, 200), note: str(p.event.note, 500), rsvp: {} } };
    } else {
      const lat = Number(p?.location?.lat), lng = Number(p?.location?.lng);
      if (!(Math.abs(lat) <= 90 && Math.abs(lng) <= 180)) return reply({ error: 'Could not read your location.' });
      const live = !!p.location.live;
      const mins = [15, 60, 480].includes(Number(p.location.minutes)) ? Number(p.location.minutes) : 15;
      fields = { type, location: { lat, lng, acc: Math.round(Number(p.location.acc) || 0), live, ...(live ? { until: now() + mins * 60000, updatedAt: now(), ended: false } : {}) } };
    }
    if (p?.replyTo) {
      const q = db.messages.find((x) => x.id === String(p.replyTo));
      if (q && (ok.group ? q.group === ok.group.id : ((q.from === me && q.to === ok.target.id) || (q.from === ok.target.id && q.to === me)))) {
        fields.replyTo = { id: q.id, from: q.from, type: q.type || 'text', text: String(q.deleted ? '' : q.text || q.file?.name || q.contact?.name || q.poll?.question || q.event?.title || '').slice(0, 120) };
      }
    }
    const m = deliverMessage(socket.user, ok.target, fields, socket.id);
    reply({ message: m });
    if (!ok.group) maybeAutoReply(socket.user, ok.target, m).catch(() => {});
  });

  // ----- message actions: react, pin, star, delete, forward -----
  socket.on('msg:edit', (p, ack) => {
    const reply = (x) => typeof ack === 'function' && ack(x);
    const m = myMsg(p?.id);
    if (!m || m.from !== me || m.deleted || (m.type || 'text') !== 'text' || m.ai) return reply({ error: 'This message cannot be edited.' });
    if (now() - m.ts > 15 * 60000) return reply({ error: 'Messages can be edited for 15 minutes after sending.' });
    const text = String(p.text || '').trim().slice(0, 4000); if (!text) return reply({ error: 'Message cannot be empty.' });
    if (text !== m.text) { m.text = text; m.editedAt = now(); save(); emitUpdate(m); }
    reply({ message: m });
  });
  socket.on('msg:opened', (p) => {
    const m = myMsg(p?.id); if (!m || !m.viewOnce || m.from === me) return;
    m.openedBy = m.openedBy || {}; if (m.openedBy[me]) return;
    m.openedBy[me] = now(); save(); emitUpdate(m);
  });
  socket.on('msg:react', (p) => {
    const m = myMsg(p?.id); if (!m || m.deleted) return;
    const emoji = String(p.emoji || '').slice(0, 16);
    m.reactions = m.reactions || {};
    if (emoji && m.reactions[me] !== emoji) m.reactions[me] = emoji; else delete m.reactions[me];
    save(); emitUpdate(m);
  });
  socket.on('msg:pin', (p) => {
    const m = myMsg(p?.id); if (!m || m.deleted) return;
    if (p.pin) {
      const other = m.from === me ? m.to : m.from;
      const pins = db.messages.filter((x) => x.pinned && ((x.from === me && x.to === other) || (x.from === other && x.to === me)));
      if (pins.length >= 3) { const oldest = pins.sort((a, b) => a.pinnedAt - b.pinnedAt)[0]; oldest.pinned = false; emitUpdate(oldest); }
      Object.assign(m, { pinned: true, pinnedAt: now(), pinnedBy: me });
    } else { m.pinned = false; }
    save(); emitUpdate(m);
  });
  socket.on('msg:star', (p) => {
    const ids = (Array.isArray(p?.ids) ? p.ids : []).map(String).filter((id) => myMsg(id));
    const set = new Set(socket.user.starred || []);
    for (const id of ids) (p.star ? set.add(id) : set.delete(id));
    socket.user.starred = [...set].slice(-1000); save();
    io.to(room(me)).emit('star:update', { ids, star: !!p.star });
  });
  socket.on('msg:delete', (p, ack) => {
    const ids = (Array.isArray(p?.ids) ? p.ids : []).map(String).slice(0, 100);
    const done = [];
    for (const id of ids) {
      const m = myMsg(id); if (!m) continue;
      if (p.everyone) {
        if ((m.from !== me && !(m.group && gAdmin(groupById(m.group), me))) || m.deleted || now() - m.ts > 48 * 3600 * 1000) continue;
        const fileId = m.file && m.file.id;
        for (const k of ['text', 'file', 'contact', 'poll', 'event', 'location', 'reactions', 'replyTo', 'forwarded']) delete m[k];
        Object.assign(m, { type: 'deleted', deleted: true, pinned: false, text: '' });
        if (fileId) releaseFile(fileId);
        emitUpdate(m); done.push(id);
      } else {
        const h = new Set(socket.user.hiddenMsgs || []); h.add(id); socket.user.hiddenMsgs = [...h].slice(-20000);
        io.to(room(me)).emit('msg:hidden', { id }); done.push(id);
      }
    }
    save();
    if (typeof ack === 'function') ack({ done });
  });
  socket.on('msg:forward', (p, ack) => {
    const reply = (x) => typeof ack === 'function' && ack(x);
    const ids = (Array.isArray(p?.ids) ? p.ids : []).map(String).slice(0, 30);
    const tos = [...new Set((Array.isArray(p?.to) ? p.to : []).map(String))].slice(0, 5);
    const src = ids.map(myMsg).filter((m) => m && !m.deleted);
    if (!src.length || !tos.length) return reply({ error: 'Choose a message and a chat.' });
    let sent = 0; const errors = [];
    for (const to of tos) {
      const ok = canMessage(socket.user, to);
      if (ok.error) { errors.push(ok.error); continue; }
      for (const m of src) {
        if (m.viewOnce) continue;
        const f = { type: m.type || 'text', text: m.text || '', forwarded: true };
        if (m.file) f.file = { ...m.file };
        if (m.contact) f.contact = { ...m.contact };
        if (m.poll) f.poll = { question: m.poll.question, options: [...m.poll.options], multi: m.poll.multi, votes: {} };
        if (m.event) f.event = { ...m.event, rsvp: {} };
        if (m.location) f.location = { lat: m.location.lat, lng: m.location.lng, acc: m.location.acc, live: false };
        if (m.gif) f.gif = { ...m.gif };
        if (m.product) f.product = { ...m.product };
        if (m.reel) f.reel = { ...m.reel };
        if (m.sticker) f.sticker = { ...m.sticker };
        deliverMessage(socket.user, ok.target, f, null); sent++;
      }
      const note = String(p.note || '').trim().slice(0, 4000);
      if (note) deliverMessage(socket.user, ok.target, { type: 'text', text: note }, null);
    }
    reply(sent ? { sent } : { error: errors[0] || 'Not forwarded.' });
  });

  socket.on('sticker:send', (p, ack) => {
    const reply = (x) => typeof ack === 'function' && ack(x);
    const ok = canMessage(socket.user, String(p?.to || ''));
    if (ok.error) return reply({ error: ok.error });
    const src = db.messages.find((x) => x.file && x.file.sticker && x.file.id === String(p?.fileId || '') && (x.from === me || x.to === me));
    if (!src) return reply({ error: 'Sticker not found.' });
    reply({ message: deliverMessage(socket.user, ok.target, { type: 'image', text: '', file: { ...src.file } }, socket.id) });
  });

  // Poll votes, event replies, live location updates
  const myMsg = (id) => { const m = db.messages.find((x) => x.id === String(id || '')); return m && (m.from === me || m.to === me || (m.group && gMember(groupById(m.group), me))) ? m : null; };
  socket.on('poll:vote', (p) => {
    const m = myMsg(p?.id); if (!m || m.type !== 'poll') return;
    let picks = (Array.isArray(p.options) ? p.options : []).map(Number).filter((i) => Number.isInteger(i) && i >= 0 && i < m.poll.options.length);
    picks = [...new Set(picks)]; if (!m.poll.multi) picks = picks.slice(0, 1);
    if (picks.length) m.poll.votes[me] = picks; else delete m.poll.votes[me];
    save(); emitUpdate(m);
  });
  socket.on('event:rsvp', (p) => {
    const m = myMsg(p?.id); if (!m || m.type !== 'event') return;
    if (['yes', 'maybe', 'no'].includes(p.answer)) m.event.rsvp[me] = p.answer; else delete m.event.rsvp[me];
    save(); emitUpdate(m);
  });
  let liveSave = 0;
  socket.on('live:update', (p) => {
    const m = myMsg(p?.id); if (!m || m.type !== 'location' || m.from !== me || !m.location.live || m.location.ended) return;
    if (now() > m.location.until) { m.location.ended = true; save(); return emitUpdate(m); }
    const lat = Number(p.lat), lng = Number(p.lng);
    if (!(Math.abs(lat) <= 90 && Math.abs(lng) <= 180)) return;
    Object.assign(m.location, { lat, lng, acc: Math.round(Number(p.acc) || 0), updatedAt: now() });
    if (now() - liveSave > 30000) { liveSave = now(); save(); }
    emitUpdate(m);
  });
  socket.on('live:stop', (p) => {
    const m = myMsg(p?.id); if (!m || m.type !== 'location' || m.from !== me || !m.location.live) return;
    m.location.ended = true; save(); emitUpdate(m);
  });

  socket.on('msg:read', (p) => {
    const from = String(p?.from || '');
    const gid = gidOf(from);
    if (gid) {
      const g = groupById(gid); if (!gMember(g, me)) return;
      const bySender = {};
      for (const m of db.messages) {
        if (m.group !== gid || m.from === me || m.type === 'system') continue;
        m.readBy = m.readBy || {};
        if (m.readBy[me]) continue;
        m.readBy[me] = now();
        if (m.status !== 'read' && g.members.every((x) => x.id === m.from || m.readBy[x.id])) { m.status = 'read'; m.readAt = now(); (bySender[m.from] = bySender[m.from] || []).push(m.id); }
      }
      save();
      for (const [s2, ids] of Object.entries(bySender)) io.to(room(s2)).emit('msg:status', { ids, status: 'read' });
      return;
    }
    const ids = [];
    for (const m of db.messages) if (m.from === from && m.to === me && m.status !== 'read') { m.status = 'read'; m.readAt = now(); if (!m.deliveredAt) m.deliveredAt = m.readAt; ids.push(m.id); }
    if (ids.length) { save(); io.to(room(from)).emit('msg:status', { ids, status: 'read' }); }
  });

  // ----- WebRTC call signaling + call records -----
  const clean = (p) => ({ callId: String(p.callId || '').slice(0, 64), kind: p.kind === 'video' ? 'video' : 'voice', sdp: p.sdp, candidate: p.candidate, from: me, fromName: socket.user.name, lang: socket.user.lang || null });
  const valid = (p) => p && typeof p.to === 'string' && p.to !== me && p.callId;
  const recFor = (p) => { const r = activeCalls.get(String(p.callId)); return r && (r.from === me || r.to === me) ? r : null; };

  socket.on('call:offer', (p) => {
    if (!valid(p)) return;
    if (!featOn(p.kind === 'video' ? 'videoCalls' : 'voiceCalls')) return socket.emit('call:unavailable', { callId: p.callId, reason: OFF_MSG });
    const target = userById(p.to);
    if (!target || target.status === 'blocked' || blockedOf(target).includes(me) || blockedOf(socket.user).includes(p.to)) return socket.emit('call:unavailable', { callId: p.callId });
    const callId = String(p.callId).slice(0, 64);
    if (!db.calls.some((c) => c.id === callId)) {
      const rec = { id: callId, from: me, to: p.to, kind: p.kind === 'video' ? 'video' : 'voice', status: 'ringing', startedAt: now(), duration: 0 };
      db.calls.push(rec); activeCalls.set(callId, rec); save(); pushLive();
      if (!isOnline(p.to) && !hasPush(target)) finishCall(rec, 'unavailable');
    }
    if (!isOnline(p.to)) {
      if (!hasPush(target)) return socket.emit('call:unavailable', { callId: p.callId });
      // their app is closed: wake the phone and keep the offer until they open Maata (40 s)
      const saved = contactsOf(target).find((c) => c.id === me), who = saved ? saved.name : socket.user.name + ' (' + socket.user.phone + ')';
      const ringPush = { type: 'call', callId, kind: p.kind === 'video' ? 'video' : 'voice', title: (p.kind === 'video' ? '🎥 ' : '📞 ') + who + ' is calling…', body: 'Maata ' + (p.kind === 'video' ? 'video' : 'voice') + ' call · tap Answer', tag: 'call-' + callId };
      const pr = { offer: clean(p), ice: [], until: now() + 40000, callId, from: me, rings: 0 };
      // "Ring": repeat the notification (sound + vibration) every 4 s until answered, cancelled or 40 s pass
      const ringOnce = () => { const cur = pendingRings.get(p.to); if (!cur || cur.callId !== callId || cur.until < now() || cur.rings >= 10) { clearInterval(pr.timer); return; } cur.rings++; pushTo(target, ringPush, { ttl: 30, urgency: 'high', webOnly: cur.rings > 1 }); };
      pendingRings.set(p.to, pr); ringOnce(); pr.timer = setInterval(ringOnce, 4000);
      socket.emit('call:waking', { callId });
      return;
    }
    io.to(room(p.to)).emit('call:incoming', clean(p));
  });
  socket.on('call:answer', (p) => {
    if (!valid(p)) return;
    const rec = recFor(p);
    if (rec && rec.to === me && !rec.answeredAt) { rec.answeredAt = now(); rec.status = 'answered'; save(); pushLive(); }
    io.to(room(p.to)).emit('call:answered', clean(p));
    socket.to(room(me)).emit('call:taken-elsewhere', { callId: p.callId });
  });
  socket.on('call:reject', (p) => {
    if (!valid(p)) return;
    finishCall(recFor(p), 'declined');
    io.to(room(p.to)).emit('call:rejected', clean(p));
    socket.to(room(me)).emit('call:taken-elsewhere', { callId: p.callId });
  });
  socket.on('call:busy', (p) => { if (!valid(p)) return; finishCall(recFor(p), 'busy'); io.to(room(p.to)).emit('call:busy', clean(p)); });
  socket.on('call:ice', (p) => {
    if (!valid(p)) return;
    const pr = pendingRings.get(p.to); if (pr && pr.callId === String(p.callId) && !isOnline(p.to)) { if (pr.ice.length < 60) pr.ice.push(clean(p)); return; }
    io.to(room(p.to)).emit('call:ice', clean(p));
  });
  // Live call translation: the speaker's browser turns speech into text, the server translates it
  // into the listener's language, and the listener's browser shows subtitles and speaks it aloud.
  // Languages always come from each person's own Settings (stored on the server), so a Telugu speaker's
  // words always reach a Tamil listener in Tamil, whatever phone or browser either of them uses.
  async function relayCaption(rec, toId, text, heardLang, ready) {
    const to = userById(toId)?.lang || 'en';
    const from = heardLang || scriptLang(text, socket.user.lang || 'en'); // the language REALLY spoken (English words while the setting is Telugu → English)
    const translated = from === to ? text : ready || await translate(text, from, to);
    rec.translated = (rec.translated || 0) + 1;
    const out = { callId: rec.id, text, translated: translated || text, fromLang: from, toLang: to, ok: !!translated || from === to, same: from === to };
    io.to(room(toId)).emit('call:caption', out);
    socket.emit('call:caption:self', out);
  }
  // the LISTENER asks: "send me your voice, I want it in my language" → the other phone starts sending short voice clips
  socket.on('call:tr', (p) => {
    if (!valid(p)) return; const rec = recFor(p); if (!rec) return;
    io.to(room(p.to)).emit('call:tr', { from: me, callId: String(p.callId).slice(0, 64), on: !!p.on, lang: socket.user.lang || 'en' });
  });
  socket.on('call:caption', async (p) => {
    if (!valid(p)) return;
    const rec = recFor(p);
    if (!rec || rec.status !== 'answered') return;
    const text = String(p.text || '').trim().slice(0, 500);
    if (text) await relayCaption(rec, p.to, text);
  });
  let sttBusy = 0, sttTimes = [];
  socket.on('call:audio', async (p) => {
    if (!valid(p) || typeof p.audio !== 'string' || p.audio.length > 1_200_000) return;
    const rec = recFor(p);
    if (!rec || rec.status !== 'answered') return;
    sttTimes = sttTimes.filter((t) => now() - t < 60_000);
    if (sttBusy >= 3 || sttTimes.length >= 40) return socket.emit('call:stt', { callId: rec.id, status: 'busy' });
    const toLang = userById(p.to)?.lang || 'en', key = me + '>' + p.to; rec.trSame = rec.trSame || {};
    const st = rec.trSame[key] || (rec.trSame[key] = { n: 0, until: 0 });
    if (now() < st.until) return; // the person has been speaking the listener's own language: no need to translate for now
    sttBusy++; sttTimes.push(now());
    try {
      const r = SPEECH_KEY ? await speechToText(p.audio, socket.user.lang || 'en') : await aiHearTranslate(p.audio, socket.user.lang || 'en', toLang);
      rec.sttClips = (rec.sttClips || 0) + 1;
      if (r.error) return socket.emit('call:stt', { callId: rec.id, status: r.error });
      if (!r.text) return socket.emit('call:stt', { callId: rec.id, status: 'empty' });
      if (r.lang && r.lang === toLang) { if (++st.n >= 3) { st.until = now() + 20000; st.n = 0; } } else st.n = 0;
      await relayCaption(rec, p.to, r.text.slice(0, 500), r.lang, r.lang && r.lang !== toLang ? r.translated : null);
    } finally { sttBusy--; }
  });

  // camera on/off during a 1-to-1 call, so the other side shows the photo + "Camera off" instead of a black screen
  socket.on('call:state', (p) => {
    const to = String(p?.to || ''); const t = userById(to); if (!t || blockedOf(t).includes(me)) return;
    io.to('user:' + to).emit('call:state', { from: me, callId: String(p?.callId || '').slice(0, 64), cam: p?.cam !== false });
  });
  socket.on('call:end', (p) => {
    if (!valid(p)) return;
    const pr = pendingRings.get(p.to);
    if (pr && pr.callId === String(p.callId)) { clearInterval(pr.timer); pendingRings.delete(p.to); const t = userById(p.to); fcmTo(t, { type: 'call_end', callId: p.callId }, 60).catch(() => {}); const saved = t && contactsOf(t).find((c) => c.id === me); pushTo(t, { type: 'missed', title: 'Missed call · Maata', body: (saved ? saved.name : socket.user.name) + ' called you', tag: 'call-' + p.callId, url: '/#chat=' + me }); }
    finishCall(recFor(p), 'missed'); io.to(room(p.to)).emit('call:ended', clean(p));
  });

  // ----- group calls (everyone connects to everyone; up to GCALL_MAX people) -----
  const confNames = (c) => (c.conf ? Object.fromEntries([...c.allowed].map((id) => [id, (userById(id) || {}).name || 'Maata user'])) : undefined);
  function gcallState(c) { io.to(c.conf ? 'gc:' + c.id : 'grp:' + c.gid).emit('gcall:state', { gid: 'g:' + c.gid, callId: c.id, kind: c.kind, participants: [...c.parts.keys()], names: confNames(c) }); }
  function gcallJoin(c) {
    const peers = [...c.parts.keys()].filter((x) => x !== me);
    c.parts.set(me, now()); c.ever.add(me); socket.join('gc:' + c.id); socket.gcall = c.id;
    socket.emit('gcall:joined', { gid: 'g:' + c.gid, callId: c.id, kind: c.kind, peers, max: GCALL_MAX, conf: !!c.conf, names: confNames(c), ...(LIVEKIT ? { sfu: { url: LK_URL, token: livekitToken(socket.user, 'maata-' + c.id) } } : {}) });
    gcallState(c); pushLive();
  }
  function gcallLeave(callId) {
    const c = [...gcalls.values()].find((x) => x.id === callId); if (!c || !c.parts.has(me)) return;
    c.parts.delete(me); socket.leave('gc:' + c.id); socket.gcall = null;
    io.to('gc:' + c.id).emit('gcall:peer-left', { callId: c.id, id: me });
    if (!c.parts.size && c.conf) { gcalls.delete(c.gid); feed('call', 'Conference ' + c.kind + ' call: ' + c.ever.size + ' people'); }
    else if (!c.parts.size) {
      gcalls.delete(c.gid);
      const rec = { id: c.id, from: c.by, to: 'g:' + c.gid, group: c.gid, kind: c.kind, startedAt: c.startedAt, endedAt: now(), participants: [...c.ever] };
      if (c.ever.size > 1) Object.assign(rec, { status: 'completed', answeredAt: c.startedAt, duration: Math.round((now() - c.startedAt) / 1000) }); else Object.assign(rec, { status: 'missed', duration: 0 });
      db.calls.push(rec); save();
      const g = groupById(c.gid);
      if (g) for (const x of g.members) { const u = userById(x.id); if (u) io.to('user:' + x.id).emit('call:log', callView(u, rec)); }
      io.to('grp:' + c.gid).emit('gcall:state', { gid: 'g:' + c.gid, callId: c.id, participants: [] });
      feed('call', 'Group ' + c.kind + ' call in ' + (g ? g.name : 'a group') + ': ' + c.ever.size + ' people');
    } else gcallState(c);
    pushLive();
  }
  socket.on('gcall:start', (p) => {
    if (!featOn('groupCalls')) return socket.emit('gcall:ended', { reason: OFF_MSG });
    const g = groupById(gidOf(String(p?.gid || ''))); if (!gMember(g, me)) return;
    if (socket.gcall) gcallLeave(socket.gcall);
    let c = gcalls.get(g.id);
    if (!c) {
      c = { id: newId(), gid: g.id, kind: p.kind === 'video' ? 'video' : 'voice', startedAt: now(), by: me, parts: new Map(), ever: new Set() };
      gcalls.set(g.id, c);
      socket.to('grp:' + g.id).emit('gcall:ring', { gid: 'g:' + g.id, callId: c.id, kind: c.kind, from: me, fromName: socket.user.name, groupName: g.name });
    }
    if (c.parts.size >= GCALL_MAX) return socket.emit('gcall:full', { callId: c.id, max: GCALL_MAX });
    gcallJoin(c);
  });
  socket.on('gcall:join', (p) => {
    const gk = gidOf(String(p?.gid || '')), c = gcalls.get(gk), g = groupById(gk);
    if (!c || !(c.conf ? c.allowed.has(me) : gMember(g, me))) return socket.emit('gcall:ended', { callId: p?.callId });
    if (c.parts.has(me)) return;
    if (c.parts.size >= GCALL_MAX) return socket.emit('gcall:full', { callId: c.id, max: GCALL_MAX });
    if (socket.gcall) gcallLeave(socket.gcall);
    gcallJoin(c);
  });
  socket.on('gcall:signal', (p) => {
    const c = [...gcalls.values()].find((x) => x.id === p?.callId);
    if (!c || !c.parts.has(me) || !c.parts.has(String(p.to))) return;
    io.to('user:' + p.to).emit('gcall:signal', { callId: c.id, from: me, data: p.data });
  });
  socket.on('gcall:leave', (p) => gcallLeave(String(p?.callId || '')));

  // ---------- Conference call (like "Add call / Merge" on a normal phone): people chosen by the caller, no group needed ----------
  const mayCall = (uid) => { const t = userById(uid); return t && t.status !== 'blocked' && !blockedOf(t).includes(me) && !blockedOf(socket.user).includes(uid); };
  function confRing(c, uid) {
    if (!isOnline(uid)) return false;
    io.to('user:' + uid).emit('gcall:ring', { gid: 'g:' + c.gid, callId: c.id, kind: c.kind, from: me, fromName: socket.user.name, groupName: 'Conference call', conf: true, names: confNames(c) });
    return true;
  }
  socket.on('conf:create', (p, reply) => {
    if (!featOn('conference')) return reply?.({ error: OFF_MSG });
    const kind = p?.kind === 'video' ? 'video' : 'voice', moveId = String(p?.move || '');
    const invite = [...new Set((Array.isArray(p?.invite) ? p.invite : []).map(String))].filter((id) => id !== me && mayCall(id)).slice(0, GCALL_MAX - 1);
    if (!invite.length) return reply?.({ error: 'Choose someone to add.' });
    if (socket.gcall) gcallLeave(socket.gcall);
    const id = newId(), c = { id, gid: 'conf-' + id, conf: true, kind, startedAt: now(), by: me, parts: new Map(), ever: new Set(), allowed: new Set([me, ...invite]) };
    gcalls.set(c.gid, c);
    const offline = [];
    for (const uid of invite) {
      if (uid === moveId) io.to('user:' + uid).emit('conf:move', { gid: 'g:' + c.gid, callId: c.id, kind, names: confNames(c) }); // already talking to us: joins without ringing
      else if (!confRing(c, uid)) offline.push((userById(uid) || {}).name || 'Someone');
    }
    reply?.({ ok: true, gid: 'g:' + c.gid, callId: c.id, names: confNames(c), offline });
  });
  socket.on('conf:invite', (p, reply) => {
    const c = [...gcalls.values()].find((x) => x.id === String(p?.callId || ''));
    if (!c || !c.conf || !c.parts.has(me)) return reply?.({ error: 'Call not found.' });
    const uid = String(p?.userId || '');
    if (!mayCall(uid)) return reply?.({ error: 'You cannot call this person.' });
    if (c.allowed.size >= GCALL_MAX) return reply?.({ error: 'The call is full (' + GCALL_MAX + ' people).' });
    c.allowed.add(uid); gcallState(c);
    if (!confRing(c, uid)) return reply?.({ error: ((userById(uid) || {}).name || 'They') + ' is not online right now.' });
    reply?.({ ok: true });
  });
  socket.on('disconnect', () => { if (socket.gcall) gcallLeave(socket.gcall); });

  socket.on('disconnect', () => {
    const n = (online.get(me) || 1) - 1;
    if (n <= 0) {
      online.delete(me);
      const u = userById(me); if (u) { u.lastSeenAt = now(); save(); }
      toWatchers(me, 'presence', { userId: me, online: false });
      for (const rec of [...activeCalls.values()]) if (rec.from === me || rec.to === me) finishCall(rec, 'missed');
    } else online.set(me, n);
    pushLive();
  });
});

loadDb()
  .then(() => {
    let fixed = 0; const dups = [];
    for (const u of db.users) {
      const m = cleanMobile(u.phone); if (!m || m === u.phone) continue;
      const other = db.users.find((x) => x !== u && x.phone === m);
      if (other) { dups.push(u.name + ' ' + u.phone + ' = ' + other.name + ' ' + m); u.duplicateOf = other.id; continue; }
      u.phone = m; fixed++;
    }
    if (fixed) { save(); console.log('[phones] cleaned ' + fixed + ' numbers to 10 digits'); }
    if (dups.length) console.log('[phones] same number registered twice (delete one in the admin panel): ' + dups.join(' | '));
  })
  .then(() => { cleanStatuses(); cleanExpiredMessages(); return setupPush().catch((e) => console.error('[push] setup failed', e.message)); })
  .then(() => server.listen(PORT, () => console.log(`Maata running on http://localhost:${PORT}  (admin: /admin)`)))
  .catch((e) => {
    console.error('[db] Could not connect to MongoDB. Check MONGODB_URI, the database password, and Network Access (allow 0.0.0.0/0).');
    console.error(e.message);
    process.exit(1);
  });
