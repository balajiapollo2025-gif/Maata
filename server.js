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
const COLLS = ['users', 'messages', 'logins', 'calls', 'reports', 'announcements', 'audit', 'statuses', 'groups', 'aichats', 'i18n', 'products', 'ads', 'communities', 'channels', 'channelPosts', 'meta'];
const SORT_BY = { users: 'createdAt', messages: 'ts', logins: 'ts', calls: 'startedAt', reports: 'ts', announcements: 'ts', audit: 'ts', statuses: 'createdAt', groups: 'createdAt', aichats: 'ts', products: 'createdAt', ads: 'createdAt', communities: 'createdAt', channels: 'createdAt', channelPosts: 'ts', meta: 'createdAt' };
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
  return u;
}
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
  return ({ image: '📷 Photo', video: '🎥 Video', audio: m.file && m.file.voice ? '🎤 Voice message' : '🎵 Audio', file: '📄 Document', contact: '👤 Contact', poll: '📊 Poll', event: '📅 Event', location: '📍 Location', gif: '🎞️ GIF', sticker: '💟 Sticker', product: '🛍️ Product' })[m.type] || 'New message';
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
const CACHE = 'maata-shell-v2';
self.addEventListener('install', (e) => { e.waitUntil(caches.open(CACHE).then((c) => c.addAll(['/', '/icons/icon-192.png', '/icons/badge-96.png']).catch(() => {}))); self.skipWaiting(); });
self.addEventListener('activate', (e) => { e.waitUntil(caches.keys().then((ks) => Promise.all(ks.filter((k) => k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim())); });
// Pages: always try the network first (so updates show at once), fall back to the saved copy when offline
self.addEventListener('fetch', (e) => {
  const r = e.request;
  if (r.method !== 'GET' || new URL(r.url).origin !== location.origin) return;
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

app.get(['/', '/index.html'], (req, res) => res.sendFile(path.join(PAGES, 'index.html')));
app.get(['/admin', '/admin.html'], (req, res) => res.sendFile(path.join(PAGES, 'admin.html')));
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
app.post('/api/register', rateLimit, async (req, res) => {
  const name = String(req.body?.name || '').trim().slice(0, 40);
  const phone = cleanMobile(req.body?.phone);
  const password = String(req.body?.password || '');
  if (name.length < 2) return res.status(400).json({ error: 'Name must be at least 2 characters.' });
  if (!phone) return res.status(400).json({ error: 'Enter your 10-digit mobile number (without +91 or 0).' });
  if (password.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters.' });
  if (findByMobile(phone)) return res.status(409).json({ error: 'This number is already registered. Log in instead.' });
  const u = { id: newId(), name, phone, passHash: await bcrypt.hash(password, 10), createdAt: now(), status: 'active', tokenVersion: 0 };
  db.users.push(u);
  recordLogin(u, req, 'signup');
  save();
  res.json({ token: sign(u), user: publicUser(u) });
});

app.post('/api/login', rateLimit, async (req, res) => {
  const phone = cleanMobile(req.body?.phone);
  const password = String(req.body?.password || '');
  if (!phone) return res.status(400).json({ error: 'Enter your 10-digit mobile number.' });
  const u = db.users.find((x) => x.phone === phone) || findByMobile(phone);
  if (!u || !(await bcrypt.compare(password, u.passHash))) return res.status(401).json({ error: 'Wrong mobile number or password.' });
  if (u.status === 'blocked') return res.status(403).json({ error: 'This account is blocked. Contact Maata support.' });
  if (u.twoStep) { const ticket = crypto.randomBytes(18).toString('base64url'); pinTickets.set(ticket, { uid: u.id, t: now(), tries: 0 }); return res.json({ needPin: true, ticket, hint: u.twoStep.hint || '' }); }
  recordLogin(u, req, 'login');
  save();
  res.json({ token: sign(u), user: publicUser(u) });
});

app.get('/api/me', auth, (req, res) => res.json(publicUser(req.user)));

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
  serverSpeech: !!SPEECH_KEY,
  serverVoice: !!(process.env.GOOGLE_TTS_KEY || process.env.GOOGLE_TRANSLATE_KEY),
  translate: GOOGLE_TRANSLATE_KEY ? 'google' : 'free',
}));

const TTS_LANG = { te: 'te-IN', hi: 'hi-IN', en: 'en-IN', ta: 'ta-IN', kn: 'kn-IN', ml: 'ml-IN', mr: 'mr-IN', bn: 'bn-IN', gu: 'gu-IN', pa: 'pa-IN', ur: 'ur-IN', or: 'or-IN' };
const ttsCache = new Map();
const ttsHits = new Map();
app.post('/api/tts', auth, async (req, res) => {
  if (!TTS_KEY) return res.status(501).json({ error: 'Server voice is not set up.' });
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
app.post('/api/translate', auth, async (req, res) => {
  const list = (trHits.get(req.user.id) || []).filter((t) => now() - t < 60_000);
  list.push(now()); trHits.set(req.user.id, list);
  if (list.length > 60) return res.status(429).json({ error: 'Too many translations. Wait a minute.' });
  const to = String(req.body?.to || req.user.lang || 'en');
  const from = String(req.body?.from || '');
  const out = await translate(req.body?.text, from, to);
  if (!out) return res.status(502).json({ error: 'Translation is not available right now.' });
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
      other: g ? { id: 'g:' + g.id, isGroup: true, name: g.name, displayName: g.name, phone: '', photo: !!g.photo, photoV: g.photoV || 0 } : { id: 'g:' + c.group, name: 'Group', displayName: 'Group', phone: '', deleted: true } };
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
async function deleteChatFile(id) { if (isR2(id)) return r2Delete(id); try { if (chatBucket) await chatBucket.delete(id); else fs.unlinkSync(path.join(CHAT_DIR, id)); } catch { /* gone */ } }
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
    photo: !!g.photo, photoV: g.photoV || 0, memberCount: g.members.length, myRole: me2 ? me2.role : null,
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
async function gemini(system, contents, { json = false, maxTokens = 600, temperature = 0.7 } = {}) {
  if (!GEMINI_KEY) return { error: 'not-configured' };
  await discoverModels();
  const body = JSON.stringify({ system_instruction: { parts: [{ text: system }] }, contents,
    generationConfig: { temperature, maxOutputTokens: maxTokens, ...(json ? { responseMimeType: 'application/json' } : {}) } });
  // the model that worked last time first, then the others, then spares; models that were busy a moment ago go last
  const all = [...new Set([geminiModelOk, ...GEMINI_MODELS, ...GEMINI_SPARES].filter(Boolean))];
  const models = [...all.filter((m) => !(busyUntil.get(m) > now())), ...all.filter((m) => busyUntil.get(m) > now())].slice(0, 6);
  const ways = geminiAuthOk ? [geminiAuthOk] : AUTH_WAYS;
  let last = { error: 'service' }, sawBusy = false;
  for (const model of models) {
    for (const way of ways) {
      let r, d;
      for (let attempt = 0; attempt < 2; attempt++) { // a busy model gets one quick retry before we move on
        try { r = await geminiFetch('models/' + model + ':generateContent', way, { method: 'POST', body }); d = await r.json().catch(() => ({})); }
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
const LANG_HINT = 'Reply in the same language and script the person used. If they write Telugu in English letters (like "ela unnav"), reply the same way. Keep replies short and friendly, suitable for a phone chat. Use plain text, no markdown symbols like ** or #.';
const plain = (t) => String(t || '').replace(/\*\*(.*?)\*\*/g, '$1').replace(/^#+\s*/gm, '').replace(/^\s*[-*]\s+/gm, '• ').trim();
app.get('/api/ai/status', auth, (req, res) => res.json({ enabled: !!GEMINI_KEY, model: geminiModelOk, lastError: aiLastError }));

// 1) Maata AI chat (history is private to each customer)
app.get('/api/ai/history', auth, (req, res) => res.json(db.aichats.filter((m) => m.userId === req.user.id).slice(-100)));
app.delete('/api/ai/history', auth, (req, res) => { db.aichats = db.aichats.filter((m) => m.userId !== req.user.id); save(); res.json({ ok: true }); });
app.post('/api/ai/chat', auth, async (req, res) => {
  const text = String(req.body?.text || '').trim().slice(0, 2000);
  if (!text) return res.status(400).json({ error: 'Type a question.' });
  if (!aiQuota(req.user)) return res.status(429).json({ error: AI_ERR.quota });
  const mine = { id: newId(), userId: req.user.id, role: 'user', text, ts: now() };
  const past = db.aichats.filter((m) => m.userId === req.user.id).slice(-12);
  const r = await gemini('You are Maata AI, a helpful assistant inside the Maata chat app, made in India. The person chatting with you is ' + req.user.name + '. ' + LANG_HINT + ' Never claim to be a human. For medical, legal or money decisions, give general information and suggest asking an expert.',
    [...past.map((m) => ({ role: m.role === 'ai' ? 'model' : 'user', parts: [{ text: m.text }] })), { role: 'user', parts: [{ text }] }]);
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
  if (!(await bcrypt.compare(String(b.password || ''), req.user.passHash))) return res.status(401).json({ error: 'Your current password is wrong.' });
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
  return { ...b, ownerId: u.id, ownerName: u.name, phone: u.phone, productCount: db.products.filter((p) => p.ownerId === u.id).length };
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

app.get('/admin/api/users', adminAuth, (req, res) => {
  const q = String(req.query.q || '').toLowerCase().trim();
  const filter = String(req.query.filter || 'all');
  const agg = userAggregates();
  const t0 = startOfToday();
  let list = db.users.map((u) => ({
    ...publicUser(u), status: u.status || 'active', createdAt: u.createdAt, lastLoginAt: u.lastLoginAt || null,
    lastSeenAt: isOnline(u.id) ? now() : u.lastSeenAt || null, online: isOnline(u.id), duplicate: !!u.duplicateOf,
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
    user: { ...publicUser(u), status: u.status || 'active', createdAt: u.createdAt, lastLoginAt: u.lastLoginAt, lastSeenAt: u.lastSeenAt, online: isOnline(u.id) },
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
    const u = findUserByToken(a.token || '');
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

io.on('connection', (socket) => {
  if (socket.isAdmin) {
    socket.join('admins');
    socket.emit('live', liveNow());
    return;
  }
  const me = socket.user.id;
  const room = (id) => 'user:' + id;
  socket.join(room(me));
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
    const type = ['contact', 'poll', 'event', 'location', 'gif', 'sticker', 'product'].includes(p?.type) ? p.type : 'text';
    const str = (v, n) => String(v ?? '').trim().slice(0, n);
    let fields;
    if (type === 'text') {
      const text = str(p?.text, 4000); if (!text) return reply({ error: 'Message not sent.' });
      fields = { type, text };
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
  async function relayCaption(rec, toId, text) {
    const from = socket.user.lang || 'en';
    const to = userById(toId)?.lang || 'en';
    const translated = from === to ? text : await translate(text, from, to);
    rec.translated = (rec.translated || 0) + 1;
    const out = { callId: rec.id, text, translated: translated || text, fromLang: from, toLang: to, ok: !!translated || from === to };
    io.to(room(toId)).emit('call:caption', out);
    socket.emit('call:caption:self', out);
  }
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
    if (sttBusy >= 2 || sttTimes.length >= 40) return socket.emit('call:stt', { callId: rec.id, status: 'busy' });
    sttBusy++; sttTimes.push(now());
    try {
      const r = await speechToText(p.audio, socket.user.lang || 'en');
      rec.sttClips = (rec.sttClips || 0) + 1;
      if (r.error) return socket.emit('call:stt', { callId: rec.id, status: r.error });
      if (!r.text) return socket.emit('call:stt', { callId: rec.id, status: 'empty' });
      await relayCaption(rec, p.to, r.text.slice(0, 500));
    } finally { sttBusy--; }
  });

  socket.on('call:end', (p) => {
    if (!valid(p)) return;
    const pr = pendingRings.get(p.to);
    if (pr && pr.callId === String(p.callId)) { clearInterval(pr.timer); pendingRings.delete(p.to); const t = userById(p.to); fcmTo(t, { type: 'call_end', callId: p.callId }, 60).catch(() => {}); const saved = t && contactsOf(t).find((c) => c.id === me); pushTo(t, { type: 'missed', title: 'Missed call · Maata', body: (saved ? saved.name : socket.user.name) + ' called you', tag: 'call-' + p.callId, url: '/#chat=' + me }); }
    finishCall(recFor(p), 'missed'); io.to(room(p.to)).emit('call:ended', clean(p));
  });

  // ----- group calls (everyone connects to everyone; up to GCALL_MAX people) -----
  function gcallState(c) { io.to('grp:' + c.gid).emit('gcall:state', { gid: 'g:' + c.gid, callId: c.id, kind: c.kind, participants: [...c.parts.keys()] }); }
  function gcallJoin(c) {
    const peers = [...c.parts.keys()].filter((x) => x !== me);
    c.parts.set(me, now()); c.ever.add(me); socket.join('gc:' + c.id); socket.gcall = c.id;
    socket.emit('gcall:joined', { gid: 'g:' + c.gid, callId: c.id, kind: c.kind, peers, max: GCALL_MAX, ...(LIVEKIT ? { sfu: { url: LK_URL, token: livekitToken(socket.user, 'maata-' + c.id) } } : {}) });
    gcallState(c); pushLive();
  }
  function gcallLeave(callId) {
    const c = [...gcalls.values()].find((x) => x.id === callId); if (!c || !c.parts.has(me)) return;
    c.parts.delete(me); socket.leave('gc:' + c.id); socket.gcall = null;
    io.to('gc:' + c.id).emit('gcall:peer-left', { callId: c.id, id: me });
    if (!c.parts.size) {
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
    const g = groupById(gidOf(String(p?.gid || ''))); const c = g && gcalls.get(g.id);
    if (!c || !gMember(g, me)) return socket.emit('gcall:ended', { callId: p?.callId });
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
