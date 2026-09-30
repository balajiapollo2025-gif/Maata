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
const COLLS = ['users', 'messages', 'logins', 'calls', 'reports', 'announcements', 'audit', 'statuses', 'groups'];
const SORT_BY = { users: 'createdAt', messages: 'ts', logins: 'ts', calls: 'startedAt', reports: 'ts', announcements: 'ts', audit: 'ts', statuses: 'createdAt', groups: 'createdAt' };
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
const publicUser = (u) => ({ id: u.id, name: u.name, phone: u.phone, lang: u.lang || null });

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
app.post('/api/register', rateLimit, async (req, res) => {
  const name = String(req.body?.name || '').trim().slice(0, 40);
  const phone = String(req.body?.phone || '').replace(/[^\d+]/g, '');
  const password = String(req.body?.password || '');
  if (name.length < 2) return res.status(400).json({ error: 'Name must be at least 2 characters.' });
  if (!/^\+?\d{8,15}$/.test(phone)) return res.status(400).json({ error: 'Enter a valid mobile number.' });
  if (password.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters.' });
  if (db.users.some((u) => u.phone === phone)) return res.status(409).json({ error: 'This number is already registered. Log in instead.' });
  const u = { id: newId(), name, phone, passHash: await bcrypt.hash(password, 10), createdAt: now(), status: 'active', tokenVersion: 0 };
  db.users.push(u);
  recordLogin(u, req, 'signup');
  save();
  res.json({ token: sign(u), user: publicUser(u) });
});

app.post('/api/login', rateLimit, async (req, res) => {
  const phone = String(req.body?.phone || '').replace(/[^\d+]/g, '');
  const password = String(req.body?.password || '');
  const u = db.users.find((x) => x.phone === phone);
  if (!u || !(await bcrypt.compare(password, u.passHash))) return res.status(401).json({ error: 'Wrong mobile number or password.' });
  if (u.status === 'blocked') return res.status(403).json({ error: 'This account is blocked. Contact Maata support.' });
  recordLogin(u, req, 'login');
  save();
  res.json({ token: sign(u), user: publicUser(u) });
});

app.get('/api/me', auth, (req, res) => res.json(publicUser(req.user)));

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
    return res.json(db.messages.filter((m) => m.group === gid && !hidden.has(m.id) && m.ts > since).slice(-300));
  }
  const hidden = new Set(req.user.hiddenMsgs || []), since = (req.user.clearedChats || {})[other] || 0;
  res.json(db.messages.filter((m) => ((m.from === me && m.to === other) || (m.from === other && m.to === me)) && !hidden.has(m.id) && m.ts > since).slice(-300));
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
  return !!owner && owner.status !== 'blocked' && isMutual(viewer, owner) && !blockedOf(owner).includes(viewer.id) && !blockedOf(viewer).includes(owner.id);
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
async function deleteChatFile(id) { try { if (chatBucket) await chatBucket.delete(id); else fs.unlinkSync(path.join(CHAT_DIR, id)); } catch { /* gone */ } }
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
    const m = { id: newId(), from: fromUser.id, to: target.id, group: g.id, ts: now(), status: anyOn ? 'delivered' : 'sent', readBy: {}, type: 'text', text: '', ...fields };
    db.messages.push(m); save();
    (exceptSocketId ? io.to('grp:' + g.id).except(exceptSocketId) : io.to('grp:' + g.id)).emit('msg:new', m);
    if (m.type !== 'system') feed('message', `${fromUser.name} → group ${g.name}: ${m.type === 'text' ? 'message' : m.type}`);
    return m;
  }
  const online2 = isOnline(target.id);
  const m = { id: newId(), from: fromUser.id, to: target.id, ts: now(), status: online2 ? 'delivered' : 'sent', ...(online2 ? { deliveredAt: now() } : {}), type: 'text', text: '', ...fields };
  db.messages.push(m); save();
  io.to('user:' + target.id).emit('msg:new', m);
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
      const chunks = []; await new Promise((ok, bad) => { const r = chatFileStream(m.file.id, 0, m.file.size - 1); r.on('data', (c) => chunks.push(c)); r.on('end', ok); r.on('error', bad); });
      await saveMedia(base.id, Buffer.concat(chunks), m.file.mime);
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
  const file = { id, name, mime, size: req.body.length, ...(req.query.voice === '1' ? { voice: true } : {}), ...(req.query.sticker === '1' && type === 'image' ? { sticker: true } : {}), ...(Number(req.query.dur) > 0 ? { duration: Math.round(Number(req.query.dur)) } : {}) };
  const m = deliverMessage(req.user, ok.target, { type, text: String(req.query.caption || '').trim().slice(0, 1000), file }, String(req.query.sid || '') || null);
  res.json({ message: m });
});

// Files open only for the two people in that chat. <img>/<video> send the login token as ?t=
app.get('/api/chat/file/:id', (req, res) => {
  let viewer = null;
  try { const h = req.headers.authorization || ''; viewer = findUserByToken(h.startsWith('Bearer ') ? h.slice(7) : String(req.query.t || '')); } catch { /* invalid */ }
  if (!viewer) return res.status(401).end();
  const m = db.messages.find((x) => x.file && x.file.id === req.params.id && (x.from === viewer.id || x.to === viewer.id || (x.group && gMember(groupById(x.group), viewer.id))));
  if (!m) return res.status(404).end();
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
const GCALL_MAX = 8;             // people in one group call (each phone sends video to every other)
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
function groupChanged(g, extraIds = []) { io.to('grp:' + g.id).emit('group:update', { id: 'g:' + g.id }); for (const id of extraIds) io.to('user:' + id).emit('group:update', { id: 'g:' + g.id }); }
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

app.get('/admin/api/users', adminAuth, (req, res) => {
  const q = String(req.query.q || '').toLowerCase().trim();
  const filter = String(req.query.filter || 'all');
  const agg = userAggregates();
  const t0 = startOfToday();
  let list = db.users.map((u) => ({
    ...publicUser(u), status: u.status || 'active', createdAt: u.createdAt, lastLoginAt: u.lastLoginAt || null,
    lastSeenAt: isOnline(u.id) ? now() : u.lastSeenAt || null, online: isOnline(u.id),
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
app.delete('/admin/api/users/:id', adminAuth, (req, res) => {
  const u = userById(req.params.id);
  if (!u) return res.status(404).json({ error: 'Customer not found.' });
  kick(u.id, 'This account was deleted.');
  const removedWatchers = watchersOf(u.id);
  db.users = db.users.filter((x) => x.id !== u.id);
  for (const g of db.groups) g.members = g.members.filter((x) => x.id !== u.id);
  db.groups = db.groups.filter((g) => g.members.length);
  for (const x of db.users) if (x.contacts) x.contacts = x.contacts.filter((c) => c.id !== u.id);
  for (const m of db.messages) if (m.file && (m.from === u.id || m.to === u.id)) deleteChatFile(m.file.id);
  db.messages = db.messages.filter((m) => m.from !== u.id && m.to !== u.id);
  for (const st of db.statuses.filter((x) => x.userId === u.id)) if (st.type !== 'text') deleteMedia(st.id);
  db.statuses = db.statuses.filter((x) => x.userId !== u.id);
  audit(req.admin, 'delete', u.name + ' (' + u.phone + ')');
  save();
  for (const w of removedWatchers) io.to('user:' + w).emit('user:removed', { id: u.id });
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

  const wasOnline = isOnline(me);
  online.set(me, (online.get(me) || 0) + 1);
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
    const type = ['contact', 'poll', 'event', 'location', 'gif', 'sticker'].includes(p?.type) ? p.type : 'text';
    const str = (v, n) => String(v ?? '').trim().slice(0, n);
    let fields;
    if (type === 'text') {
      const text = str(p?.text, 4000); if (!text) return reply({ error: 'Message not sent.' });
      fields = { type, text };
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
  });

  // ----- message actions: react, pin, star, delete, forward -----
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
        const f = { type: m.type || 'text', text: m.text || '', forwarded: true };
        if (m.file) f.file = { ...m.file };
        if (m.contact) f.contact = { ...m.contact };
        if (m.poll) f.poll = { question: m.poll.question, options: [...m.poll.options], multi: m.poll.multi, votes: {} };
        if (m.event) f.event = { ...m.event, rsvp: {} };
        if (m.location) f.location = { lat: m.location.lat, lng: m.location.lng, acc: m.location.acc, live: false };
        if (m.gif) f.gif = { ...m.gif };
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
      if (!isOnline(p.to)) finishCall(rec, 'unavailable');
    }
    if (!isOnline(p.to)) return socket.emit('call:unavailable', { callId: p.callId });
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
  socket.on('call:ice', (p) => { if (valid(p)) io.to(room(p.to)).emit('call:ice', clean(p)); });
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

  socket.on('call:end', (p) => { if (!valid(p)) return; finishCall(recFor(p), 'missed'); io.to(room(p.to)).emit('call:ended', clean(p)); });

  // ----- group calls (everyone connects to everyone; up to GCALL_MAX people) -----
  function gcallState(c) { io.to('grp:' + c.gid).emit('gcall:state', { gid: 'g:' + c.gid, callId: c.id, kind: c.kind, participants: [...c.parts.keys()] }); }
  function gcallJoin(c) {
    const peers = [...c.parts.keys()].filter((x) => x !== me);
    c.parts.set(me, now()); c.ever.add(me); socket.join('gc:' + c.id); socket.gcall = c.id;
    socket.emit('gcall:joined', { gid: 'g:' + c.gid, callId: c.id, kind: c.kind, peers });
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
    if (c.parts.size >= GCALL_MAX) return socket.emit('gcall:full', { callId: c.id });
    gcallJoin(c);
  });
  socket.on('gcall:join', (p) => {
    const g = groupById(gidOf(String(p?.gid || ''))); const c = g && gcalls.get(g.id);
    if (!c || !gMember(g, me)) return socket.emit('gcall:ended', { callId: p?.callId });
    if (c.parts.has(me)) return;
    if (c.parts.size >= GCALL_MAX) return socket.emit('gcall:full', { callId: c.id });
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
  .then(() => { cleanStatuses(); })
  .then(() => server.listen(PORT, () => console.log(`Maata running on http://localhost:${PORT}  (admin: /admin)`)))
  .catch((e) => {
    console.error('[db] Could not connect to MongoDB. Check MONGODB_URI, the database password, and Network Access (allow 0.0.0.0/0).');
    console.error(e.message);
    process.exit(1);
  });
