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

// ---------- Tiny JSON database (swap for MongoDB/PostgreSQL when you scale) ----------
const DATA_DIR = path.join(__dirname, 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');
fs.mkdirSync(DATA_DIR, { recursive: true });
let db = {};
try { db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch { /* first run */ }
for (const k of ['users', 'messages', 'logins', 'calls', 'reports', 'announcements', 'audit']) db[k] = db[k] || [];
// Calls left open by a server restart are closed so reports stay accurate
for (const c of db.calls) if (!c.endedAt) { c.endedAt = c.answeredAt || c.startedAt; c.status = c.answeredAt ? 'completed' : 'missed'; c.duration = c.duration || 0; }
let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    const tmp = DB_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(db));
    fs.renameSync(tmp, DB_FILE);
  }, 200);
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
const publicUser = (u) => ({ id: u.id, name: u.name, phone: u.phone });
const userById = (id) => db.users.find((u) => u.id === id);
const nameOf = (id) => userById(id)?.name || 'Deleted user';
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
const io = new Server(server);

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
  io.emit('user:new', { ...publicUser(u), online: false, lastMessage: null, unread: 0 });
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

app.get('/api/users', auth, (req, res) => {
  const me = req.user.id;
  const list = db.users.filter((u) => u.id !== me && u.status !== 'blocked').map((u) => {
    let lastMessage = null, unread = 0;
    for (const m of db.messages) {
      if ((m.from === me && m.to === u.id) || (m.from === u.id && m.to === me)) {
        lastMessage = m;
        if (m.from === u.id && m.status !== 'read') unread++;
      }
    }
    return { ...publicUser(u), online: isOnline(u.id), lastMessage, unread };
  });
  res.json(list);
});

app.get('/api/messages/:userId', auth, (req, res) => {
  const me = req.user.id, other = req.params.userId;
  res.json(db.messages.filter((m) => (m.from === me && m.to === other) || (m.from === other && m.to === me)).slice(-300));
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
  db.users = db.users.filter((x) => x.id !== u.id);
  db.messages = db.messages.filter((m) => m.from !== u.id && m.to !== u.id);
  audit(req.admin, 'delete', u.name + ' (' + u.phone + ')');
  save();
  io.emit('user:removed', { id: u.id });
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

  const wasOnline = isOnline(me);
  online.set(me, (online.get(me) || 0) + 1);
  if (!wasOnline) socket.broadcast.emit('presence', { userId: me, online: true });
  pushLive();

  const bySender = {};
  for (const m of db.messages) {
    if (m.to === me && m.status === 'sent') { m.status = 'delivered'; (bySender[m.from] = bySender[m.from] || []).push(m.id); }
  }
  if (Object.keys(bySender).length) {
    save();
    for (const [sender, ids] of Object.entries(bySender)) io.to(room(sender)).emit('msg:status', { ids, status: 'delivered' });
  }

  socket.on('msg:send', (p, ack) => {
    const text = String(p?.text || '').trim().slice(0, 4000);
    const to = String(p?.to || '');
    const target = userById(to);
    if (!text || to === me || !target || target.status === 'blocked') return typeof ack === 'function' && ack({ error: 'Message not sent.' });
    const m = { id: newId(), from: me, to, text, ts: now(), status: isOnline(to) ? 'delivered' : 'sent' };
    db.messages.push(m); save();
    io.to(room(to)).emit('msg:new', m);
    socket.to(room(me)).emit('msg:new', m);
    feed('message', `${socket.user.name} → ${target.name}: message`);
    if (typeof ack === 'function') ack({ message: m });
  });

  socket.on('msg:read', (p) => {
    const from = String(p?.from || '');
    const ids = [];
    for (const m of db.messages) if (m.from === from && m.to === me && m.status !== 'read') { m.status = 'read'; ids.push(m.id); }
    if (ids.length) { save(); io.to(room(from)).emit('msg:status', { ids, status: 'read' }); }
  });

  // ----- WebRTC call signaling + call records -----
  const clean = (p) => ({ callId: String(p.callId || '').slice(0, 64), kind: p.kind === 'video' ? 'video' : 'voice', sdp: p.sdp, candidate: p.candidate, from: me, fromName: socket.user.name });
  const valid = (p) => p && typeof p.to === 'string' && p.to !== me && p.callId;
  const recFor = (p) => { const r = activeCalls.get(String(p.callId)); return r && (r.from === me || r.to === me) ? r : null; };

  socket.on('call:offer', (p) => {
    if (!valid(p)) return;
    const target = userById(p.to);
    if (!target || target.status === 'blocked') return socket.emit('call:unavailable', { callId: p.callId });
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
  socket.on('call:end', (p) => { if (!valid(p)) return; finishCall(recFor(p), 'missed'); io.to(room(p.to)).emit('call:ended', clean(p)); });

  socket.on('disconnect', () => {
    const n = (online.get(me) || 1) - 1;
    if (n <= 0) {
      online.delete(me);
      const u = userById(me); if (u) { u.lastSeenAt = now(); save(); }
      io.emit('presence', { userId: me, online: false });
      for (const rec of [...activeCalls.values()]) if (rec.from === me || rec.to === me) finishCall(rec, 'missed');
    } else online.set(me, n);
    pushLive();
  });
});

server.listen(PORT, () => console.log(`Maata running on http://localhost:${PORT}  (admin: /admin)`));
