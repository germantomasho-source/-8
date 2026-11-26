'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { FileStore } = require('./store');

const PORT = process.env.PORT || 3000;
const MIN = 2, MAX = 30;
const TTL = +process.env.ROOM_TTL_MS || 24 * 3600e3;
const HEARTBEAT_MS = +process.env.HEARTBEAT_MS || 20000;
const CLEANUP_MS = +process.env.CLEANUP_MS || 600000;
const COUNTDOWN_LEAD_MS = +process.env.COUNTDOWN_LEAD_MS || 1500; // запас на доставку команды всем клиентам
const COUNTDOWN_MS = +process.env.COUNTDOWN_MS || 5000;           // 5 → 4 → 3 → 2 → 1
const SPIN_MS = +process.env.SPIN_MS || 9000;                     // одинаковая длительность вращения для всех
const DONE_TTL = +process.env.DONE_TTL_MS || 30 * 24 * 3600e3;    // комнаты с готовым розыгрышем живут дольше
const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // без O/0/I/1
const store = new FileStore(process.env.DATA_FILE || path.join(__dirname, '..', 'data', 'rooms.json'));
const INDEX = path.join(__dirname, '..', 'public', 'index.html');

const conns = new Map(); // code -> Set<ws>; ws.meta = {code, pid}
const createLog = new Map(); // ip -> [timestamps]

/* ---------- helpers ---------- */
const rid = n => crypto.randomBytes(n).toString('hex');
// Алгоритм Саттоло: ровно один цикл длины n => никто не получает себя, все получатели уникальны.
// Случайность — crypto.randomInt (криптографически стойкая, без смещения).
function sattolo(n) {
  const a = Array.from({ length: n }, (_, i) => i);
  for (let i = n - 1; i > 0; i--) { const j = crypto.randomInt(i); [a[i], a[j]] = [a[j], a[i]]; } // j in [0, i-1]
  return a; // a[i] — индекс получателя для участника i, a[i] !== i
}
const norm = s => String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim();
const sameName = (a, b) => a.toLowerCase() === b.toLowerCase();
const normCode = s => String(s || '').toUpperCase().replace(/\s+/g, '');
function newCode() {
  for (;;) {
    let c = ''; for (let i = 0; i < 5; i++) c += ALPHA[crypto.randomInt(ALPHA.length)];
    if (!store.get(c)) return c;
  }
}
const isOnline = (code, pid) => [...(conns.get(code) || [])].some(w => w.meta.pid === pid);
const view = r => ({
  code: r.code, closed: r.closed, hostId: r.hostId, min: MIN, max: MAX,
  draw: r.draw ? { countdownAt: r.draw.countdownAt, spinStart: r.draw.spinStart, spinEnd: r.draw.spinEnd } : null, // только время; распределение сюда не попадает
  players: r.players.map(p => ({ id: p.id, name: p.name, online: isOnline(r.code, p.id) })) // токены не передаются
});
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const err = (ws, code, msg) => send(ws, { t: 'error', code, msg });
function broadcast(r) {
  const v = view(r);
  for (const w of conns.get(r.code) || []) send(w, { t: 'room', room: v, you: w.meta.pid, now: Date.now() });
}
// Личный результат — ТОЛЬКО этому участнику. Получатель определяется по сохранённому серверному распределению.
function sendResult(ws, r) {
  if (!r.draw) return;
  const pid = ws.meta && ws.meta.pid, tid = r.draw.assign[pid];
  const target = tid && r.players.find(p => p.id === tid);
  if (!target) return;
  send(ws, { t: 'result', targetId: target.id, target: target.name, jitter: r.draw.jitter[pid] });
}
// Удаляет сокет из Set комнаты (и сам Set, если он опустел).
function unlink(ws, code) {
  const set = conns.get(code);
  if (set) { set.delete(ws); if (!set.size) conns.delete(code); }
}
// Страховка: убирает сокет из ВСЕХ Set, даже если ws.meta уже обнулён.
function purge(ws) { for (const code of [...conns.keys()]) unlink(ws, code); }
function attach(ws, r, pid) {
  const prev = ws.meta || {};
  // тот же сокет перешёл в другую комнату/к другому игроку: корректно выходим из прежней
  if (prev.code && !(prev.code === r.code && prev.pid === pid)) detach(ws, true);
  // старое соединение этого же игрока (другая вкладка, «зависший» сокет): сначала убираем из Set, потом закрываем
  for (const w of [...(conns.get(r.code) || [])]) {
    if (w !== ws && w.meta.pid === pid) { unlink(w, r.code); w.meta = {}; try { w.close(4001, 'replaced'); } catch (e) {} }
  }
  ws.meta = { code: r.code, pid };
  if (!conns.has(r.code)) conns.set(r.code, new Set());
  conns.get(r.code).add(ws);
  r.updatedAt = Date.now();
}
function detach(ws, notify = true) {
  const { code } = ws.meta || {};
  if (!code) return;
  unlink(ws, code);
  ws.meta = {};
  const r = store.get(code);
  if (r && notify) { r.updatedAt = Date.now(); store.touch(); broadcast(r); }
}
function rateOk(ip) {
  const now = Date.now(), a = (createLog.get(ip) || []).filter(t => now - t < 3600e3);
  if (a.length >= (+process.env.CREATE_LIMIT || 20)) { createLog.set(ip, a); return false; }
  a.push(now); createLog.set(ip, a); return true;
}

/* ---------- обработчики сообщений ---------- */
function checkName(ws, raw) {
  const name = norm(raw);
  if ([...name].length < 1 || [...name].length > 16) { err(ws, 'bad_name', 'Имя должно быть от 1 до 16 символов'); return null; }
  return name;
}
const H = {
  peek(ws, m) {
    const r = store.get(normCode(m.code));
    send(ws, { t: 'peek', code: normCode(m.code), exists: !!r, closed: !!(r && (r.closed || r.draw)), full: !!(r && r.players.length >= MAX) });
  },
  create(ws, m, ip) {
    if (!rateOk(ip)) return err(ws, 'rate', 'Слишком много комнат. Попробуйте позже');
    const name = checkName(ws, m.name); if (!name) return;
    const p = { id: rid(4), name, token: rid(16) };
    const r = { code: newCode(), createdAt: Date.now(), updatedAt: Date.now(), closed: false, hostId: p.id, players: [p] };
    store.set(r); attach(ws, r, p.id);
    send(ws, { t: 'joined', code: r.code, token: p.token, you: p.id });
    broadcast(r);
  },
  join(ws, m) {
    const code = normCode(m.code), r = store.get(code);
    if (!r) return err(ws, 'not_found', 'Комната не найдена или уже удалена');
    const ex = m.token && r.players.find(p => p.token === m.token);
    if (ex) { // возвращение в своё место
      attach(ws, r, ex.id); store.touch();
      send(ws, { t: 'joined', code, token: ex.token, you: ex.id });
      broadcast(r); sendResult(ws, r); // после перезахода/перезапуска сервера результат берётся из хранилища, новый не создаётся
      return;
    }
    if (m.token && !m.name) return err(ws, 'bad_token', 'Ваше место в комнате не найдено. Введите имя');
    if (r.draw) return err(ws, 'started', 'Розыгрыш уже начался — список участников зафиксирован');
    if (r.closed) return err(ws, 'closed', 'Организатор закрыл набор участников');
    if (r.players.length >= MAX) return err(ws, 'full', 'В комнате уже ' + MAX + ' участников');
    const name = checkName(ws, m.name); if (!name) return;
    if (r.players.some(p => sameName(p.name, name))) return err(ws, 'name_taken', 'Имя «' + name + '» уже занято. Добавьте фамилию или букву');
    const p = { id: rid(4), name, token: rid(16) };
    r.players.push(p); store.set(r); attach(ws, r, p.id);
    send(ws, { t: 'joined', code, token: p.token, you: p.id });
    broadcast(r);
  },
  setclosed(ws, m) {
    const r = store.get(ws.meta.code);
    if (!r) return err(ws, 'not_found', 'Вы не в комнате');
    if (r.hostId !== ws.meta.pid) return err(ws, 'not_host', 'Только организатор может это сделать');
    if (r.draw) return err(ws, 'started', 'Розыгрыш уже начался');
    r.closed = !!m.closed; r.updatedAt = Date.now(); store.set(r); broadcast(r);
  },
  leave(ws) {
    const { code, pid } = ws.meta, r = store.get(code);
    if (!r) return send(ws, { t: 'left' });
    detach(ws, false);
    if (r.draw) { // после старта участник и его результат остаются в комнате: список зафиксирован
      r.updatedAt = Date.now(); store.touch(); broadcast(r); return send(ws, { t: 'left', kept: true });
    }
    r.players = r.players.filter(p => p.id !== pid);
    if (!r.players.length) store.delete(code);
    else { if (r.hostId === pid) r.hostId = r.players[0].id; r.updatedAt = Date.now(); store.set(r); broadcast(r); }
    send(ws, { t: 'left' });
  },
  startdraw(ws) {
    const r = store.get(ws.meta.code);
    if (!r) return err(ws, 'not_found', 'Вы не в комнате');
    if (r.hostId !== ws.meta.pid) return err(ws, 'not_host', 'Только организатор может начать розыгрыш');
    if (r.draw) return err(ws, 'started', 'Розыгрыш уже запущен'); // повторный запуск невозможен
    const n = r.players.length;
    if (n < MIN) return err(ws, 'too_few', 'Для розыгрыша нужно минимум ' + MIN + ' участника');
    if (n > MAX) return err(ws, 'full', 'Слишком много участников');
    const off = r.players.filter(p => !isOnline(r.code, p.id)).map(p => p.name);
    if (off.length) { const e = { t: 'error', code: 'offline', msg: 'Не все участники в сети: ' + off.join(', '), names: off }; return send(ws, e); }
    // 1) распределение (один случайный цикл)  2) фиксация  3) сохранение на диск ДО рассылки
    const perm = sattolo(n), assign = {}, jitter = {};
    r.players.forEach((p, i) => {
      assign[p.id] = r.players[perm[i]].id;
      jitter[p.id] = (crypto.randomInt(1000) / 1000 - 0.5) * 0.6; // где именно в секторе остановится стрелка (−0.3…+0.3 ширины)
    });
    const now = Date.now(), countdownAt = now + COUNTDOWN_LEAD_MS, spinStart = countdownAt + COUNTDOWN_MS;
    r.draw = { assign, jitter, countdownAt, spinStart, spinEnd: spinStart + SPIN_MS, startedAt: now };
    r.closed = true; r.updatedAt = now; store.set(r);
    if (!store.flush()) { delete r.draw; r.closed = false; return err(ws, 'storage', 'Не удалось сохранить результаты. Попробуйте ещё раз'); }
    broadcast(r);
    for (const w of conns.get(r.code) || []) sendResult(w, r); // каждому — только его собственный результат
  },
  sync(ws, m) { send(ws, { t: 'sync', c: typeof m.c === 'number' ? m.c : 0, s: Date.now() }); },
  ping(ws) { send(ws, { t: 'hb' }); }
};

/* ---------- HTTP + WebSocket ---------- */
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  const sec = {
    'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
    'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; connect-src 'self' ws: wss:; img-src 'self' data:"
  };
  if (url === '/__stats' && process.env.NYSR_TEST) {
    const c = {}; for (const [k, v] of conns) c[k] = v.size;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ rooms: store.all().length, conns: c, sockets: wss.clients.size }));
  }
  if (url === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    return fs.readFile(INDEX, (e, buf) => {
      if (e) { res.writeHead(500); return res.end('index.html missing'); }
      res.writeHead(200, { ...sec, 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); res.end(buf);
    });
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found');
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2048 });
wss.on('connection', (ws, req) => {
  const ip = (process.env.TRUST_PROXY && String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress;
  ws.meta = {}; ws.alive = true; ws.hits = []; 
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', data => {
    const now = Date.now(); ws.hits = ws.hits.filter(t => now - t < 10000); ws.hits.push(now);
    if (ws.hits.length > 40) return ws.close(1008, 'rate');
    let m; try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    const fn = ({ peek: H.peek, create: H.create, join: H.join, ping: H.ping, sync: H.sync, startdraw: H.startdraw, setclosed: H.setclosed, leave: H.leave })[m.t];
    if (!fn) return;
    if ((m.t === 'setclosed' || m.t === 'leave' || m.t === 'startdraw') && !ws.meta.code) return err(ws, 'not_found', 'Вы не в комнате');
    try { fn(ws, m, ip); } catch (e) { console.error(e); err(ws, 'server', 'Внутренняя ошибка'); }
  });
  ws.on('close', () => { detach(ws, true); purge(ws); });
  ws.on('error', () => {});
});
setInterval(() => { // heartbeat: убираем «мёртвые» соединения, остальным шлём hb
  for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); send(ws, { t: 'hb' }); }
}, HEARTBEAT_MS).unref();
setInterval(() => { // удаляем заброшенные комнаты
  const now = Date.now();
  for (const r of store.all()) if (now - r.updatedAt > (r.draw ? Math.max(TTL, DONE_TTL) : TTL) && !conns.has(r.code)) store.delete(r.code);
}, CLEANUP_MS).unref();

function shutdown() { store.flush(); process.exit(0); }
process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
server.listen(PORT, () => console.log('NYSR server: http://localhost:' + PORT + ' (комнат в хранилище: ' + store.all().length + ')'));
