const http = require('http');
const https = require('https');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const url = require('url');
const { Pool } = require('pg');

const PORT = 3000;
const DATA_FILE = path.join(__dirname, 'data.json');

// ── Conexão PostgreSQL ─────────────────────────────────────────────────────
const pool = new Pool({
  host:     process.env.DB_HOST     || 'evolution_evolution-api-db',
  port:     parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME     || 'evolution',
  user:     process.env.DB_USER     || 'postgres',
  password: process.env.DB_PASSWORD || '8d934495ca2caf161c50',
  ssl: false
});

async function dbInit() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS atendimentos (
      id TEXT PRIMARY KEY,
      nome TEXT,
      remote_jid TEXT,
      instance TEXT,
      chegou TIMESTAMPTZ,
      assumido_em TIMESTAMPTZ,
      encerrado_em TIMESTAMPTZ,
      status TEXT DEFAULT 'concluido'
    );
  `);
  console.log('Tabela atendimentos OK');
}

async function dbInsert(client) {
  await pool.query(
    `INSERT INTO atendimentos (id, nome, remote_jid, instance, chegou, assumido_em, encerrado_em, status)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (id) DO UPDATE SET
       assumido_em  = EXCLUDED.assumido_em,
       encerrado_em = EXCLUDED.encerrado_em,
       status       = EXCLUDED.status`,
    [
      client.id,
      client.nome,
      client.remoteJid,
      client.instance,
      client.chegou,
      client.assumidoEm  || null,
      client.encerradoEm || null,
      client.status
    ]
  );
}

async function dbQuery(from, to) {
  let query = `SELECT * FROM atendimentos WHERE status = 'concluido'`;
  const params = [];
  if (from) { params.push(from + 'T00:00:00-03:00'); query += ` AND encerrado_em >= $${params.length}`; }
  if (to)   { params.push(to   + 'T23:59:59-03:00'); query += ` AND encerrado_em <= $${params.length}`; }
  query += ` ORDER BY encerrado_em DESC`;
  const result = await pool.query(query, params);
  return result.rows.map(r => ({
    id:          r.id,
    nome:        r.nome,
    remoteJid:   r.remote_jid,
    instance:    r.instance,
    chegou:      r.chegou,
    assumidoEm:  r.assumido_em,
    encerradoEm: r.encerrado_em,
    status:      r.status
  }));
}

// ── Webhooks n8n ───────────────────────────────────────────────────────────
const WEBHOOKS = {
  'acutix':   'https://n8n-n8n.mvnptn.easypanel.host/webhook/painel-concluido-acutix',
  'Boafarma': 'https://n8n-n8n.mvnptn.easypanel.host/webhook/painel-concluido-boafarma',
  'teste':    'https://n8n-n8n.mvnptn.easypanel.host/webhook/painel-concluido-teste'
};

function loadData() {
  if (!fs.existsSync(DATA_FILE)) {
    fs.writeFileSync(DATA_FILE, JSON.stringify({ queue: [] }, null, 2));
    return { queue: [] };
  }
  try {
    const d = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    return { queue: d.queue || [] };
  } catch(e) { return { queue: [] }; }
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify({ queue: data.queue || [] }, null, 2));
}

let appData = loadData();

function notifyN8N(remoteJid, instance) {
  const webhookUrl = WEBHOOKS[instance];
  if (!webhookUrl) return;
  const payload = JSON.stringify({ remoteJid, instance });
  const urlObj = new URL(webhookUrl);
  const req = https.request({
    hostname: urlObj.hostname,
    path: urlObj.pathname,
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }
  }, res => console.log(`n8n [${instance}]: ${res.statusCode}`));
  req.on('error', e => console.error(`Erro n8n [${instance}]:`, e.message));
  req.write(payload);
  req.end();
}

function serveFile(res, filePath, contentType) {
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    res.writeHead(200, { 'Content-Type': contentType + '; charset=utf-8' });
    res.end(content);
  } catch(e) {
    res.writeHead(404);
    res.end('Not found');
  }
}

// ── HTTP ───────────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname  = parsedUrl.pathname;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, apikey');
  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  if (pathname === '/' || pathname === '/index.html') {
    return serveFile(res, path.join(__dirname, 'index.html'), 'text/html');
  }
  if (pathname === '/dashboard' || pathname === '/dashboard.html') {
    return serveFile(res, path.join(__dirname, 'dashboard.html'), 'text/html');
  }

  // novo-cliente
  if (pathname === '/novo-cliente' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const client = JSON.parse(body);
        const newClient = {
          id: Date.now().toString(),
          nome: client.nome || 'Cliente',
          remoteJid: client.remoteJid || '',
          instance: client.instance || '',
          numero: client.numero || '',
          tipo: client.tipo || null,  
          chegou: new Date().toISOString(),
          status: 'aguardando',
          assumidoEm: null,
          encerradoEm: null
        };
        const existing = appData.queue.find(c =>
          c.remoteJid === newClient.remoteJid &&
          c.instance  === newClient.instance &&
          (c.status === 'aguardando' || c.status === 'em_atendimento')
        );
        if (existing) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true, id: existing.id, duplicate: true }));
          return;
        }
        appData.queue.push(newClient);
        saveData(appData);
        broadcast({ type: 'novo_cliente', client: newClient });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, id: newClient.id }));
      } catch(e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }
    });
    return;
  }

  // assumir
  if (pathname.startsWith('/assumir/') && req.method === 'POST') {
    const clientId = pathname.split('/')[2];
    const client = appData.queue.find(c => c.id === clientId);
    if (client && client.status === 'aguardando') {
      client.status = 'em_atendimento';
      client.assumidoEm = new Date().toISOString();
      saveData(appData);
      broadcast({ type: 'atualizar', queue: appData.queue });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true }));
    return;
  }

  // encerrar
  if (pathname.startsWith('/encerrar/') && req.method === 'POST') {
    const clientId = pathname.split('/')[2];
    const client = appData.queue.find(c => c.id === clientId);
    if (client) {
      client.status      = 'concluido';
      client.encerradoEm = new Date().toISOString();
      appData.queue      = appData.queue.filter(c => c.id !== clientId);
      saveData(appData);
      try { await dbInsert(client); }
      catch(e) { console.error('Erro DB insert:', e.message); }
      broadcast({ type: 'atualizar', queue: appData.queue });
      if (client.remoteJid && client.instance) notifyN8N(client.remoteJid, client.instance);
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true }));
    return;
  }

  // state
  if (pathname === '/state' && req.method === 'GET') {
    try {
      const hoje = new Date().toISOString().slice(0, 10);
      const history = await dbQuery(hoje, hoje);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ queue: appData.queue, history: history.slice(0, 50) }));
    } catch(e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ queue: appData.queue, history: [] }));
    }
    return;
  }

  // api/history
  if (pathname === '/api/history' && req.method === 'GET') {
    try {
      const { from, to } = parsedUrl.query;
      const data = await dbQuery(from, to);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ total: data.length, data }));
    } catch(e) {
      console.error('Erro /api/history:', e.message);
      res.writeHead(500);
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // api/stats
  if (pathname === '/api/stats' && req.method === 'GET') {
    try {
      const hoje = new Date().toISOString().slice(0, 10);
      const todayHistory = await dbQuery(hoje, hoje);
      const waiting    = appData.queue.filter(c => c.status === 'aguardando').length;
      const inProgress = appData.queue.filter(c => c.status === 'em_atendimento').length;

      const temposEspera = todayHistory
        .filter(c => c.assumidoEm && c.chegou)
        .map(c => (new Date(c.assumidoEm) - new Date(c.chegou)) / 1000);
      const temposAtend = todayHistory
        .filter(c => c.assumidoEm && c.encerradoEm)
        .map(c => (new Date(c.encerradoEm) - new Date(c.assumidoEm)) / 1000);

      const avg = arr => arr.length ? Math.round(arr.reduce((a,b)=>a+b,0)/arr.length) : 0;
      const max = arr => arr.length ? Math.round(Math.max(...arr)) : 0;
      const min = arr => arr.length ? Math.round(Math.min(...arr)) : 0;

      const porHora = Array(24).fill(0);
      todayHistory.forEach(c => { if (c.encerradoEm) porHora[new Date(c.encerradoEm).getHours()]++; });

      const sla = { rapido:0, medio:0, lento:0 };
      temposEspera.forEach(t => { if(t<300) sla.rapido++; else if(t<600) sla.medio++; else sla.lento++; });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        live: { waiting, inProgress },
        hoje: {
          total: todayHistory.length,
          espera:      { avg: avg(temposEspera), max: max(temposEspera), min: min(temposEspera) },
          atendimento: { avg: avg(temposAtend),  max: max(temposAtend),  min: min(temposAtend)  },
          sla, porHora
        }
      }));
    } catch(e) {
      console.error('Erro /api/stats:', e.message);
      res.writeHead(500);
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

// ── WebSocket ──────────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

wss.on('connection', async (ws) => {
  try {
    const hoje = new Date().toISOString().slice(0, 10);
    const history = await dbQuery(hoje, hoje);
    ws.send(JSON.stringify({ type: 'state', queue: appData.queue, history: history.slice(0, 50) }));
  } catch(e) {
    ws.send(JSON.stringify({ type: 'state', queue: appData.queue, history: [] }));
  }
});

// ── Start ──────────────────────────────────────────────────────────────────
dbInit()
  .then(() => server.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`);
    console.log(`Dashboard: http://localhost:${PORT}/dashboard`);
  }))
  .catch(e => { console.error('Falha ao conectar no banco:', e.message); process.exit(1); });
