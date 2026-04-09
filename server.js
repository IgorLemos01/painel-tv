const http = require('http');
const https = require('https');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = 3000;
const DATA_FILE = path.join(__dirname, 'data.json');
const HISTORY_FILE = path.join(__dirname, 'history.json');

const WEBHOOKS = {
  'acutix':   'https://n8n-n8n.mvnptn.easypanel.host/webhook/painel-concluido-acutix',
  'Boafarma': 'https://n8n-n8n.mvnptn.easypanel.host/webhook/painel-concluido-boafarma',
  'teste':    'https://n8n-n8n.mvnptn.easypanel.host/webhook/painel-concluido-teste'
};

function loadData() {
  if (!fs.existsSync(DATA_FILE)) {
    const initial = { queue: [] };
    fs.writeFileSync(DATA_FILE, JSON.stringify(initial, null, 2));
    return initial;
  }
  try {
    const d = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    // migração: se tinha history no data.json antigo, move para history.json
    if (d.history && d.history.length > 0) {
      const existing = loadHistory();
      const merged = [...d.history, ...existing];
      const seen = new Set();
      const deduped = merged.filter(c => {
        if (seen.has(c.id)) return false;
        seen.add(c.id);
        return true;
      });
      saveHistory(deduped);
      delete d.history;
      fs.writeFileSync(DATA_FILE, JSON.stringify({ queue: d.queue || [] }, null, 2));
    }
    return d;
  }
  catch(e) { return { queue: [] }; }
}

function loadHistory() {
  if (!fs.existsSync(HISTORY_FILE)) {
    fs.writeFileSync(HISTORY_FILE, JSON.stringify([], null, 2));
    return [];
  }
  try { return JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8')); }
  catch(e) { return []; }
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify({ queue: data.queue || [] }, null, 2));
}

function saveHistory(history) {
  fs.writeFileSync(HISTORY_FILE, JSON.stringify(history, null, 2));
}

let appData = loadData();
let appHistory = loadHistory();

function notifyN8N(remoteJid, instance) {
  const webhookUrl = WEBHOOKS[instance];
  if (!webhookUrl) { console.log(`Instância desconhecida: ${instance}`); return; }
  const payload = JSON.stringify({ remoteJid, instance });
  const urlObj = new URL(webhookUrl);
  const options = {
    hostname: urlObj.hostname,
    path: urlObj.pathname,
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }
  };
  const req = https.request(options, (res) => {
    console.log(`n8n [${instance}] notificado: ${res.statusCode}`);
  });
  req.on('error', (e) => console.error(`Erro ao notificar n8n [${instance}]:`, e.message));
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

const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, apikey');

  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  // ─── Páginas HTML ──────────────────────────────────────────────────────────
  if (pathname === '/' || pathname === '/index.html') {
    return serveFile(res, path.join(__dirname, 'index.html'), 'text/html');
  }

  if (pathname === '/dashboard' || pathname === '/dashboard.html') {
    return serveFile(res, path.join(__dirname, 'dashboard.html'), 'text/html');
  }

  // ─── Novo cliente ──────────────────────────────────────────────────────────
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
          chegou: new Date().toISOString(),
          status: 'aguardando',
          assumidoEm: null,
          encerradoEm: null
        };
        const existing = appData.queue.find(c =>
          c.remoteJid === newClient.remoteJid &&
          c.instance === newClient.instance &&
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

  // ─── Assumir ───────────────────────────────────────────────────────────────
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

  // ─── Encerrar ──────────────────────────────────────────────────────────────
  if (pathname.startsWith('/encerrar/') && req.method === 'POST') {
    const clientId = pathname.split('/')[2];
    const client = appData.queue.find(c => c.id === clientId);
    if (client) {
      client.status = 'concluido';
      client.encerradoEm = new Date().toISOString();
      const record = { ...client };
      appHistory.unshift(record);
      appData.queue = appData.queue.filter(c => c.id !== clientId);
      saveData(appData);
      saveHistory(appHistory);
      broadcast({ type: 'atualizar', queue: appData.queue, history: appHistory.slice(0, 50) });
      if (client.remoteJid && client.instance) notifyN8N(client.remoteJid, client.instance);
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true }));
    return;
  }

  // ─── State (painel TV) ────────────────────────────────────────────────────
  if (pathname === '/state' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ queue: appData.queue, history: appHistory.slice(0, 50) }));
    return;
  }

  // ─── API de histórico para o dashboard ────────────────────────────────────
  // GET /api/history?from=2025-01-01&to=2025-12-31
  if (pathname === '/api/history' && req.method === 'GET') {
    const { from, to } = parsedUrl.query;
    let result = appHistory;

    if (from) {
      const fromDate = new Date(from + 'T00:00:00');
      result = result.filter(c => c.encerradoEm && new Date(c.encerradoEm) >= fromDate);
    }
    if (to) {
      const toDate = new Date(to + 'T23:59:59');
      result = result.filter(c => c.encerradoEm && new Date(c.encerradoEm) <= toDate);
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ total: result.length, data: result }));
    return;
  }

  // ─── Stats ao vivo para o dashboard ───────────────────────────────────────
  if (pathname === '/api/stats' && req.method === 'GET') {
    const now = new Date();
    const todayStr = now.toDateString();

    const todayHistory = appHistory.filter(c =>
      c.encerradoEm && new Date(c.encerradoEm).toDateString() === todayStr
    );

    const waiting = appData.queue.filter(c => c.status === 'aguardando').length;
    const inProgress = appData.queue.filter(c => c.status === 'em_atendimento').length;

    // tempo de espera = assumidoEm - chegou
    const temposEspera = todayHistory
      .filter(c => c.assumidoEm && c.chegou)
      .map(c => (new Date(c.assumidoEm) - new Date(c.chegou)) / 1000);

    // tempo de atendimento = encerradoEm - assumidoEm
    const temposAtendimento = todayHistory
      .filter(c => c.assumidoEm && c.encerradoEm)
      .map(c => (new Date(c.encerradoEm) - new Date(c.assumidoEm)) / 1000);

    const avg = arr => arr.length ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : 0;
    const max = arr => arr.length ? Math.round(Math.max(...arr)) : 0;
    const min = arr => arr.length ? Math.round(Math.min(...arr)) : 0;

    // distribuição por hora
    const porHora = Array(24).fill(0);
    todayHistory.forEach(c => {
      if (c.encerradoEm) porHora[new Date(c.encerradoEm).getHours()]++;
    });

    // SLA: <5min, 5-10min, >10min (espera)
    const sla = { rapido: 0, medio: 0, lento: 0 };
    temposEspera.forEach(t => {
      if (t < 300) sla.rapido++;
      else if (t < 600) sla.medio++;
      else sla.lento++;
    });

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      live: { waiting, inProgress },
      hoje: {
        total: todayHistory.length,
        espera: { avg: avg(temposEspera), max: max(temposEspera), min: min(temposEspera) },
        atendimento: { avg: avg(temposAtendimento), max: max(temposAtendimento), min: min(temposAtendimento) },
        sla,
        porHora
      }
    }));
    return;
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: 'Not found' }));
});

const wss = new WebSocket.Server({ server });

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'state', queue: appData.queue, history: appHistory.slice(0, 50) }));
});

server.listen(PORT, () => {
  console.log(`Painel TV rodando na porta ${PORT}`);
  console.log(`Dashboard: http://localhost:${PORT}/dashboard`);
});
