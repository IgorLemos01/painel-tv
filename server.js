const http = require('http');
const https = require('https');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = 3000;
const DATA_FILE = path.join(__dirname, 'data.json');

// Mapa de instâncias para webhooks do n8n
const WEBHOOKS = {
  'acutix':   'https://n8n-n8n.mvnptn.easypanel.host/webhook/painel-concluido-acutix',
  'Boafarma': 'https://n8n-n8n.mvnptn.easypanel.host/webhook/painel-concluido-boafarma',
  'teste':    'https://n8n-n8n.mvnptn.easypanel.host/webhook/painel-concluido-teste'
};

function loadData() {
  if (!fs.existsSync(DATA_FILE)) {
    const initial = { queue: [], history: [] };
    fs.writeFileSync(DATA_FILE, JSON.stringify(initial, null, 2));
    return initial;
  }
  try { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
  catch(e) { return { queue: [], history: [] }; }
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
}

let appData = loadData();

function cleanHistory() {
  if (appData.history.length > 20) appData.history = appData.history.slice(0, 20);
}

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

const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, apikey');

  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  if (pathname === '/' || pathname === '/index.html') {
    const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

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

  if (pathname.startsWith('/encerrar/') && req.method === 'POST') {
    const clientId = pathname.split('/')[2];
    const client = appData.queue.find(c => c.id === clientId);
    if (client) {
      client.status = 'concluido';
      client.encerradoEm = new Date().toISOString();
      appData.history.unshift({ ...client });
      appData.queue = appData.queue.filter(c => c.id !== clientId);
      cleanHistory();
      saveData(appData);
      broadcast({ type: 'atualizar', queue: appData.queue, history: appData.history });
      if (client.remoteJid && client.instance) notifyN8N(client.remoteJid, client.instance);
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true }));
    return;
  }

  if (pathname === '/state' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(appData));
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
  ws.send(JSON.stringify({ type: 'state', queue: appData.queue, history: appData.history }));
});

server.listen(PORT, () => {
  console.log(`Painel TV rodando na porta ${PORT}`);
});
