const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = 3000;
const DATA_FILE = path.join(__dirname, 'data.json');

function loadData() {
  if (!fs.existsSync(DATA_FILE)) {
    const initial = { queue: [], history: [] };
    fs.writeFileSync(DATA_FILE, JSON.stringify(initial, null, 2));
    return initial;
  }
  try {
    return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  } catch(e) {
    return { queue: [], history: [] };
  }
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
}

let appData = loadData();

// Clean old concluded clients (keep last 20)
function cleanHistory() {
  if (appData.history.length > 20) {
    appData.history = appData.history.slice(0, 20);
  }
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

  // n8n sends new client
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
          chegou: new Date().toISOString(),
          status: 'aguardando',
          assumidoEm: null,
          encerradoEm: null,
          alertado: false
        };
        // Avoid duplicate by remoteJid if already waiting/active
        const existing = appData.queue.find(c =>
          c.remoteJid === newClient.remoteJid &&
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
      } catch (e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }
    });
    return;
  }

  // Mark as in attendance
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

  // Mark as concluded
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
