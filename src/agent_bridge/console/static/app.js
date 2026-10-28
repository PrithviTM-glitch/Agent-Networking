"use strict";
// Every string from agents goes through textContent, never innerHTML.
const ADMIN = "admin";
const S = {token: null, agents: [], msgs: [], audit: [], jobs: {}, view: {type: "chan", id: "broadcast"}, unread: {},
  extra: new Set(), drafts: {}, atme: {}, atOnly: false, rooms: {}, auditQ: {action: "", target: "", range: "all"}, tab: "overview", profile: null, ledger: null, connected: false};
const $ = id => document.getElementById(id);

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = String(text);
  return e;
}
function add(parent, ...kids) { for (const k of kids) if (k != null) parent.append(k); return parent; }
function button(text, fn, cls) { const b = el("button", cls, text); b.type = "button"; b.onclick = fn; return b; }
function hue(s) { let h = 0; for (const ch of String(s)) h = (h * 31 + ch.charCodeAt(0)) >>> 0; return h % 360; }
function clock(ts) { return new Date(ts * 1000).toLocaleString([], {month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit"}); }
function ago(ts) {
  const s = Math.round(Date.now() / 1000 - ts), a = Math.abs(s);
  const v = a < 60 ? a + "s" : a < 3600 ? Math.round(a / 60) + "m" : a < 86400 ? Math.round(a / 3600) + "h" : Math.round(a / 86400) + "d";
  return s >= 0 ? v + " ago" : "in " + v;
}
function toast(text, bad) {
  const d = el("div", bad ? "bad" : "", text);
  $("toast").append(d);
  setTimeout(() => d.remove(), bad ? 8000 : 4000);
}

// api

async function api(path, body) {
  const opt = {headers: {"X-Admin-Token": S.token || ""}};
  if (body) { opt.method = "POST"; opt.headers["Content-Type"] = "application/json"; opt.body = JSON.stringify(body); }
  const r = await fetch(path, opt);
  const j = await r.json().catch(() => ({error: "HTTP " + r.status}));
  if (r.status === 401) { showLogin(); throw new Error(j.error); }
  if (!r.ok) throw new Error(j.error || "HTTP " + r.status);
  return j;
}
async function act(path, body, what) {
  try {
    const r = await api(path, body);
    toast((what ? what + ": " : "") + r.result);
    return r;
  } catch (e) { toast(e.message, true); return null; }
}

// names and channels

function agentOf(addr) { return S.agents.find(a => a.addr === addr); }
function byKey(key) { return S.agents.find(a => a.key === key); }
function ident(a) { return a.addr ? {addr: a.addr} : {session: a.session}; }
function nameOf(addr) { const a = agentOf(addr); return a && a.friendly !== addr ? a.friendly : addr; }
function chanOf(m) {
  if (m.topic === "broadcast") return "broadcast";
  if (m.topic.startsWith("room/")) return "room:" + m.topic.slice(5);
  if (m.topic.startsWith("inbox/")) return "dm:" + [m.topic.slice(6), m.from || "?"].sort().join("|");
  return "topic:" + m.topic;
}
function label(id) {
  if (id === "broadcast") return "Broadcast";
  if (id.startsWith("room:")) return "# " + id.slice(5);
  if (id.startsWith("dm:")) return id.slice(3).split("|").map(nameOf).join(" ↔ ");
  return id.slice(6);
}
function channels() {
  const rooms = new Set(), dms = new Set(), topics = new Set();
  for (const a of S.agents) for (const r of a.rooms) rooms.add("room:" + r);
  for (const c of S.extra) (c.startsWith("room:") ? rooms : c.startsWith("dm:") ? dms : topics).add(c);
  for (const m of S.msgs) {
    const c = chanOf(m);
    if (c !== "broadcast") (c.startsWith("room:") ? rooms : c.startsWith("dm:") ? dms : topics).add(c);
  }
  const byLabel = (a, b) => label(a).localeCompare(label(b));
  return {rooms: [...rooms].sort(), dms: [...dms].sort(byLabel), topics: [...topics].sort()};
}
function viewKey(v) {
  if (v.type === "chan") return v.id;
  if (v.type === "agent" || v.type === "stop") return "agent:" + v.key;
  return v.room ? "room:" + v.room : v.type;
}
function go(v) {
  S.view = v;
  if (v.type === "chan") { delete S.unread[v.id]; delete S.atme[v.id]; }
  if (v.type === "agent") { S.tab = "overview"; S.profile = null; loadProfile(); loadCapacity(); }
  if (v.type === "ledger") { S.ledger = null; loadLedger(); }
  if (v.type === "kill" || v.type === "stop") {
    S.kill = {signal: "terminate", sessions: false, preview: null, results: null, selected: new Set()};
    loadKillPreview();
  }
  if (v.type === "capacity") loadCapacity();
  if (v.type === "leaks") { S.leaks = null; loadLeaks(); }
  if (v.type === "audit") loadAudit();
  $("nav").classList.remove("open");
  render(true);
}

// sidebar

function renderNav() {
  const alive = S.agents.filter(a => a.alive).length, bus = S.agents.filter(a => a.on_bus).length;
  $("stat").textContent = bus + " on the bus · " + alive + " sessions alive · " + S.msgs.length + " messages";
  const nav = $("nav"), top = nav.scrollTop, cur = viewKey(S.view), ch = channels();
  // Rebuilding the sidebar under a pressed mouse button loses the click, so only rebuild when something shows.
  const sig = JSON.stringify([cur, ch, S.unread, S.atme, S.agents.map(a => [a.key, a.friendly, a.alive, a.rooms, a.status])]);
  if (sig === S.navSig) return;
  S.navSig = sig;
  nav.replaceChildren();
  const item = (key, v, text, sub, extra) => {
    const d = el("div", "ch" + (key === cur ? " sel" : ""));
    d.tabIndex = 0;
    const l = el("span", "lbl", text);
    if (sub) add(l, el("span", "sub", sub));
    add(d, extra, l);
    if (S.atme[key]) add(d, el("span", "badge at", "@" + S.atme[key]));
    if (S.unread[key]) add(d, el("span", "badge", S.unread[key]));
    d.onclick = () => go(v);
    d.onkeydown = e => { if (e.key === "Enter") go(v); };
    nav.append(d);
    return d;
  };
  const head = (text, plus) => {
    const h = add(el("h3"), el("span", null, text));
    if (plus) add(h, button("+", plus));
    nav.append(h);
  };
  item("broadcast", {type: "chan", id: "broadcast"}, "📣 Broadcast");
  head("Rooms", () => newChan("room"));
  for (const c of ch.rooms) {
    const n = S.agents.filter(a => a.rooms.includes(c.slice(5))).length;
    item(c, {type: "chan", id: c}, label(c), n + " member" + (n === 1 ? "" : "s"));
  }
  head("Direct messages");
  for (const c of ch.dms) item(c, {type: "chan", id: c}, label(c));
  if (!ch.dms.length) nav.append(el("div", "ch muted", "none yet"));
  head("Topics", () => newChan("topic"));
  for (const c of ch.topics) item(c, {type: "chan", id: c}, label(c));
  const entity = a => {
    const dot = el("span", "live " + (a.alive === true ? "yes" : a.alive === false ? "no" : ""));
    dot.title = a.alive === true ? "alive" : a.alive === false ? "dead" : "no process info";
    const sub = [a.friendly === a.addr ? null : a.addr, a.alive && a.status].filter(Boolean).join(" · ");
    const d = item("agent:" + a.key, {type: "agent", key: a.key}, a.friendly, sub, dot);
    if (a.alive === false) d.classList.add("dead");
  };
  const order = (x, y) => (y.alive === true) - (x.alive === true) || y.seen - x.seen;
  const onBus = S.agents.filter(a => a.on_bus).sort(order), off = S.agents.filter(a => !a.on_bus).sort(order);
  head("Agents on the bus (" + onBus.length + ")");
  onBus.forEach(entity);
  head("Other sessions (" + off.length + ")");
  off.forEach(entity);
  head("Admin");
  item("capacity", {type: "capacity"}, "Capacity");
  item("leaks", {type: "leaks"}, "Leftovers");
  item("ledger", {type: "ledger"}, "Ledger");
  item("audit", {type: "audit"}, "Audit log");
  nav.scrollTop = top;
}
function newChan(kind) {
  const name = prompt(kind === "room" ? "Room name" : "Topic");
  if (!name) return;
  const id = kind + ":" + name;
  S.extra.add(id);
  go({type: "chan", id});
}

// panes

function setTitle(t1, t2, actions) {
  $("t1").textContent = t1;
  $("t2").textContent = t2 || "";
  $("tactions").replaceChildren(...(actions || []));
}
function msgNode(m, tag) {
  const d = el("div", "msg" + (m.from === ADMIN ? " admin" : "")), h = el("div", "h");
  const who = el("span", "who", m.from ? nameOf(m.from) : "(anonymous)");
  who.style.color = "hsl(" + hue(m.from || "") + " 55% 48%)";
  add(h, who);
  if (m.from && nameOf(m.from) !== m.from) add(h, el("span", "t mono", m.from));
  if (tag) add(h, el("span", "tag", label(chanOf(m))));
  if (m.ts) add(h, el("span", "t", clock(m.ts)));
  return add(d, h, bodyNode(m));
}
function mentioned(m, word) {
  // Highlights only what the bus resolved: an address in m.mentions, the name of a mentioned agent, or @all.
  if (word === "all") return m.topic.startsWith("room/");
  const ms = m.mentions || [];
  return ms.includes(word) || ms.some(a => (agentOf(a) || {}).name === word);
}
function bodyNode(m) {
  const b = el("div", "b");
  if (!m.topic.startsWith("room/") || !m.body.includes("@")) { b.textContent = m.body; return b; }
  let last = 0;
  for (const x of m.body.matchAll(/(^|\s)@([A-Za-z0-9_-]+)/g)) {
    const at = x.index + x[1].length, word = x[2];
    if (!mentioned(m, word)) continue;
    b.append(document.createTextNode(m.body.slice(last, at)));
    const me = word === ADMIN || ((m.mentions || []).includes(ADMIN) && agentOf(ADMIN)?.name === word);
    b.append(el("span", "mention" + (me ? " me" : ""), "@" + word));
    last = at + word.length + 1;
  }
  b.append(document.createTextNode(m.body.slice(last)));
  return b;
}
function renderChannel(id, fresh) {
  const body = $("body"), stick = fresh || body.scrollTop + body.clientHeight >= body.scrollHeight - 40;
  const atOnly = S.atOnly && id.startsWith("room:");
  const list = S.msgs.filter(m => chanOf(m) === id && (!atOnly || (m.mentions || []).includes(ADMIN)));
  let sub = "", note = "", can = true;
  if (id === "broadcast") {
    sub = "Every agent on the bus gets broadcasts. Sessions with the hook installed see them on their next prompt.";
    note = "Sends to everyone as admin.";
  } else if (id.startsWith("room:")) {
    sub = S.agents.filter(a => a.rooms.includes(id.slice(5))).map(a => a.addr).join(", ") || "no members";
    note = "Posting to " + label(id) + " as admin.";
  } else if (id.startsWith("dm:")) {
    const pair = id.slice(3).split("|");
    sub = pair.join(" ↔ ");
    can = pair.includes(ADMIN);
    note = can ? "Direct message to " + nameOf(pair.find(p => p !== ADMIN) || ADMIN) + " as admin."
      : "A conversation between two agents. Open an agent to message it as admin.";
  } else {
    sub = "Raw topic";
    note = "Publishing to " + id.slice(6) + " as admin.";
  }
  const room = id.startsWith("room:") && id.slice(5);
  const filter = button(atOnly ? "Showing @admin only" : "Mentions of admin", () => { S.atOnly = !S.atOnly; renderChannel(id, true); });
  if (atOnly) filter.className = "primary";
  if (room) {
    const r = roomInfo(room);
    sub = (r.mode === "announce" ? "announce only · " : "") + (r.moderators.length ? "moderators " + r.moderators.join(", ") + " · " : "") + sub;
  }
  const tools = room ? [filter, button("Settings", () => go({type: "roomset", room})), button("Kill room…", () => go({type: "kill", room}), "danger")] : [];
  setTitle(label(id), sub, tools);
  body.replaceChildren();
  if (!list.length) body.append(el("div", "empty", atOnly ? "No messages mention admin." : "No messages yet."));
  for (const m of list.slice(-1500)) body.append(msgNode(m));
  if (stick) body.scrollTop = body.scrollHeight;
  $("compose").hidden = false;
  $("cnote").textContent = note;
  $("text").disabled = $("send").disabled = !can;
  $("text").placeholder = id === "broadcast" ? "Broadcast to every agent" : "Message";
  $("send").textContent = id === "broadcast" ? "Broadcast" : "Send";
}
async function sendCompose(e) {
  e.preventDefault();
  const id = S.view.id, text = $("text").value;
  if (!text.trim() || S.view.type !== "chan") return;
  let r;
  if (id === "broadcast") {
    if (!confirm("Broadcast to every agent?\n\n" + text)) return;
    r = await act("/api/broadcast", {text});
  } else if (id.startsWith("dm:")) {
    r = await act("/api/send", {kind: "dm", target: id.slice(3).split("|").find(p => p !== ADMIN), text});
  } else {
    r = await act("/api/send", {kind: id.startsWith("room:") ? "room" : "topic", target: id.slice(id.indexOf(":") + 1), text});
  }
  if (r) $("text").value = "";
}

// @mentions in the compose box

const M = {items: [], index: 0, start: 0};
function mentionCandidates(prefix) {
  if (S.view.type !== "chan" || !S.view.id.startsWith("room:")) return [];
  const room = S.view.id.slice(5), p = prefix.toLowerCase();
  const members = S.agents.filter(a => a.addr && a.addr !== ADMIN && a.rooms.includes(room));
  const items = [{addr: "all", label: "everyone in # " + room}, ...members.map(a => ({addr: a.addr, label: a.friendly}))];
  const starts = i => i.addr.toLowerCase().startsWith(p) || i.label.toLowerCase().split(/\s+/).some(w => w.startsWith(p));
  return items.filter(starts).slice(0, 8);
}
function mentionSuggest() {
  const t = $("text"), before = t.value.slice(0, t.selectionStart), m = before.match(/(^|\s)@([A-Za-z0-9_-]*)$/);
  M.items = m ? mentionCandidates(m[2]) : [];
  M.start = m ? before.length - m[2].length - 1 : 0;
  M.index = 0;
  renderMention();
}
function renderMention() {
  const box = $("mention");
  box.hidden = !M.items.length;
  box.replaceChildren(...M.items.map((it, i) => {
    const d = el("div", "opt" + (i === M.index ? " sel" : ""));
    d.setAttribute("role", "option");
    d.append(el("span", "mono", "@" + it.addr), el("span", "muted", it.label === it.addr ? "" : it.label));
    d.onmousedown = e => { e.preventDefault(); M.index = i; mentionPick(); };
    return d;
  }));
}
function mentionPick() {
  const t = $("text"), it = M.items[M.index], end = t.selectionStart;
  t.value = t.value.slice(0, M.start) + "@" + it.addr + " " + t.value.slice(end);
  t.selectionStart = t.selectionEnd = M.start + it.addr.length + 2;
  mentionClose();
  t.focus();
}
function mentionClose() { M.items = []; renderMention(); }
function mentionKey(e) {
  if (!M.items.length) return false;
  if (e.key === "ArrowDown" || e.key === "ArrowUp") M.index = (M.index + (e.key === "ArrowDown" ? 1 : M.items.length - 1)) % M.items.length;
  else if (e.key === "Enter" || e.key === "Tab") mentionPick();
  else if (e.key === "Escape") mentionClose();
  else return false;
  e.preventDefault();
  renderMention();
  return true;
}

// agent profile

async function loadProfile() {
  const key = S.view.key, a = byKey(key), here = () => S.view.type === "agent" && S.view.key === key;
  if (!a) return;
  try {
    const p = await api("/api/agent?" + new URLSearchParams(ident(a)));
    if (here()) { S.profile = p; if (S.tab !== "overview") renderPane(); }
  } catch (e) {
    if (here()) { S.profile = {error: e.message}; renderPane(); }
  }
}
function field(dl, k, v, mono) {
  add(dl, el("dt", null, k));
  const dd = el("dd", mono ? "mono" : null, v == null || v === "" ? "—" : v);
  dl.append(dd);
  return dd;
}
function status(a) {
  const s = a.alive === true ? "alive" : a.alive === false ? "dead" : "unknown";
  return el("span", "pill " + s, a.alive == null ? "no process info" : s);
}
function renderAgent() {
  const a = byKey(S.view.key) || (S.profile && S.profile.agent);
  const body = $("body"), top = body.scrollTop;
  $("compose").hidden = true;
  if (!a) { setTitle(S.view.key, "gone"); body.replaceChildren(el("div", "empty", "This agent or session is no longer listed.")); return; }
  const dmId = a.addr && "dm:" + [a.addr, ADMIN].sort().join("|");
  const tools = a.addr ? [button("Message", () => go({type: "chan", id: dmId})),
    button("Stop agent…", () => go({type: "stop", key: a.key, addr: a.addr}), "danger")] : [];
  setTitle(a.friendly, a.addr || "session " + a.session, tools);
  body.replaceChildren();
  const tabs = el("div", "tabs");
  const names = a.addr ? [["overview", "Overview"], ["dms", "DMs"], ["ledger", "Ledger"], ["activity", "Activity"]]
    : [["overview", "Overview"], ["activity", "Activity"]];
  if (!names.some(([k]) => k === S.tab)) S.tab = "overview";
  for (const [k, t] of names) {
    const b = button(t, () => { S.tab = k; renderPane(); });
    if (S.tab === k) b.className = "sel";
    tabs.append(b);
  }
  body.append(tabs);
  const p = S.profile;
  if (S.tab === "overview") renderOverview(body, a);
  else if (!p) body.append(el("div", "empty", "Loading…"));
  else if (p.error) body.append(el("div", "empty", p.error));
  else if (S.tab === "dms") {
    if (!p.dms.length) body.append(el("div", "empty", "No direct messages in the log."));
    for (const m of p.dms) body.append(msgNode(m, true));
  } else if (S.tab === "ledger") renderAgentLedger(body, a, p.ledger);
  else renderActivity(body, p.activity);
  body.scrollTop = top;
}
function renderOverview(body, a) {
  const card = el("div", "card"), dl = el("dl");
  field(dl, "Name", a.friendly);
  field(dl, "Process", null).replaceChildren(status(a));
  field(dl, "Activity", a.status);
  field(dl, "Kind", [a.kind, a.mode].filter(Boolean).join(" · "));
  field(dl, "Session", a.session, true);
  field(dl, "Desktop session", a.desktop_id, true);
  field(dl, "PID", a.pid, true);
  field(dl, "Claude Code", a.version);
  field(dl, "Started", a.started && clock(a.started) + " (" + ago(a.started) + ")");
  field(dl, "Working dir", a.cwd, true);
  field(dl, "Branch", a.branch, true);
  field(dl, "Title", a.title);
  field(dl, "First prompt", a.prompt);
  field(dl, "Relay", a.relay == null ? null : a.relay ? "running (bus messages reach the session directly)" : "none");
  field(dl, "Registry", a.in_registry ? "listed" : "not listed");
  field(dl, "Bus address", a.addr || "not on the bus", true);
  if (a.on_bus) {
    field(dl, "Display name", a.name);
    field(dl, "Last seen", clock(a.seen) + " (" + ago(a.seen) + ")");
    field(dl, "Presence", a.keep ? "kept (never expires)" : "expires " + ago(a.expires));
    field(dl, "Caps", a.caps.join(", "));
    field(dl, "Rooms", a.rooms.join(", "));
    field(dl, "Topics", a.topics.join(", "), true);
  }
  body.append(add(card, dl), resourcesCard(a), controls(a));
}
function controls(a) {
  const card = add(el("div", "card"), el("h4", null, "Process control"));
  if (a.alive === true) {
    card.append(el("div", "muted", "Signals go to pid " + a.pid + " only after re-checking it is still this claude session."));
    const sig = (name, cls) => button(name[0].toUpperCase() + name.slice(1), () => {
      if (confirm("Send " + name + " to " + a.friendly + " (pid " + a.pid + ")?")) act("/api/signal", {...ident(a), signal: name}, name);
    }, cls);
    card.append(add(el("div", "act"), sig("interrupt"), sig("terminate", "danger"), sig("kill", "danger")));
  } else if (a.alive === false && a.kind === "desktop") {
    card.append(el("div", null, "This is a Claude Desktop session. Reopen it in the Claude Desktop app; resuming it here would fork it outside the app."));
  } else if (a.alive === false) {
    card.append(el("div", "muted", "The session is dead. Resume it in a terminal, or run one prompt in the background and watch the reply here."));
    const term = button("Open in terminal", () => {
      if (confirm("Open gnome-terminal running claude --resume " + a.session + " in " + a.cwd + "?")) act("/api/restart/terminal", ident(a), "terminal");
    });
    const ta = el("textarea");
    ta.rows = 2; ta.placeholder = "Message for claude -p --resume";
    ta.value = S.drafts[a.key] || "";
    ta.oninput = () => { S.drafts[a.key] = ta.value; };
    const go = button("Resume with message", async () => {
      if (!ta.value.trim() || !confirm("Run claude -p --resume " + a.session + " with this message?")) return;
      if (await act("/api/restart/resume", {...ident(a), text: ta.value}, "resume")) delete S.drafts[a.key];
    }, "primary");
    card.append(add(el("div", "act"), term), add(el("div", "row act"), ta, go));
  } else {
    card.append(el("div", "muted", "No claude process is recorded for this agent, so it cannot be signalled or restarted."));
  }
  for (const j of Object.values(S.jobs).filter(j => j.key === a.key).slice(-3)) {
    card.append(el("div", "res muted", "Job " + j.id + " · " + clock(j.started) + " · " + j.state));
    const pre = el("pre", "out", j.lines.join("\n") || "(no output yet)");
    pre.id = "job-" + j.id;
    card.append(pre);
  }
  return card;
}
function renderAgentLedger(body, a, l) {
  if (l.error) { body.append(el("div", "empty", l.error)); return; }
  if (l.truncated) body.append(el("div", "card", "The ledger dump was truncated, so this view may be partial."));
  body.append(table("Resources", ["Resource", "Visibility", "Max", "Description"],
    l.resources.map(r => [r.name, r.visibility, r.max_level, r.description])));
  body.append(table("Grants given (requests on its resources)", ["Resource", "Holder", "Level", "Status", ""], l.given.map(requestRow)));
  body.append(table("Grants held (its requests)", ["Resource", "Owner", "Level", "Status", ""],
    l.held.map(r => [r.resource, r.owner, r.level, r.status, reqAction(r)])));
  body.append(table("Leases", ["Resource", "Holder", "Level", "Expires", "Token", ""], l.leases.map(leaseRow)));
  body.append(table("Waiting", ["Resource", "Holder", "Level", "Deadline"], l.waiters.map(waiterRow)));
}
function renderActivity(body, items) {
  if (!items.length) { body.append(el("div", "empty", "No transcript found for this session.")); return; }
  for (const it of items.slice().reverse()) {
    const d = el("div", "msg"), h = el("div", "h");
    add(h, el("span", "who", it.role === "tool" ? "tool" : it.role), it.ts && el("span", "t", new Date(it.ts).toLocaleString()));
    d.append(h, el("div", "b" + (it.role === "tool" ? " mono" : ""), it.text));
    body.append(d);
  }
}

// ledger

function table(title, cols, rows) {
  const card = add(el("div", "card"), el("h4", null, title + " (" + rows.length + ")"));
  if (!rows.length) { card.append(el("div", "muted", "none")); return card; }
  const t = el("table"), hr = el("tr");
  for (const c of cols) hr.append(el("th", null, c));
  t.append(hr);
  for (const r of rows) {
    const tr = el("tr");
    for (const v of r) { const td = el("td"); if (v instanceof Node) td.append(v); else td.textContent = v == null ? "—" : String(v); tr.append(td); }
    t.append(tr);
  }
  return add(card, add(el("div", "tablewrap"), t));
}
function reqAction(r) {
  if (r.status === "denied") return null;
  return button(r.status === "granted" ? "Revoke" : "Deny", () => {
    if (confirm((r.status === "granted" ? "Revoke " : "Deny ") + r.holder + "'s " + r.level + " grant on " + r.resource + "?"))
      act("/api/ledger/revoke", {request_id: r.id}, "revoke").then(refreshLedgerViews);
  }, "danger");
}
function requestRow(r) { return [r.resource, r.holder, r.level, r.status + " · " + ago(r.ts), reqAction(r)]; }
function leaseRow(l) {
  return [l.resource, l.holder, l.level, ago(l.expires_at), l.token == null ? "—" : String(l.token), button("Force release", () => {
    if (confirm("Force-release " + l.holder + "'s " + l.level + " lease on " + l.resource + "?"))
      act("/api/ledger/release", {lease_id: l.lease_id}, "release").then(refreshLedgerViews);
  }, "danger")];
}
function waiterRow(w) { return [w.resource, w.holder, w.level, ago(w.deadline)]; }
async function loadLedger() {
  try { S.ledger = await api("/api/ledger"); } catch (e) { S.ledger = {error: e.message}; }
  if (S.view.type === "ledger") renderPane();
}
function refreshLedgerViews() { if (S.view.type === "ledger") loadLedger(); if (S.view.type === "agent") loadProfile(); }
function renderLedger() {
  const body = $("body"), top = body.scrollTop, d = S.ledger;
  $("compose").hidden = true;
  setTitle("Ledger", d && d.ts ? "as of " + clock(d.ts) : "", [button("Refresh", loadLedger)]);
  body.replaceChildren();
  if (!d) { body.append(el("div", "empty", "Loading…")); return; }
  if (d.error) { body.append(el("div", "empty", d.error)); return; }
  if (d.truncated) body.append(el("div", "card", "The dump was truncated, so lists are partial."));
  for (const r of d.resources) {
    const card = add(el("div", "card"), el("h4", "mono", r.id));
    card.append(el("div", "muted", r.visibility + " · max " + r.max_level + " · owner " + nameOf(r.owner) + (r.description ? " · " + r.description : "")));
    const leases = d.leases.filter(l => l.resource === r.id), waits = d.waiters.filter(w => w.resource === r.id);
    const reqs = d.requests.filter(q => q.resource === r.id);
    if (leases.length) card.append(table("Leases", ["Holder", "Level", "Expires", "Token", ""], leases.map(l => leaseRow(l).slice(1))));
    if (waits.length) card.append(table("Waiting", ["Holder", "Level", "Deadline"], waits.map(w => waiterRow(w).slice(1))));
    if (reqs.length) card.append(table("Requests", ["Holder", "Level", "Status", ""], reqs.map(q => requestRow(q).slice(1))));
    body.append(card);
  }
  if (!d.resources.length) body.append(el("div", "empty", "No resources in any ledger."));
  body.append(table("Ledgers", ["Owner", "Since", "Last request"], d.ledgers.map(x => [x.owner, clock(x.since), ago(x.last_seen)])));
  body.scrollTop = top;
}

// audit

// kill switch: a whole room, or one agent ("stop")

function killScope() {
  const v = S.view;
  return v.type === "kill" ? {room: v.room, name: v.room, path: "/api/room/kill"} : {addr: v.addr, name: v.addr, path: "/api/agent/stop"};
}
async function loadKillPreview() {
  const k = S.kill, sc = killScope(), here = viewKey(S.view);
  const q = new URLSearchParams({...(sc.room ? {room: sc.room} : {addr: sc.addr}), signal: k.signal, kill_sessions: String(k.sessions)});
  try { k.preview = await api(sc.path + "?" + q); } catch (e) { k.preview = {error: e.message}; }
  if (k.preview.targets) k.selected = new Set(k.preview.targets.filter(t => !t.action.startsWith("skip")).map(t => t.addr));
  if (viewKey(S.view) === here) renderKill();
}
function targetRows(targets, withResult, selected) {
  return targets.map(t => {
    const who = el("div", null, t.friendly);
    who.append(el("div", "muted mono", t.addr));
    const act = el("div", null, t.action);
    if (t.reason) act.append(el("div", "muted", "why: " + t.reason));
    const row = [who, el("span", "nowrap", t.class), act];
    if (withResult) row.push(t.result);
    if (selected) {
      const box = el("input"); box.type = "checkbox"; box.checked = selected.has(t.addr);
      box.disabled = t.action.startsWith("skip");
      box.onchange = () => { box.checked ? selected.add(t.addr) : selected.delete(t.addr); renderKill(); };
      row.unshift(box);
    }
    return row;
  });
}
function renderKill() {
  const sc = killScope(), k = S.kill, body = $("body"), room = sc.room;
  $("compose").hidden = true;
  const back = room ? button("Back to room", () => go({type: "chan", id: "room:" + room}))
    : button("Back to agent", () => go({type: "agent", key: S.view.key}));
  setTitle(room ? "Kill # " + room : "Stop " + sc.addr, room ? "Stops the agents of the room" : "Stops this one agent", [back]);
  body.replaceChildren();
  const card = add(el("div", "card"), el("h4", null, "How"));
  card.append(el("div", "muted", "By default only subagents stop: each loses its own agent-bridge processes and " +
    "its bus identity, and its parent session keeps running. Whole Claude sessions are killed only when you tick " +
    "the box, and only through each session's own primary agent. Agents whose process cannot be verified are only " +
    "wiped from the bus."));
  const sel = el("select");
  for (const [v, t] of [["terminate", "SIGTERM (terminate)"], ["kill", "SIGKILL (kill)"]]) {
    const o = el("option", null, t); o.value = v; o.selected = k.signal === v; sel.append(o);
  }
  sel.onchange = () => { k.signal = sel.value; k.results = null; loadKillPreview(); };
  const whole = el("input"); whole.type = "checkbox"; whole.checked = k.sessions;
  whole.onchange = () => { k.sessions = whole.checked; k.results = null; loadKillPreview(); };
  const lbl = add(el("label"), whole, el("span", null, " also kill whole sessions"));
  card.append(add(el("div", "act"), sel, lbl));
  body.append(card);
  if (k.results) {
    body.append(table("Results", ["Agent", "Class", "Action", "Result"], targetRows(k.results, true)));
    return;
  }
  if (!k.preview) { body.append(el("div", "empty", "Loading…")); return; }
  if (k.preview.error) { body.append(el("div", "empty", k.preview.error)); return; }
  const targets = k.preview.targets;
  body.append(room ? table("Preview: what will happen (untick to leave a member alone)", ["", "Agent", "Class", "Action"], targetRows(targets, false, k.selected))
    : table("Preview: what will happen", ["Agent", "Class", "Action"], targetRows(targets)));
  if (!targets.length || (room && !k.selected.size)) return;
  const conf = add(el("div", "card"), el("h4", null, "Confirm"));
  const input = el("input"); input.placeholder = "type " + sc.name + " to confirm"; input.autocomplete = "off";
  const all = !room || targets.filter(t => !t.action.startsWith("skip")).every(t => k.selected.has(t.addr));
  const go_ = button(room ? (all ? "Kill room" : "Kill " + k.selected.size + " selected") : "Stop agent", async () => {
    const body = {...(room ? {room} : {addr: sc.addr}), confirm: input.value, signal: k.signal, kill_sessions: k.sessions};
    if (room && !all) body.only = [...k.selected];
    const r = await act(sc.path, body, room ? "kill room" : "stop agent");
    if (r) { k.results = r.targets; renderKill(); }
  }, "danger");
  go_.disabled = true;
  input.oninput = () => { go_.disabled = input.value !== sc.name; };
  conf.append(add(el("div", "row act"), input, go_));
  body.append(conf);
}

// capacity: CPU and memory per session

function bytes(n) {
  if (n == null) return "—";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : String(n)) + " " + u[i];
}
function num(v) { return el("span", "nowrap", v); }
function pct(v) { return v == null ? "…" : v.toFixed(1) + "%"; }
async function loadCapacity() {
  try { S.capacity = await api("/api/capacity"); } catch (e) { S.capacity = {error: e.message}; }
  if (S.view.type === "capacity") renderCapacity();
  else if (S.view.type === "agent" && S.tab === "overview" && $("resources")) {
    const a = byKey(S.view.key);
    if (a) $("resources").replaceWith(resourcesCard(a));
  }
}
function sessionLink(key) {
  const a = byKey(key);
  if (!a) return key || "—";
  const l = el("a", null, a.friendly); l.href = "#"; l.onclick = e => { e.preventDefault(); go({type: "agent", key}); };
  return l;
}
function procRows(rows, withSession) {
  return rows.map(r => {
    const cmd = el("div", "mono", r.cmd);
    const row = [num(r.pid), cmd, num(pct(r.cpu)), num(bytes(r.rss))];
    if (withSession) row.push(r.session ? sessionLink(r.session) : r.mine ? "outside any session" : "other user (" + r.comm + ")");
    return row;
  });
}
function renderCapacity() {
  const c = S.capacity, body = $("body"), top = body.scrollTop;
  $("compose").hidden = true;
  setTitle("Capacity", "CPU is the share of one core since the last sample; memory is resident size, shared pages counted per process", [button("Refresh", loadCapacity)]);
  body.replaceChildren();
  if (!c) { body.append(el("div", "empty", "Loading…")); return; }
  if (c.error) { body.append(el("div", "empty", c.error)); return; }
  const m = c.machine, card = add(el("div", "card"), el("h4", null, "Machine"));
  const used = m.mem_total && m.mem_available != null ? m.mem_total - m.mem_available : null;
  card.append(el("div", null, "load " + (m.load ? m.load.join(" · ") : "—") + " on " + m.cpus + " cores · memory " +
    bytes(used) + " used of " + bytes(m.mem_total) + (c.first ? " · CPU appears from the next sample" : "")));
  body.append(card);
  body.append(table("Claude sessions, by CPU (each with everything it started)", ["Session", "Kind", "CPU", "Memory", "Processes"],
    c.sessions.map(r => [sessionLink(r.key), r.kind || "—", num(pct(r.cpu)), num(bytes(r.rss)), num(r.procs)])));
  body.append(table("Top processes on the machine", ["PID", "Command", "CPU", "Memory", "Belongs to"], procRows(c.top, true)));
  body.scrollTop = top;
}
function resourcesCard(a) {
  const c = S.capacity, row = c && c.sessions && c.sessions.find(r => r.pid === a.pid);
  const card = add(el("div", "card"), el("h4", null, "Resources"));
  card.id = "resources";
  if (!a.alive) { card.append(el("div", "muted", "Not running.")); return card; }
  if (!row) { card.append(el("div", "muted", "Measuring…")); return card; }
  card.append(el("div", null, pct(row.cpu) + " CPU · " + bytes(row.rss) + " memory · " + row.procs + " processes (the session and everything it started)"));
  const t = table("Busiest processes", ["PID", "Command", "CPU", "Memory"], procRows(row.tree));
  t.classList.remove("card");
  card.append(t);
  return card;
}

// leftovers: test containers and orphaned processes

async function loadLeaks() {
  try { S.leaks = await api("/api/leaks"); } catch (e) { S.leaks = {error: e.message}; }
  if (S.view.type === "leaks") renderLeaks();
}
async function leakAct(path, body, ask) {
  if (confirm(ask) && await act(path, body, "cleanup")) loadLeaks();
}
function renderLeaks() {
  const d = S.leaks, body = $("body");
  $("compose").hidden = true;
  setTitle("Leftovers", "Test containers and agent-bridge processes that outlived what started them", [button("Refresh", loadLeaks)]);
  body.replaceChildren();
  if (!d) { body.append(el("div", "empty", "Loading…")); return; }
  if (d.error) { body.append(el("div", "empty", d.error)); return; }
  if (d.busy) body.append(add(el("div", "card"), el("h4", null, "Test cleanup is paused"), el("div", null, d.busy +
    ". Test containers and test processes may be in use; they can be removed once no test run is active.")));
  body.append(table("Test broker containers", ["Name", "Status", "Created", ""], d.containers.map(c => {
    const b = button("Remove", () => leakAct("/api/leaks/container", {name: c.name}, "docker rm -f " + c.name + "?"), "danger");
    b.disabled = !!d.busy;
    return [el("span", "mono", c.name), c.status, c.created, b];
  })));
  body.append(table("Orphaned agent-bridge processes", ["PID", "Command", "Why", "Memory", ""], d.processes.map(p => {
    const b = button("Stop", () => leakAct("/api/leaks/process", {pid: p.pid, start: p.start}, "SIGTERM pid " + p.pid + "?\n\n" + p.cmd), "danger");
    b.disabled = !!d.busy && p.kind === "test";
    return [num(p.pid), el("div", "mono", p.cmd), p.reason, num(bytes(p.rss)), b];
  })));
}

// room settings

function roomInfo(room) {
  return S.rooms[room] || {room, mode: "open", moderators: [], rules: "", muted: [], banned: []};
}
function roomAct(path, body, what, ask) {
  if (ask && !confirm(ask)) return;
  act(path, {room: S.view.room, ...body}, what);
}
function renderRoomSettings() {
  const room = S.view.room, r = roomInfo(room), body = $("body"), top = body.scrollTop;
  $("compose").hidden = true;
  setTitle("Settings # " + room, r.by ? "last changed by " + r.by + (r.ts ? " " + ago(r.ts) : "") : "open, no moderators set",
    [button("Back to room", () => go({type: "chan", id: "room:" + room}))]);
  body.replaceChildren();
  const card = add(el("div", "card"), el("h4", null, "Mode and rules"));
  const mode = el("select");
  for (const [v, t] of [["open", "open: anyone may post"], ["announce", "announce: only moderators post"]]) {
    const o = el("option", null, t); o.value = v; o.selected = r.mode === v; mode.append(o);
  }
  const rules = el("textarea"); rules.rows = 4; rules.maxLength = 2048; rules.value = S.drafts["rules:" + room] ?? r.rules;
  rules.placeholder = "Room rules shown to members";
  rules.oninput = () => { S.drafts["rules:" + room] = rules.value; };
  const save = button("Save", async () => {
    if (await act("/api/room/settings", {room, mode: mode.value, rules: rules.value}, "settings")) delete S.drafts["rules:" + room];
  }, "primary");
  card.append(add(el("div", "act"), mode), add(el("div", "act"), rules), add(el("div", "act"), save));
  body.append(card);
  const members = S.agents.filter(a => a.addr && a.rooms.includes(room));
  const mods = r.moderators.map(a => [nameOf(a), a, button("Remove", () => roomAct("/api/room/moderator", {action: "remove", addr: a}, "moderator"))]);
  const modCard = table("Moderators", ["Agent", "Address", ""], mods);
  const pick = el("select");
  for (const a of members.filter(a => !r.moderators.includes(a.addr) && a.addr !== ADMIN)) {
    const o = el("option", null, a.friendly + " (" + a.addr + ")"); o.value = a.addr; pick.append(o);
  }
  if (pick.options.length) modCard.append(add(el("div", "act"), pick, button("Make moderator", () => roomAct("/api/room/moderator", {action: "add", addr: pick.value}, "moderator"))));
  body.append(modCard);
  const state = a => [r.moderators.includes(a) && "moderator", r.muted.includes(a) && "muted", r.banned.includes(a) && "banned"].filter(Boolean).join(", ") || "member";
  const actions = a => {
    const box = el("div", "act");
    const muted = r.muted.includes(a), banned = r.banned.includes(a);
    box.append(button(muted ? "Unmute" : "Mute", () => roomAct("/api/room/member", {action: muted ? "unmute" : "mute", addr: a}, muted ? "unmute" : "mute")));
    box.append(button(banned ? "Unban" : "Ban", () => roomAct("/api/room/member", {action: banned ? "unban" : "ban", addr: a}, banned ? "unban" : "ban",
      banned ? null : "Ban " + a + " from # " + room + "? It can no longer post or join."), banned ? null : "danger"));
    if (!banned) box.append(button("Kick", () => roomAct("/api/room/member", {action: "kick", addr: a}, "kick",
      "Kick " + a + " from # " + room + "? This bans it and drops its subscription; its session reconnects in about a second."), "danger"));
    return box;
  };
  body.append(table("Members", ["Agent", "State", ""], members.filter(a => a.addr !== ADMIN).map(a => {
    const who = el("div", null, a.friendly); who.append(el("div", "muted mono", a.addr));
    return [who, state(a.addr), actions(a.addr)];
  })));
  const listed = [...new Set([...r.muted, ...r.banned])].filter(a => !members.some(m => m.addr === a));
  if (listed.length) body.append(table("Muted or banned, not in the room", ["Address", "State", ""], listed.map(a => [a, state(a), actions(a)])));
  body.scrollTop = top;
}

const RANGES = [["all", "any time", 0], ["1h", "last hour", 3600], ["24h", "last day", 86400], ["7d", "last week", 604800]];
async function loadAudit() {
  const f = S.auditQ, q = new URLSearchParams();
  if (f.action) q.set("action", f.action);
  if (f.target) q.set("target", f.target);
  const span = RANGES.find(r => r[0] === f.range)[2];
  if (span) q.set("since", String(Date.now() / 1000 - span));
  try { S.auditRes = await api("/api/audit?" + q); } catch (e) { S.auditRes = {error: e.message}; }
  if (S.view.type === "audit") renderAudit();
}
function targetLink(t) {
  // Targets read "room:<room>", "<room or kind>:<address>" or a bare address; link the agent, else the room.
  if (!t) return "—";
  const [head, tail] = t.includes(":") ? t.split(":", 2) : [null, t];
  const link = v => { const l = el("a", null, t); l.href = "#"; l.onclick = e => { e.preventDefault(); go(v); }; return l; };
  const a = head !== "room" && agentOf(tail);
  if (a) return link({type: "agent", key: a.key});
  const room = head === "room" ? tail : head;
  return room && channels().rooms.includes("room:" + room) ? link({type: "chan", id: "room:" + room}) : t;
}
function renderAudit() {
  $("compose").hidden = true;
  setTitle("Audit log", "Every admin action, newest first (admin-audit.jsonl in the state directory)", [button("Refresh", loadAudit)]);
  const body = $("body"), f = S.auditQ, r = S.auditRes;
  const focused = document.activeElement && document.activeElement.id === "audit-target";
  body.replaceChildren();
  const act_ = el("select");
  for (const a of ["", ...((r && r.actions) || [])]) {
    const o = el("option", null, a || "any action"); o.value = a; o.selected = f.action === a; act_.append(o);
  }
  act_.onchange = () => { f.action = act_.value; loadAudit(); };
  const tgt = el("input"); tgt.id = "audit-target"; tgt.placeholder = "target contains…"; tgt.value = f.target;
  tgt.oninput = () => { f.target = tgt.value; clearTimeout(S.auditTimer); S.auditTimer = setTimeout(loadAudit, 300); };
  const rng = el("select");
  for (const [v, t] of RANGES) { const o = el("option", null, t); o.value = v; o.selected = f.range === v; rng.append(o); }
  rng.onchange = () => { f.range = rng.value; loadAudit(); };
  body.append(add(el("div", "card"), add(el("div", "act"), act_, tgt, rng)));
  if (focused) { tgt.focus(); tgt.selectionStart = tgt.selectionEnd = tgt.value.length; }
  if (!r) { body.append(el("div", "empty", "Loading…")); return; }
  if (r.error) { body.append(el("div", "empty", r.error)); return; }
  body.append(table("Actions", ["When", "Who", "Action", "Target", "Result"],
    r.entries.slice().reverse().map(e => [clock(e.ts), e.who + " " + (e.peer || ""), e.action, targetLink(e.target), e.result])));
}

function renderPane(fresh) {
  const v = S.view;
  if (v.type === "chan") renderChannel(v.id, fresh);
  else if (v.type === "agent") renderAgent();
  else if (v.type === "ledger") renderLedger();
  else if (v.type === "kill") renderKill();
  else if (v.type === "roomset") renderRoomSettings();
  else if (v.type === "stop") renderKill();
  else if (v.type === "capacity") renderCapacity();
  else if (v.type === "leaks") renderLeaks();
  else renderAudit();
}
function render(fresh) {
  renderNav();
  renderPane(fresh);
}

// live updates

function onEvent(ev) {
  if (ev.kind === "agents") {
    const before = new Map(S.agents.map(a => [a.key, a.alive]));
    const was = S.view.type === "agent" && JSON.stringify(byKey(S.view.key));
    const roomSig = () => JSON.stringify(S.agents.filter(a => a.rooms.includes(S.view.room)).map(a => [a.addr, a.friendly]));
    const roomMembers = S.view.type === "roomset" && roomSig();
    S.agents = ev.agents;
    for (const a of S.agents) {
      if (before.get(a.key) === true && a.alive === false) toast(a.friendly + " died");
      if (before.get(a.key) === false && a.alive === true) toast(a.friendly + " is alive again");
    }
    renderNav();
    if (S.view.type === "agent" && was !== JSON.stringify(byKey(S.view.key))) renderAgent();
    else if (S.view.type === "roomset" && roomMembers !== roomSig()) renderRoomSettings();
    else if (S.view.type === "chan") renderChannel(S.view.id);
  } else if (ev.kind === "msg") {
    const m = ev.msg, c = chanOf(m);
    S.msgs.push(m);
    if (S.msgs.length > 5000) S.msgs.shift();
    const looking = S.view.type === "chan" && S.view.id === c && !document.hidden;
    if (!looking && m.from !== ADMIN) S.unread[c] = (S.unread[c] || 0) + 1;
    if (!looking && (m.mentions || []).includes(ADMIN)) S.atme[c] = (S.atme[c] || 0) + 1;
    renderNav();
    if (S.view.type === "chan" && S.view.id === c) renderChannel(c);
    if (S.view.type === "agent" && S.tab === "dms" && m.topic.startsWith("inbox/")) loadProfile();
  } else if (ev.kind === "room") {
    S.rooms[ev.info.room] = ev.info;
    if (S.view.room === ev.info.room && S.view.type === "roomset") renderRoomSettings();
    if (S.view.type === "chan" && S.view.id === "room:" + ev.info.room) renderChannel(S.view.id);
  } else if (ev.kind === "audit") {
    S.audit.push(ev.entry);
    if (S.view.type === "audit") loadAudit();
  } else if (ev.kind === "job") {
    const j = S.jobs[ev.id] = S.jobs[ev.id] || {id: ev.id, key: ev.key, started: Date.now() / 1000, state: "running", lines: []};
    if (ev.line != null) j.lines.push(ev.line);
    if (ev.state) j.state = ev.state;
    const pre = $("job-" + ev.id);
    if (pre && ev.line != null && !ev.state) { pre.textContent = j.lines.join("\n"); pre.scrollTop = pre.scrollHeight; }
    else if (S.view.type === "agent" && S.view.key === ev.key) renderAgent();
  }
}
function setConn(on) { S.connected = on; $("dot").classList.toggle("on", on); }
async function loadState() {
  const st = await api("/api/state");
  S.agents = st.agents; S.msgs = st.messages; S.audit = st.audit; S.rooms = st.rooms || {};
  S.jobs = Object.fromEntries(st.jobs.map(j => [j.id, j]));
  render(true);
}
async function stream() {
  for (;;) {
    try {
      await loadState();
      const r = await fetch("/api/stream", {headers: {"X-Admin-Token": S.token}});
      if (r.status === 401) { showLogin(); return; }
      setConn(true);
      const rd = r.body.pipeThrough(new TextDecoderStream()).getReader();
      let buf = "";
      for (;;) {
        const {value, done} = await rd.read();
        if (done) break;
        buf += value;
        let i;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const chunk = buf.slice(0, i);
          buf = buf.slice(i + 2);
          for (const line of chunk.split("\n")) if (line.startsWith("data: ")) onEvent(JSON.parse(line.slice(6)));
        }
      }
    } catch (e) {
      if (!$("login").hidden) return;
    }
    setConn(false);
    await new Promise(r => setTimeout(r, 2000));
  }
}

// login and theme

function storage(k, v) {
  try { if (v === undefined) return localStorage.getItem(k); if (v === null) localStorage.removeItem(k); else localStorage.setItem(k, v); }
  catch (e) { return null; }
}
function showLogin() { storage("adminToken", null); S.token = null; $("login").hidden = false; setConn(false); }
function applyTheme() {
  const t = storage("theme") || "auto";
  if (t === "auto") delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme = t;
  $("theme").textContent = t === "dark" ? "☾" : t === "light" ? "☀" : "◐";
  $("theme").title = "Theme: " + t;
}

function boot() {
  const m = location.hash.match(/token=([A-Za-z0-9_-]+)/);
  if (m) { storage("adminToken", m[1]); history.replaceState(null, "", location.pathname); }
  S.token = (m && m[1]) || storage("adminToken");
  applyTheme();
  $("theme").onclick = () => {
    const order = ["auto", "light", "dark"];
    storage("theme", order[(order.indexOf(storage("theme") || "auto") + 1) % 3]);
    applyTheme();
  };
  $("menu").onclick = () => $("nav").classList.toggle("open");
  $("bcast").onclick = () => { go({type: "chan", id: "broadcast"}); $("text").focus(); };
  $("compose").onsubmit = sendCompose;
  $("text").onkeydown = e => {
    if (mentionKey(e)) return;
    if (e.key === "Enter" && !e.shiftKey) sendCompose(e);
  };
  $("text").oninput = mentionSuggest;
  $("text").onblur = () => setTimeout(mentionClose, 150);
  $("loginf").onsubmit = e => {
    e.preventDefault();
    S.token = $("tok").value.trim();
    storage("adminToken", S.token);
    $("login").hidden = true;
    stream();
  };
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && S.view.type === "chan") { delete S.unread[S.view.id]; delete S.atme[S.view.id]; renderNav(); }
  });
  setInterval(() => {
    if (S.view.type === "agent") { loadProfile(); if (S.tab === "overview") loadCapacity(); }
    else if (S.view.type === "ledger") loadLedger();
  }, 5000);
  setInterval(() => { if (S.view.type === "capacity") loadCapacity(); }, 3000);
  setInterval(() => { if (S.view.type === "leaks") loadLeaks(); }, 10000);
  setInterval(() => { if (S.view.type === "chan") renderNav(); }, 30000);
  if (!S.token) { showLogin(); return; }
  stream();
}
boot();
