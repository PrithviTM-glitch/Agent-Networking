// Message markup for the console page: ``` fenced blocks render as monospace <pre>, and Markdown pipe tables (a
// header row, then a |---| separator, alignment colons allowed) as a real <table>, short rows unpadded. Everything is built as DOM nodes
// with textContent only, so message text can never inject markup; anything malformed (an unclosed fence, a table
// without its separator or with a mismatched one) stays plain text. No DOM is assumed: render() takes the document,
// so the tests run it under node with a stub.
"use strict";
const Markup = (() => {
  const FENCE = /^\s*```/;
  const SEP_CELL = /^:?-+:?$/; // GitHub accepts one or more dashes per separator cell
  // Bounds on what a table may build: every row is padded to the header's width, so an uncapped 64 KiB message could
  // ask for a hundred million cells and freeze every page showing it. A bigger block stays plain text.
  const MAX_COLS = 50;
  const MAX_ROWS = 500;
  // And on how many blocks one message makes: each is an element where plain text was one node, so a backlog of
  // messages full of tiny fences would reach millions of nodes. Past the cap the rest of the body is one text run.
  const MAX_BLOCKS = 200;
  // A cell this short (an MR id, a state, a count) never wraps, so a narrow column can't split it mid-token.
  const NOWRAP_MAX = 12;

  // A table row's cells: outer pipes dropped, split on unescaped |, a \| kept as a literal |.
  function cells(line) {
    let s = line.trim();
    if (s.startsWith("|")) s = s.slice(1);
    if (s.endsWith("|") && !s.endsWith("\\|")) s = s.slice(0, -1);
    const out = [];
    let cur = "";
    for (let i = 0; i < s.length; i++) {
      if (s[i] === "\\" && s[i + 1] === "|") {
        cur += "|";
        i++;
      } else if (s[i] === "|") {
        out.push(cur.trim());
        cur = "";
      } else cur += s[i];
    }
    out.push(cur.trim());
    return out;
  }

  function alignment(sep, n) {
    const a = cells(sep);
    if (a.length !== n || !a.every(c => SEP_CELL.test(c))) return null;
    return a.map(c => (c.startsWith(":") && c.endsWith(":") ? "center" : c.endsWith(":") ? "right" : c.startsWith(":") ? "left" : ""));
  }

  // The body as segments: {kind: "text", text}, {kind: "code", text} or {kind: "table", head, align, rows}.
  function blocks(text) {
    const lines = String(text).split("\n"), out = [];
    let plain = [];
    const flush = () => {
      if (plain.length) out.push({kind: "text", text: plain.join("\n")});
      plain = [];
    };
    let closers = true; // false once an opener found no closer: none after a later line either, so stop looking
    let made = 0;
    for (let i = 0; i < lines.length; ) {
      if (made === MAX_BLOCKS) {
        for (; i < lines.length; i++) plain.push(lines[i]);
        break;
      }
      const line = lines[i];
      if (closers && FENCE.test(line)) {
        let end = -1;
        for (let j = i + 1; j < lines.length; j++) if (lines[j].trim() === "```") { end = j; break; } // from i, linear
        if (end < 0) closers = false;
        if (end > i) {
          flush();
          out.push({kind: "code", text: lines.slice(i + 1, end).join("\n")});
          made++;
          i = end + 1;
          continue;
        }
      } else if (line.trim().startsWith("|") && i + 1 < lines.length) {
        const head = cells(line), align = head.length <= MAX_COLS && alignment(lines[i + 1], head.length);
        if (align) {
          let j = i + 2;
          while (j < lines.length && lines[j].trim().startsWith("|")) j++;
          if (j - i - 2 <= MAX_ROWS) {
            flush();
            out.push({kind: "table", head, align, rows: lines.slice(i + 2, j).map(cells)});
            made++;
            i = j;
            continue;
          }
          for (; i < j; i++) plain.push(lines[i]); // too many rows: the whole block as text, scanned once
          continue;
        }
      }
      plain.push(line);
      i++;
    }
    flush();
    return out;
  }

  function cell(doc, tag, text, align) {
    const c = doc.createElement(tag);
    c.textContent = text;
    if (text.length <= NOWRAP_MAX) c.className = "nw";
    if (align) c.style.textAlign = align;
    return c;
  }

  // Append text's segments to parent; inline(parent, text) renders a plain-text run (the page adds mentions there).
  function render(doc, parent, text, inline) {
    for (const seg of blocks(text)) {
      if (seg.kind === "text") inline(parent, seg.text);
      else if (seg.kind === "code") {
        const pre = doc.createElement("pre");
        pre.className = "out code";
        pre.textContent = seg.text;
        parent.append(pre);
      } else {
        // Only the cells a row has, up to the header's width: never padded, so a table can't build more cells than
        // its text has pipes, and a short row just ends early.
        const row = (values, tag) => {
          const tr = doc.createElement("tr");
          values.slice(0, seg.head.length).forEach((v, k) => tr.append(cell(doc, tag, v, seg.align[k])));
          return tr;
        };
        const wrap = doc.createElement("div"), table = doc.createElement("table");
        const thead = doc.createElement("thead"), tbody = doc.createElement("tbody");
        wrap.className = "mdtable";
        thead.append(row(seg.head, "th"));
        for (const r of seg.rows) tbody.append(row(r, "td"));
        table.append(thead, tbody);
        wrap.append(table);
        parent.append(wrap);
      }
    }
  }

  return {blocks, cells, render, MAX_COLS, MAX_ROWS, MAX_BLOCKS};
})();
if (typeof module !== "undefined") module.exports = Markup;
