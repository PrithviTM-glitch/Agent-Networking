// Auto-links for the console page. The operator's rules (console/links.py checks them; /api/state serves them) turn a
// matching token into a link; git hashes and paths become click-to-copy. Text is split on whitespace and only tokens
// of at most TOKEN_MAX characters are matched, each against an anchored pattern, so a rule's cost stays bounded. Links
// are built from textContent and the href property only, and each href is checked again: http(s), on the template's
// own origin, message text percent-encoded. No DOM is assumed: render() takes the document, so the tests run it under
// node with a stub.
"use strict";
const Links = (() => {
  const TOKEN_MAX = 200;
  const MAX_TOKENS = 2000; // tokens matched per text run; the rest of a huge body stays plain
  const LEAD = /^[(\[{"'<]+/;
  const TRAIL = /[)\]}"'>.,;:!?]+$/;
  // A git hash has a digit and a letter, so plain numbers and words stay text.
  const SHA = /^(?=[0-9a-f]*[0-9])(?=[0-9a-f]*[a-f])[0-9a-f]{7,40}$/;
  const PATH = /^(?:~|\.\.?)?\/[\w.~+@%=-][\w.~+@%=\/:-]*$/;

  function origin(url) {
    try {
      return new URL(url.replace(/\{[1-3]\}/g, "")).origin;
    } catch (e) {
      return null;
    }
  }

  // The page's rules: each compiled once, anchored; one that this engine refuses is dropped.
  function compile(rules) {
    const out = [];
    for (const r of rules || []) {
      try {
        const at = origin(r.url);
        if (at) out.push({name: r.name, re: new RegExp("^(?:" + r.pattern + ")$"), url: r.url, origin: at});
      } catch (e) {
        // a pattern this engine refuses never runs
      }
    }
    return out;
  }

  // The link for a token, or null. Captured text is percent-encoded into the path, query or fragment only; a capture
  // of "." or ".." (which encoding keeps, and URL parsing would resolve to another path) links nothing.
  function href(rule, token) {
    const m = rule.re.exec(token);
    if (!m || m.slice(1).some(g => g === "." || g === "..")) return null;
    const h = rule.url.replace(/\{([1-3])\}/g, (_, n) => encodeURIComponent(m[+n] === undefined ? "" : m[+n]));
    try {
      const u = new URL(h);
      return (u.protocol === "https:" || u.protocol === "http:") && u.origin === rule.origin ? u.href : null;
    } catch (e) {
      return null;
    }
  }

  // Append text to parent with links and copy spans. copy(text) is called on a click (the page's clipboard helper).
  function render(doc, parent, text, rules, copy) {
    let plain = "", seen = 0;
    const flush = () => {
      if (plain) parent.append(doc.createTextNode(plain));
      plain = "";
    };
    for (const part of String(text).split(/(\s+)/)) {
      if (!part) continue;
      if (/^\s/.test(part) || part.length > TOKEN_MAX || ++seen > MAX_TOKENS) {
        plain += part;
        continue;
      }
      const lead = (part.match(LEAD) || [""])[0];
      const rest = part.slice(lead.length);
      const trail = (rest.match(TRAIL) || [""])[0];
      const core = rest.slice(0, rest.length - trail.length);
      let node = null;
      if (core) {
        for (const r of rules || []) {
          const h = href(r, core);
          if (h) {
            node = doc.createElement("a");
            node.className = "autolink";
            node.href = h;
            node.rel = "noopener noreferrer";
            node.target = "_blank";
            node.title = r.name;
            node.textContent = core;
            break;
          }
        }
        if (!node && (SHA.test(core) || PATH.test(core))) {
          node = doc.createElement("span");
          node.className = "copy";
          node.title = "click to copy";
          node.textContent = core;
          if (copy) node.onclick = () => copy(core, node);
        }
      }
      if (!node) {
        plain += part;
        continue;
      }
      plain += lead;
      flush();
      parent.append(node);
      plain += trail;
    }
    flush();
  }

  return {compile, href, render, TOKEN_MAX, MAX_TOKENS};
})();
if (typeof module !== "undefined") module.exports = Links;
