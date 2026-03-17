// ─── State ────────────────────────────────────────────────────────────────
  let isAuthenticated = false;
  let sdeReady        = false;

  // Character/corp order overlay
  let charOrders = {};  // type_id → {char_buy, char_sell}
  let corpOrders = {};  // type_id → {corp_buy, corp_sell}

  // Selected market locations
  const market = {
    src: { id: null, name: null, type: null },  // type: 'npc'|'freeport'|'structure'
    dst: { id: null, name: null, type: null },
  };

  // Table state
  let allRows      = [];
  let filteredRows = [];
  let sortCol      = 'type_name';
  let sortDir      = 1;
  let page         = 1;
  const PAGE_SIZE  = 200;

  // System search timers per side
  const sysSearchTimer = { src: null, dst: null };

  // ─── Init ─────────────────────────────────────────────────────────────────
  window.onload = () => {
    document.getElementById('headerDate').textContent =
      new Date().toUTCString().slice(0,16) + ' UTC';

    // OAuth popup detection
    if (window.opener && !window.opener.closed) {
      fetch('/api/auth/status').then(r => r.json()).then(d => {
        if (d.authenticated) {
          window.opener.postMessage({type: 'eve_auth_ok', character: d.character_name}, '*');
          window.close();
        } else {
          document.body.innerHTML = '<div style="background:#050a0f;color:#ff3b5c;font-family:monospace;padding:40px;text-align:center"><h2>Authentication failed.</h2><p>You may close this window.</p></div>';
        }
      });
      return;
    }

    window.addEventListener('message', e => {
      if (e.data && e.data.type === 'eve_auth_ok') {
        checkAuthStatus();
        toast('Logged in as ' + e.data.character + '.');
      }
    });

    // Restore exclude skins preference
    const savedExcl = localStorage.getItem('excludeSkins');
    document.getElementById('excludeSkins').checked =
      savedExcl === null ? true : savedExcl === '1';

    checkSdeStatus();
    setTimeout(checkAuthStatus, 150);
    loadNpcHubs();
    loadFreeports();
    setTimeout(checkForUpdate, 5000);  // check after 5s so it doesn't block startup
  };

  async function checkForUpdate() {
    try {
      const res  = await fetch('/api/update/check');
      if (!res.ok) return;
      const data = await res.json();
      if (data.up_to_date) return;

      const badge = document.getElementById('updateBadge');
      badge.style.display = 'inline-flex';
      badge.style.gap = '6px';
      badge.style.alignItems = 'center';

      // Always show a link to the release notes
      const releaseLink =
        `<a href="${data.url}" target="_blank"
            style="color:var(--accent2);text-decoration:none;font-family:var(--mono);
                   font-size:10px;letter-spacing:1px"
            title="${(data.notes || 'New release available').replace(/"/g,'&quot;')}">
           ↑ ${data.latest}
         </a>`;

      if (data.can_update) {
        // Running from git — offer a one-click update
        badge.innerHTML = releaseLink +
          `<button id="btnApplyUpdate"
              style="background:rgba(0,200,255,0.15);border:1px solid var(--accent2);
                     color:var(--accent2);font-family:var(--mono);font-size:10px;
                     letter-spacing:1px;padding:3px 8px;cursor:pointer;border-radius:2px"
              onclick="applyUpdate(this, '${data.latest}')">
             UPDATE
           </button>`;
      } else {
        // Not a git clone — just show the version link
        badge.innerHTML = releaseLink;
      }
    } catch(e) {}
  }

  async function applyUpdate(btn, version) {
    btn.disabled    = true;
    btn.textContent = 'UPDATING…';

    try {
      const res  = await fetch('/api/update/apply', {method: 'POST'});
      const data = await res.json();

      if (!data.ok) {
        btn.textContent = 'FAILED';
        btn.style.color = 'var(--danger)';
        toast(data.error || 'Update failed.', true);
        return;
      }

      btn.textContent = 'RESTARTING…';
      toast(`Updated to ${version}. Reloading…`);

      // Poll until the server comes back up, then reload
      await _waitForRestart();
      window.location.reload();

    } catch(e) {
      btn.textContent = 'FAILED';
      btn.style.color = 'var(--danger)';
      toast('Update request failed.', true);
    }
  }

  async function _waitForRestart(attempts = 0) {
    if (attempts > 30) return;  // give up after ~15s
    await new Promise(r => setTimeout(r, 500));
    try {
      const res = await fetch('/api/update/check', {cache: 'no-store'});
      if (res.ok) return;  // server is back
    } catch(e) {
      // Server is restarting — keep waiting
    }
    return _waitForRestart(attempts + 1);
  }

  // ─── Auth ─────────────────────────────────────────────────────────────────
  async function checkAuthStatus() {
    try {
      const res  = await fetch('/api/auth/status');
      const data = await res.json();
      const badge     = document.getElementById('authBadge');
      const btnLogin  = document.getElementById('btnLogin');
      const cidStatus = document.getElementById('clientIdStatus');

      isAuthenticated = data.authenticated;

      if (data.authenticated) {
        badge.textContent = '✓ ' + data.characters.length + ' character' + (data.characters.length !== 1 ? 's' : '');
        badge.className   = 'status-badge ok';
        btnLogin.style.display = 'inline-flex';  // always allow adding more characters
      } else {
        badge.textContent = 'NOT LOGGED IN';
        badge.className   = 'status-badge missing';
        btnLogin.style.display = data.has_client_id ? 'inline-flex' : 'none';
      }

      // Render character list
      renderCharacterList(data.characters || []);

      // Client ID status
      if (data.has_client_id) {
        cidStatus.innerHTML = '<span style="color:var(--accent3)">✓ Client ID saved</span>';
        document.getElementById('clientIdInput').placeholder = 'Update client ID…';
      } else {
        cidStatus.textContent = '';
      }

      // Gate panels
      const ready = data.authenticated && data.has_client_id;
      const marketPanels = document.getElementById('marketPanels');
      const configPanel  = document.getElementById('configPanel');
      marketPanels.style.opacity       = ready ? '1'    : '0.4';
      marketPanels.style.pointerEvents = ready ? 'auto' : 'none';
      configPanel.style.opacity        = ready ? '1'    : '0.4';
      configPanel.style.pointerEvents  = ready ? 'auto' : 'none';

      if (ready) enableStructureSearch();

    } catch(e) {
      setTimeout(checkAuthStatus, 3000);
    }
  }

  function renderCharacterList(characters) {
    const el = document.getElementById('authCharName');
    if (!characters.length) { el.innerHTML = ''; return; }
    el.innerHTML = characters.map(c => `
      <div style="display:flex;align-items:center;justify-content:space-between;
                  padding:4px 0;border-bottom:1px solid rgba(26,58,92,0.4)">
        <span style="font-size:12px;color:${c.is_primary ? 'var(--accent3)' : 'var(--text)'}">
          ${c.is_primary ? '★ ' : ''}${c.character_name}
        </span>
        <div style="display:flex;gap:4px">
          ${!c.is_primary ? `<button class="btn" onclick="setPrimary(${c.character_id})"
            style="padding:2px 6px;font-size:9px;border-color:var(--accent);color:var(--accent)">PRIMARY</button>` : ''}
          <button class="btn danger" onclick="logoutChar(${c.character_id})"
            style="padding:2px 6px;font-size:9px">✕</button>
        </div>
      </div>`).join('');
  }

  async function _triggerLogin() {
    const res  = await fetch('/api/auth/start', {method:'POST'});
    const data = await res.json();
    if (!data.ok) { toast(data.error || 'Auth failed.', true); return; }
    window.open(data.url, 'eve_sso', 'width=600,height=700,left=200,top=100');
  }

  async function logoutChar(charId) {
    await fetch('/api/auth/logout', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({character_id: charId})});
    isAuthenticated = false;
    await checkAuthStatus();
    toast('Character removed.');
  }

  async function setPrimary(charId) {
    await fetch('/api/auth/set_primary', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({character_id: charId})});
    await checkAuthStatus();
    toast('Primary character updated.');
  }

  async function _triggerLogout() {
    await fetch('/api/auth/logout', {method:'POST'});
    isAuthenticated = false;
    await checkAuthStatus();
    toast('Logged out.');
  }

  async function _saveClientId() {
    const cid = document.getElementById('clientIdInput').value.trim();
    if (!cid) { toast('Paste your Client ID first.', true); return; }
    const res  = await fetch('/api/auth/save_client_id', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({client_id: cid}),
    });
    const data = await res.json();
    if (data.ok) { toast('Client ID saved.'); await checkAuthStatus(); }
    else toast(data.error || 'Failed to save.', true);
  }

  // ─── SDE Status ───────────────────────────────────────────────────────────
  async function checkSdeStatus() {
    try {
      const res  = await fetch('/api/sde/status');
      const data = await res.json();
      const el   = document.getElementById('sdeStatus');
      if (data.ready) {
        sdeReady = true;
        el.innerHTML = '<span style="color:var(--accent3)">✓</span> Universe loaded &nbsp;<span style="color:var(--text-dim)">' +
          data.systems.toLocaleString() + ' systems · ' + data.regions + ' regions</span>';
        enableStructureSearch();
      } else {
        el.innerHTML = '<span class="spinner"></span> Downloading universe data…';
        setTimeout(checkSdeStatus, 2000);
      }
    } catch(e) {
      setTimeout(checkSdeStatus, 3000);
    }
  }

  function enableStructureSearch() {
    if (!sdeReady) return;
    for (const side of ['src','dst']) {
      document.getElementById(side + 'SdeGate').style.display = 'none';
      document.getElementById(side + 'StructureSearch').style.display = 'block';
    }
  }

  // ─── NPC Hubs ─────────────────────────────────────────────────────────────
  async function loadNpcHubs() {
    const res  = await fetch('/api/npc_hubs');
    const hubs = await res.json();
    for (const side of ['src','dst']) {
      const el = document.getElementById(side + '-tab-npc');
      el.innerHTML = hubs.map(h => `
        <div class="loc-card" id="${side}-npc-${h.id}"
             onclick="selectMarket('${side}', ${h.id}, '${h.short}', 'npc')">
          <div>
            <div class="loc-name">${h.short}</div>
            <div class="loc-sub">${h.name}</div>
          </div>
          <div class="loc-badge missing" id="${side}-snap-${h.id}">NO DATA</div>
        </div>`).join('');
      // Check snapshot status for each hub
      hubs.forEach(h => checkSnapStatus(side, h.id));
    }
  }

  // ─── Freeports ────────────────────────────────────────────────────────────
  async function loadFreeports() {
    for (const side of ['src','dst']) {
      document.getElementById(side + '-tab-freeport').innerHTML =
        '<span class="spinner"></span> Loading…';
    }
    try {
      const res   = await fetch('/api/freeports');
      const ports = await res.json();
      for (const side of ['src','dst']) {
        const el = document.getElementById(side + '-tab-freeport');
        if (!ports.length) {
          el.innerHTML = '<div style="font-family:var(--mono);font-size:11px;color:var(--text-dim);padding:8px 0">' +
            'No freeports found yet. Refresh to scan A4E.' +
            '<br><button class="btn" onclick="refreshFreeports()" style="margin-top:8px;padding:4px 10px;font-size:10px">↺ REFRESH</button></div>';
          continue;
        }
        el.innerHTML =
          '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">' +
          '<span style="font-family:var(--mono);font-size:10px;color:var(--text-dim)">' + ports.length + ' markets</span>' +
          '<button class="btn" onclick="refreshFreeports()" style="padding:2px 8px;font-size:9px">↺ REFRESH</button></div>' +
          ports.map(p => {
            const sell = p.sell_orders ? p.sell_orders.toLocaleString() + ' sell' : '';
            return `<div class="loc-card" id="${side}-fp-${p.id}"
                 data-name="${p.name.replace(/"/g, '&quot;')}"
                 onclick="selectMarket('${side}', ${p.id}, this.dataset.name, 'freeport')"
                 oncontextmenu="showContextMenu(event, ${p.id}, this.dataset.name)">
              <div style="min-width:0">
                <div class="loc-name" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${p.name}</div>
                <div class="loc-sub">${p.system}${p.region ? ' · ' + p.region : ''}</div>
              </div>
              <div style="text-align:right;flex-shrink:0">
                <div class="loc-badge missing" id="${side}-snap-${p.id}" style="margin-bottom:3px">NO DATA</div>
                ${sell ? '<div style="font-family:var(--mono);font-size:9px;color:var(--text-dim)">' + sell + '</div>' : ''}
              </div>
            </div>`;
          }).join('');
        ports.forEach(p => checkSnapStatus(side, p.id));
      }
    } catch(e) {
      for (const side of ['src','dst']) {
        document.getElementById(side + '-tab-freeport').innerHTML =
          '<div style="font-family:var(--mono);font-size:11px;color:var(--danger)">Failed to load freeports.</div>';
      }
    }
  }

  async function refreshFreeports() {
    await fetch('/api/freeports/refresh', {method:'POST'});
    toast('Refreshing market hub list from A4E…');
    setTimeout(loadFreeports, 3000);  // give the scraper a moment
  }

  // ─── Snapshot status ──────────────────────────────────────────────────────
  async function checkSnapStatus(side, locationId) {
    try {
      const res  = await fetch(`/api/snapshot/${locationId}/status`);
      const data = await res.json();
      // Update badge in card list
      const badge = document.getElementById(`${side}-snap-${locationId}`);
      if (badge) {
        badge.textContent = data.has_snapshot ? '✓' : 'NO DATA';
        badge.className   = 'loc-badge ' + (data.has_snapshot ? 'ok' : 'missing');
      }
      // Update selected panel badge if this is the selected market
      if (market[side].id === locationId) {
        const snap = document.getElementById(side + 'SnapBadge');
        snap.textContent = data.has_snapshot ? '✓ DATA' : 'NO DATA';
        snap.className   = 'status-badge ' + (data.has_snapshot ? 'ok' : 'missing');
        updateCompareButton();
      }
    } catch(e) {}
  }

  // ─── Market selection ─────────────────────────────────────────────────────
  function selectMarket(side, id, name, type) {
    market[side] = { id, name, type };

    // Show selected panel, hide picker
    document.getElementById(side + 'Selected').style.display = 'block';
    document.getElementById(side + 'Picker').style.display   = 'none';
    const metaLabel = type === 'npc' ? 'NPC Trade Hub' : type === 'freeport' ? 'Freeport Structure' : 'Player Structure';
    const _nameEl = document.getElementById(side + 'Name');
    _nameEl.innerHTML =
      `<span onclick="deselectMarket('${side}')" title="Change market"
        style="cursor:pointer;margin-right:6px;opacity:0.6;transition:opacity 0.15s;font-size:13px"
        onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.6">↺</span>${name}`;
    _nameEl.oncontextmenu = e => showContextMenu(e, id, name);
    document.getElementById(side + 'Meta').textContent = metaLabel;

    // Highlight selected card
    document.querySelectorAll(`[id^="${side}-npc-"], [id^="${side}-fp-"], [id^="${side}-struct-"]`)
      .forEach(c => c.classList.remove('selected'));
    const card = document.getElementById(`${side}-${type === 'npc' ? 'npc' : type === 'freeport' ? 'fp' : 'struct'}-${id}`);
    if (card) card.classList.add('selected');

    checkSnapStatus(side, id);
    loadDateOptions();
    updateCompareButton();
  }

  function deselectMarket(side) {
    market[side] = { id: null, name: null, type: null };
    document.getElementById(side + 'Selected').style.display = 'none';
    document.getElementById(side + 'Picker').style.display   = 'block';
    // Reset to NPC Hubs tab
    const tabNpc = document.querySelector(`#${side}Picker .market-tab`);
    if (tabNpc) switchTab(side, 'npc', tabNpc);
    updateCompareButton();
    allRows = []; filteredRows = [];
    document.getElementById('tableBody').innerHTML =
      '<tr><td colspan="18"><div class="empty-state"><div class="big">◈</div>Select source &amp; destination markets, then click COMPARE</div></td></tr>';
    document.getElementById('btnExportCsv').disabled = true;
  }

  function updateCompareButton() {
    const btn = document.getElementById('btnCompare');
    btn.disabled = !(market.src.id && market.dst.id);
  }

  // ─── Tab switching ────────────────────────────────────────────────────────
  function switchTab(side, tabName, el) {
    document.querySelectorAll(`#${side}Picker .market-tab`).forEach(t => t.classList.remove('active'));
    document.querySelectorAll(`#${side}Picker .tab-pane`).forEach(p => p.classList.remove('active'));
    el.classList.add('active');
    document.getElementById(`${side}-tab-${tabName}`).classList.add('active');
  }

  // ─── System search (for structures) ───────────────────────────────────────
  function onSystemSearch(side, q) {
    clearTimeout(sysSearchTimer[side]);
    const dd = document.getElementById(side + 'SystemDropdown');
    if (q.length < 2) { dd.style.display = 'none'; return; }
    sysSearchTimer[side] = setTimeout(() => doSystemSearch(side, q), 250);
  }

  async function doSystemSearch(side, q) {
    const res  = await fetch(`/api/search/systems?q=${encodeURIComponent(q)}`);
    const data = await res.json();
    const dd   = document.getElementById(side + 'SystemDropdown');
    if (!data.length) { dd.style.display = 'none'; return; }
    dd.innerHTML = data.map(sys => {
      const secClass = 'sec-' + String(Math.round(Math.max(0, Math.min(1, sys.security || 0)) * 10)).padStart(2,'0');
      return `<div class="dropdown-item" onclick="selectSystem('${side}', ${sys.id}, '${sys.name.replace(/'/g,"\\'")}', ${sys.security || 0})">
        <span class="${secClass}">${(sys.security||0).toFixed(1)}</span>
        &nbsp;<strong>${sys.name}</strong>
        <span style="color:var(--text-dim);font-size:10px"> · ${sys.region}</span>
      </div>`;
    }).join('');
    dd.style.display = 'block';
  }

  async function selectSystem(side, systemId, systemName, security) {
    document.getElementById(side + 'SystemSearch').value = systemName;
    document.getElementById(side + 'SystemDropdown').style.display = 'none';
    const secClass = 'sec-' + String(Math.round(Math.max(0, Math.min(1, security)) * 10)).padStart(2,'0');
    document.getElementById(side + 'SystemInfo').innerHTML =
      `System: <span class="${secClass}">${security.toFixed(1)}</span> <strong>${systemName}</strong> — scanning structures…`;

    const list = document.getElementById(side + 'StructureList');
    list.innerHTML = '<span class="spinner"></span>';

    if (!isAuthenticated) {
      list.innerHTML = '<div style="font-family:var(--mono);font-size:11px;color:var(--text-dim)">Login required to see player structures.</div>';
      return;
    }

    const res  = await fetch(`/api/systems/${systemId}/structures`);
    if (res.status === 401) {
      list.innerHTML = '<div style="font-family:var(--mono);font-size:11px;color:var(--danger)">Authentication error.</div>';
      return;
    }
    const data = await res.json();
    if (!data.length) {
      list.innerHTML = '<div style="font-family:var(--mono);font-size:11px;color:var(--text-dim)">No market structures found in ' + systemName + '.</div>';
      return;
    }
    list.innerHTML = data.map(s =>
      `<div class="loc-card" id="${side}-struct-${s.id}"
            data-name="${s.name.replace(/"/g, '&quot;')}"
            oncontextmenu="showContextMenu(event, ${s.id}, this.dataset.name)"
            onclick="selectMarket('${side}', ${s.id}, this.dataset.name, 'structure')">
        <div>
          <div class="loc-name">${s.name}</div>
          <div class="loc-sub">${s.type_name || 'Unknown'}</div>
        </div>
        <div class="loc-badge missing" id="${side}-snap-${s.id}">NO DATA</div>
      </div>`
    ).join('');
    data.forEach(s => checkSnapStatus(side, s.id));
    document.getElementById(side + 'SystemInfo').innerHTML =
      `<span class="${secClass}">${security.toFixed(1)}</span> <strong>${systemName}</strong> — ${data.length} market structure${data.length !== 1 ? 's' : ''} found`;
  }

  // ─── Snapshot fetch ───────────────────────────────────────────────────────
  async function fetchSnapshot(side) {
    const loc = market[side];
    if (!loc.id) return;
    const btn     = document.getElementById('btnFetch' + side.charAt(0).toUpperCase() + side.slice(1));
    const spinner = document.getElementById(side + 'Spinner');
    const excl    = document.getElementById('excludeSkins').checked ? '1' : '0';
    btn.disabled = true;
    spinner.style.display = 'inline-block';
    try {
      const res  = await fetch(`/api/snapshot/${loc.id}?exclude_skins=${excl}`, {method:'POST'});
      const data = await res.json();
      if (data.status === 'created')       toast(`${loc.name}: ${data.types} types fetched.`);
      else if (data.status === 'already_exists') toast(`${loc.name}: today's data already exists.`);
      else if (data.status === 'empty')    toast(`${loc.name}: no orders found.`, true);
      else toast(data.message || 'Error.', true);
      await checkSnapStatus(side, loc.id);
      await loadDateOptions();
    } catch(e) {
      toast('Fetch failed.', true);
    } finally {
      btn.disabled = false;
      spinner.style.display = 'none';
    }
  }

  // ─── Swap markets ─────────────────────────────────────────────────────────
  function swapMarkets() {
    if (!market.src.id && !market.dst.id) return;
    const tmp = {...market.src};
    market.src = {...market.dst};
    market.dst = tmp;

    // Re-render both selected panels
    for (const side of ['src','dst']) {
      const loc = market[side];
      if (loc.id) {
        document.getElementById(side + 'Selected').style.display = 'block';
        document.getElementById(side + 'Picker').style.display   = 'none';
        const _swapEl = document.getElementById(side + 'Name');
        _swapEl.innerHTML =
          '<span onclick="deselectMarket(\'' + side + '\')" title="Change market"'
          + ' style="cursor:pointer;margin-right:6px;opacity:0.6;transition:opacity 0.15s;font-size:13px"'
          + ' onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.6">↺</span>'
          + loc.name;
        _swapEl.oncontextmenu = e => showContextMenu(e, loc.id, loc.name);
        document.getElementById(side + 'Meta').textContent = loc.type === 'npc' ? 'NPC Trade Hub' :
                                                              loc.type === 'freeport' ? 'Freeport Structure' : 'Player Structure';
        checkSnapStatus(side, loc.id);
      } else {
        document.getElementById(side + 'Selected').style.display = 'none';
        document.getElementById(side + 'Picker').style.display   = 'block';
      }
    }

    // Reverse import/export on existing data without re-fetching
    if (allRows.length) {
      allRows = allRows.map(r => ({
        ...r,
        import_margin: r.export_margin,
        export_margin: r.import_margin,
        src_supply: r.dst_supply, src_demand: r.dst_demand,
        src_split:  r.dst_split,  src_sell:   r.dst_sell,  src_buy: r.dst_buy,
        dst_supply: r.src_supply, dst_demand: r.src_demand,
        dst_split:  r.src_split,  dst_sell:   r.src_sell,  dst_buy: r.src_buy,
        src_spread: r.dst_spread,
        dst_spread: r.src_spread,
      }));
      applyFilter();
      toast('Markets swapped. Import/export recalculated.');
    } else {
      toast('Markets swapped.');
    }
    updateCompareButton();
  }

  function round(v, d) { return Math.round(v * 10**d) / 10**d; }

  // ─── Date options ─────────────────────────────────────────────────────────
  async function loadDateOptions() {
    if (!market.src.id && !market.dst.id) return;
    const locId = market.src.id || market.dst.id;
    try {
      const res   = await fetch(`/api/snapshots/dates/${locId}`);
      const dates = await res.json();
      const sel   = document.getElementById('dateSelect');
      const cur   = sel.value;
      sel.innerHTML = '<option value="">Today</option>' +
        dates.map(d => `<option value="${d}"${d===cur?' selected':''}>${d}</option>`).join('');
    } catch(e) {}
  }

  // ─── Comparison ───────────────────────────────────────────────────────────
  async function loadComparison() {
    if (!market.src.id || !market.dst.id) return;
    const dateVal = document.getElementById('dateSelect').value;
    const url = `/api/compare/${market.src.id}/${market.dst.id}` + (dateVal ? `?date=${dateVal}` : '');

    document.getElementById('tableBody').innerHTML =
      '<tr><td colspan="18"><div class="empty-state"><span class="spinner"></span>&nbsp; Loading…</div></td></tr>';

    try {
      const res = await fetch(url);
      allRows   = await res.json();
      page      = 1;
      await loadFilterOptions();
      applyFilter();
      document.getElementById('btnExportCsv').disabled = !allRows.length;
      // Load order overlays in background
      loadOrderOverlays();
    } catch(e) {
      toast('Failed to load comparison.', true);
    }
  }

  async function loadOrderOverlays() {
    try {
      const params = `src=${market.src.id}&dst=${market.dst.id}`;
      const [charRes, corpRes] = await Promise.all([
        fetch(`/api/orders/character?${params}`),
        fetch(`/api/orders/corporation?${params}`),
      ]);
      charOrders = charRes.ok ? await charRes.json() : {};
      corpOrders = corpRes.ok ? await corpRes.json() : {};
      // Merge into allRows so sort/filter/export can see the fields
      allRows = allRows.map(r => {
        const k  = String(r.type_id);
        const co = charOrders[k] || {};
        const cr = corpOrders[k] || {};
        return {
          ...r,
          char_sell: co.char_sell || null,
          char_buy:  co.char_buy  || null,
          corp_sell: cr.corp_sell || null,
          corp_buy:  cr.corp_buy  || null,
        };
      });
      // Re-filter so filteredRows gets the merged fields too
      applyFilter();
    } catch(e) {
      charOrders = {}; corpOrders = {};
    }
  }

  async function loadFilterOptions() {
    try {
      const res  = await fetch('/api/filter_options');
      const data = await res.json();

      const catSel = document.getElementById('filterCategory');
      const grpSel = document.getElementById('filterGroup');
      const curCat = catSel.value;
      const curGrp = grpSel.value;

      // Populate category dropdown
      catSel.innerHTML = '<option value="">All</option>' +
        data.categories.map(c => `<option value="${c}"${c===curCat?' selected':''}>${c}</option>`).join('');

      // Populate group dropdown (category filtering is handled live in applyFilter)
      grpSel.innerHTML = '<option value="">All</option>' +
        data.groups.map(g => `<option value="${g}"${g===curGrp?' selected':''}>${g}</option>`).join('');
    } catch(e) {}
  }

  // In-game metaLevel values (stored as meta_level in type_groups):
  //   0/null = Tech I ("Level 0") or unclassified
  //   1–4    = Meta variants ("Level 1–4")
  //   5      = Tech II / Tech III
  //   6      = Storyline
  //   7–9    = Faction
  //   10–14  = Deadspace
  //   15     = Officer
  //   other  = null / unclassified

  function getMetaBucket(metaLevel) {
    const m = (metaLevel === null || metaLevel === undefined) ? -1 : metaLevel;
    if (m === 0)                return 't1';
    if (m >= 1 && m <= 4)       return 'meta';
    if (m === 5)                return 't2';
    if (m === 6)                return 'storyline';
    if (m >= 7 && m <= 9)       return 'faction';
    if (m >= 10 && m <= 14)     return 'deadspace';
    if (m === 15)               return 'officer';
    return 'null';  // -1 (unset) or any other value
  }

  function getActiveMetaPills() {
    return new Set(
      Array.from(document.querySelectorAll('.meta-pill.active'))
           .map(p => p.dataset.meta)
    );
  }

  function toggleMetaPill(el) {
    el.classList.toggle('active');
    applyFilter();
  }

  function applyFilter() {
    const q    = document.getElementById('filterInput').value.toLowerCase();
    const mode = document.getElementById('showMode').value;
    const cat  = document.getElementById('filterCategory').value;
    const grp  = document.getElementById('filterGroup').value;
    const activeMeta = getActiveMetaPills();
    const allPills   = document.querySelectorAll('.meta-pill').length;
    const metaAllOn  = activeMeta.size === allPills;

    filteredRows = allRows.filter(r => {
      if (q    && !r.type_name.toLowerCase().includes(q)) return false;
      if (cat  && r.category_name !== cat) return false;
      if (grp  && r.group_name    !== grp) return false;
      if (!metaAllOn && !activeMeta.has(getMetaBucket(r.meta_level))) return false;
      if (mode === 'src_only' && (r.dst_supply > 0 || r.dst_demand > 0)) return false;
      if (mode === 'both'     && (r.src_supply === 0 || r.dst_supply === 0)) return false;
      if (mode === 'dst_only' && (r.src_supply > 0 || r.src_demand > 0)) return false;
      return true;
    });

    // When a category is selected, narrow the group dropdown to matching groups
    if (cat) {
      const grpSel    = document.getElementById('filterGroup');
      const curGrp    = grpSel.value;
      const available = new Set(allRows.filter(r => r.category_name === cat).map(r => r.group_name).filter(Boolean));
      Array.from(grpSel.options).forEach(opt => {
        opt.style.display = (!opt.value || available.has(opt.value)) ? '' : 'none';
      });
      if (curGrp && !available.has(curGrp)) {
        grpSel.value = '';
      }
    } else {
      Array.from(document.getElementById('filterGroup').options).forEach(o => o.style.display = '');
    }

    sortRows();
  }

  function sortBy(col) {
    if (sortCol === col) { sortDir *= -1; }
    else { sortCol = col; sortDir = -1; }
    document.querySelectorAll('th').forEach(th => {
      th.classList.remove('sorted-asc','sorted-desc');
      if (th.dataset.col === col)
        th.classList.add(sortDir === -1 ? 'sorted-desc' : 'sorted-asc');
    });
    sortRows();
  }

  const STRING_COLS = new Set(['type_name', 'category_name', 'group_name']);

  function sortRows() {
    filteredRows.sort((a, b) => {
      let av = a[sortCol];
      let bv = b[sortCol];
      // Push nulls/undefined to the end regardless of sort direction
      if (av === null || av === undefined) return 1;
      if (bv === null || bv === undefined) return -1;
      // String columns: alphabetic
      if (STRING_COLS.has(sortCol)) return sortDir * String(av).localeCompare(String(bv));
      // Everything else: coerce to number
      av = Number(av); bv = Number(bv);
      return sortDir * (av - bv);
    });
    page = 1;
    renderTable();
  }

  // ─── Table render ─────────────────────────────────────────────────────────
  function renderTable() {
    const tbody = document.getElementById('tableBody');
    const total = filteredRows.length;
    document.getElementById('rowCount').textContent = total + ' items';
    if (!total) {
      tbody.innerHTML = '<tr><td colspan="18"><div class="empty-state"><div class="big">◈</div>No items match your filter.</div></td></tr>';
      updatePager(0); return;
    }
    const start = (page - 1) * PAGE_SIZE;
    const slice = filteredRows.slice(start, start + PAGE_SIZE);
    tbody.innerHTML = slice.map(r => {
      return `<tr>
        <td title="${r.type_name}">${r.type_name}</td>
        <td style="color:var(--text-dim);font-size:11px">${r.category_name || '—'}</td>
        <td style="color:var(--text-dim);font-size:11px">${r.group_name || '—'}</td>
        <td>${fmt(r.src_supply)}</td>
        <td>${fmt(r.src_demand)}</td>
        <td>${fmtISK(r.src_split)}</td>
        <td>${fmt(r.dst_supply)}</td>
        <td>${fmt(r.dst_demand)}</td>
        <td>${fmtISK(r.dst_split)}</td>
        <td style="color:var(--accent3)">${fmtOrder(r.char_sell)}</td>
        <td style="color:var(--accent2)">${fmtOrder(r.char_buy)}</td>
        <td style="color:var(--accent3)">${fmtOrder(r.corp_sell)}</td>
        <td style="color:var(--accent2)">${fmtOrder(r.corp_buy)}</td>
        <td class="margin-cell" style="color:${spreadColor(r.src_spread)}">${fmtSpread(r.src_spread)}</td>
        <td class="margin-cell" style="color:${spreadColor(r.dst_spread)}">${fmtSpread(r.dst_spread)}</td>
        <td class="margin-cell" style="color:${marginColor(r.import_margin,r.dst_sell,r.src_buy)}">${fmtMargin(r.import_margin,r.dst_sell,r.src_buy,'import')}</td>
        <td class="margin-cell" style="color:${marginColor(r.export_margin,r.src_sell,r.dst_buy)}">${fmtMargin(r.export_margin,r.src_sell,r.dst_buy,'export')}</td>
        <td style="text-align:center"><img src="/static/market_details.png"
          onclick="openInGame(${r.type_id})"
          style="width:16px;height:16px;cursor:pointer;opacity:0.7;vertical-align:middle;transition:opacity 0.15s"
          onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.7"
          title="Open in EVE client" draggable="false"></td>
      </tr>`;
    }).join('');
    updatePager(total);
  }

  function updatePager(total) {
    const pages = Math.ceil(total / PAGE_SIZE);

    for (const suffix of ['', 'Top']) {
      const info = document.getElementById('pagerInfo' + suffix);
      if (!info) continue;
      if (!total) { info.innerHTML = ''; continue; }

      info.innerHTML = `
        <span style="font-family:var(--mono);font-size:11px;color:var(--text-dim);display:inline-flex;align-items:center;gap:4px">
          Page
          <input id="pageInput${suffix}" type="number" min="1" max="${pages}" value="${page}"
            style="width:${String(pages).length + 2}ch;background:var(--bg3);border:1px solid var(--border);
                   color:var(--text);font-family:var(--mono);font-size:11px;padding:1px 4px;
                   border-radius:2px;text-align:center;outline:none;
                   -moz-appearance:textfield;appearance:textfield"
            onfocus="this.style.background='var(--bg2)';this.style.borderColor='var(--accent)';this.select()"
            onblur="commitPage(this,'${suffix}',${pages})"
            onkeydown="if(event.key==='Enter'){this.blur()}else if(event.key==='Escape'){this.value=${page};this.blur()}"
            oninput="this.value=this.value.replace(/[^0-9]/g,'')">
          / ${pages}
          &nbsp;·&nbsp; ${total} rows
        </span>`;
    }

    document.getElementById('btnPrev').disabled    = page <= 1;
    document.getElementById('btnNext').disabled    = page >= pages;
    document.getElementById('btnPrevTop').disabled = page <= 1;
    document.getElementById('btnNextTop').disabled = page >= pages;
  }

  function commitPage(input, suffix, maxPages) {
    input.style.background   = 'var(--bg3)';
    input.style.borderColor  = 'var(--border)';
    const n = parseInt(input.value);
    if (!isNaN(n) && n >= 1 && n <= maxPages && n !== page) {
      page = n;
      renderTable();
      window.scrollTo({top: 0, behavior: 'smooth'});
    } else {
      input.value = page; // reset to current if invalid
    }
  }

  function changePage(delta) {
    page += delta;
    renderTable();
    window.scrollTo({top:0, behavior:'smooth'});
  }

  // ─── Context menu ─────────────────────────────────────────────────────────
  // Suppress the browser default context menu everywhere except inputs,
  // textareas, and table cells (so text is still selectable/copyable).
  document.addEventListener('contextmenu', e => {
    const tag = e.target.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
    if (e.target.closest('td') || e.target.closest('th')) return;
    e.preventDefault();
    hideContextMenu();
  });

  const _ctxMenu = document.getElementById('ctxMenu');

  function showContextMenu(e, locationId, locationName) {
    e.preventDefault();
    e.stopPropagation();
    if (!isAuthenticated) return;
    _ctxMenu.innerHTML =
      `<div class="ctx-item" onclick="setWaypoint(${locationId},'primary',this.closest('#ctxMenu').dataset.name)">✦ Set Destination (Primary)</div>`
      + `<div class="ctx-item" onclick="setWaypoint(${locationId},'all',this.closest('#ctxMenu').dataset.name)">✦ Set Destination (All Characters)</div>`;
    _ctxMenu.dataset.name = locationName;
    const mx = e.clientX, my = e.clientY;
    _ctxMenu.style.left    = 'px'; // position after display so offsetWidth is valid
    _ctxMenu.style.top     = 'px';
    _ctxMenu.style.display = 'block';
    const mw = _ctxMenu.offsetWidth, mh = _ctxMenu.offsetHeight;
    _ctxMenu.style.left = (mx + mw > window.innerWidth  ? mx - mw : mx) + 'px';
    _ctxMenu.style.top  = (my + mh > window.innerHeight ? my - mh : my) + 'px';
  }

  function hideContextMenu() { if (_ctxMenu) _ctxMenu.style.display = 'none'; }

  document.addEventListener('click',  hideContextMenu);
  document.addEventListener('scroll', hideContextMenu, true);

  async function setWaypoint(locationId, scope, locationName) {
    hideContextMenu();
    try {
      const res  = await fetch(`/api/ui/waypoint/${locationId}?scope=${scope}`, {method: 'POST'});
      const data = await res.json();
      if (data.ok) {
        toast(`Destination set: ${locationName}`);
      } else {
        const failed = (data.results || []).filter(r => !r.ok).map(r => r.char).join(', ');
        toast(`Waypoint failed${failed ? ' for ' + failed : ''}.`, true);
      }
    } catch(e) {
      toast('Waypoint request failed.', true);
    }
  }

  // Arrow key navigation — but not when typing in an input/select
  document.addEventListener('keydown', e => {
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'SELECT' || e.target.tagName === 'TEXTAREA') return;
    if (e.key === 'ArrowLeft'  && page > 1) { page--; renderTable(); window.scrollTo({top:0,behavior:'smooth'}); }
    if (e.key === 'ArrowRight' && filteredRows.length > page * PAGE_SIZE) { page++; renderTable(); window.scrollTo({top:0,behavior:'smooth'}); }
  });

  // ─── Formatting ───────────────────────────────────────────────────────────
  function fmt(v) {
    if (v === null || v === undefined || v === 0) return '<span style="color:var(--text-dim)">—</span>';
    return v >= 1e9 ? (v/1e9).toFixed(1)+'B' : v >= 1e6 ? (v/1e6).toFixed(1)+'M' : v >= 1e3 ? (v/1e3).toFixed(1)+'K' : v.toFixed(0);
  }

  function fmtISK(v) {
    if (!v) return '<span style="color:var(--text-dim)">—</span>';
    return v >= 1e9 ? (v/1e9).toFixed(2)+'B' : v >= 1e6 ? (v/1e6).toFixed(2)+'M' : v.toLocaleString(undefined,{maximumFractionDigits:0});
  }

  function fmtPct(v) {
    if (v === null || v === undefined) return '<span style="color:var(--text-dim)">—</span>';
    return v.toFixed(1) + '%';
  }

  function ratioClass(v) {
    if (v === null || v === undefined) return 'ratio-none';
    if (v >= 80)  return 'ratio-high';
    if (v >= 40)  return 'ratio-mid';
    return 'ratio-low';
  }

  // numerator = the price being divided; denominator = reference
  function fmtMargin(v, numerator, denominator, kind) {
    if (v !== null && v !== undefined) return v.toFixed(1) + '%';
    if (!numerator && !denominator) return '<span style="color:var(--text-dim)">—</span>';
    if (!numerator)
      return kind === 'import'
        ? '<span style="color:#9b59ff;font-size:10px;letter-spacing:1px">VACANT</span>'
        : '<span style="color:var(--text-dim);font-size:10px;letter-spacing:1px">NO REF</span>';
    if (!denominator)
      return kind === 'export'
        ? '<span style="color:#ff7b00;font-size:10px;letter-spacing:1px">UNSOUGHT</span>'
        : '<span style="color:var(--text-dim);font-size:10px;letter-spacing:1px">NO REF</span>';
    return '<span style="color:var(--text-dim)">—</span>';
  }

  // Spread colour: wide spread = opportunity (green), tight = competitive (dim)
  // 0–2%: very tight (dim), 2–10%: normal (accent), 10–25%: wide (orange), 25%+: very wide (green)
  function spreadColor(v) {
    if (v === null || v === undefined) return 'transparent';
    if (v >= 25) return 'var(--accent3)';
    if (v >= 10) return 'var(--accent2)';
    if (v >=  2) return 'var(--accent)';
    return 'var(--text-dim)';
  }

  function fmtOrder(v) {
    if (!v) return '<span style="color:var(--text-dim)">—</span>';
    return fmt(v);
  }

  function fmtSpread(v) {
    if (v === null || v === undefined) return '<span style="color:var(--text-dim)">—</span>';
    return v.toFixed(1) + '%';
  }

  function marginColor(v, numerator, denominator) {
    if (v === null || v === undefined) return 'transparent';
    const clamped = Math.min(200, Math.max(100, v));
    const stops = [
      [100, [43,  117, 222]],
      [125, [0,   255, 157]],
      [150, [255, 215,   0]],
      [200, [204,   0,  34]],
    ];
    for (let i = 0; i < stops.length - 1; i++) {
      const [lo, cL] = stops[i];
      const [hi, cH] = stops[i+1];
      if (clamped <= hi) {
        const t = (clamped - lo) / (hi - lo);
        const r = Math.round(cL[0] + t*(cH[0]-cL[0]));
        const g = Math.round(cL[1] + t*(cH[1]-cL[1]));
        const b = Math.round(cL[2] + t*(cH[2]-cL[2]));
        return `rgb(${r},${g},${b})`;
      }
    }
    return `rgb(${stops[stops.length-1][1].join(',')})`;
  }

  // Security status colour
  const SEC_COLORS = {1.0:'#2b75de',0.9:'#3a9cf1',0.8:'#4bcef4',0.7:'#62d9a8',0.6:'#71e452',0.5:'#ecff81',0.4:'#df6e07',0.3:'#d0400b',0.2:'#bb0f19',0.1:'#751f18',0.0:'#8e3161'};
  function secColor(sec) {
    if (sec === null || sec === undefined) return 'var(--text-dim)';
    return SEC_COLORS[Math.max(0,Math.min(1,Math.round(sec*10)/10))] || 'var(--text-dim)';
  }

  // ─── In-game windows ──────────────────────────────────────────────────────
  async function openInGame(typeId) {
    if (!isAuthenticated) { toast('Login with EVE character to use in-game links.', true); return; }
    const res  = await fetch('/api/ui/market/' + typeId, {method:'POST'});
    const data = await res.json();
    if (!data.ok) toast('Could not open in-game window. Is the EVE client running?', true);
  }

  // ─── CSV export ───────────────────────────────────────────────────────────
  function exportCsv() {
    if (!filteredRows.length) return;
    const headers = [
      'type_id','item_name','category','group',
      'src_supply','src_demand','src_split','src_sell','src_buy',
      'dst_supply','dst_demand','dst_split','dst_sell','dst_buy',
      'src_spread_pct','dst_spread_pct',
      'import_margin_pct','export_margin_pct',
    ];
    const esc = v => (v === null || v === undefined || v === '') ? '' : String(v).includes(',') ? `"${v}"` : v;
    const rows = filteredRows.map(r => [
      r.type_id, r.type_name, r.category_name, r.group_name,
      r.src_supply, r.src_demand, r.src_split || '', r.src_sell || '', r.src_buy || '',
      r.dst_supply, r.dst_demand, r.dst_split || '', r.dst_sell || '', r.dst_buy || '',
      r.src_spread || '', r.dst_spread || '',
      r.import_margin || '', r.export_margin || '',
    ].map(esc).join(','));

    const src  = (market.src.name || 'src').split(' - ')[0].replace(/\s+/g,'_');
    const dst  = (market.dst.name || 'dst').split(' - ')[0].replace(/\s+/g,'_');
    const date = document.getElementById('dateSelect').value || new Date().toISOString().slice(0,10);
    const filename = `caravanserai_${src}_vs_${dst}_${date}.csv`;

    const blob = new Blob([[headers.join(','), ...rows].join('\n')], {type:'text/csv'});
    const a    = document.createElement('a');
    a.href     = URL.createObjectURL(blob);
    a.download = filename;
    document.body.appendChild(a); a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(a.href);
  }

  // ─── Donation ─────────────────────────────────────────────────────────────
  function openDonationPage() {
    window.open('/donate', '_blank', 'width=520,height=480,left=200,top=100');
  }

  // ─── Toast ────────────────────────────────────────────────────────────────
  let _toastTimer;
  function toast(msg, isError=false) {
    const el = document.getElementById('toast');
    el.textContent = msg;
    el.className   = 'show' + (isError ? ' error' : '');
    clearTimeout(_toastTimer);
    _toastTimer = setTimeout(() => { el.className = ''; }, 3000);
  }