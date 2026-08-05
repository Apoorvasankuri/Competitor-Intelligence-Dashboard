/* ============================================================
   CMIE Civil Highlights panel — add to index.html
   ============================================================

   WHERE TO PLUG THIS IN (index.html, existing CMIE section):

   1. In fetchSummary()/fetchProjects() area (~line 3140-3159), add the
      new fetch function below (fetchCivilHighlights).

   2. In renderCivilCapexTab() (~line 3162-3170), add one line so the
      panel has a mount point:

         '<div id="cxFilters"></div>' +
         '<div id="cxHighlightsWrap"></div>' +          // <-- ADD THIS LINE
         '<div id="cxTableWrap"></div>' +

   3. In initCivilCapex() (~line 3173-3186), fetch it alongside the
      existing calls:

         var [summaryRes, projectsRes, highlightsRes] = await Promise.all([
           fetchSummary(), fetchProjects(), fetchCivilHighlights()
         ]);
         cx.highlights = highlightsRes;
         ...
         renderHighlights();   // add to renderAll(), alongside renderTable() etc.

   4. Paste the three functions below anywhere in the CMIE script block.
   ============================================================ */

async function fetchCivilHighlights() {
  var res = await fetch(getAPI() + '/api/cmie/civil-highlights?token=' + getToken());
  if (!res.ok) throw new Error('Civil Highlights API error ' + res.status);
  return res.json();
}

function renderHighlights() {
  var el = document.getElementById('cxHighlightsWrap');
  if (!el) return;
  var h = cx.highlights;

  if (!h || !h.projects || h.projects.length === 0) {
    el.innerHTML = '<div class="cx-glass" style="margin-bottom:24px;padding:20px;">' +
      '<div class="cx-table-title">High-Value Recent Activity</div>' +
      '<p style="opacity:0.7;margin-top:8px;">No projects over ₹' +
      (h ? h.criteria.min_cost_crore : 250) +
      ' Cr updated in the last ' + (h ? h.criteria.months_back : 3) + ' months.</p>' +
    '</div>';
    return;
  }

  var cards = h.projects.map(function(p) {
    var eventsHtml = (p.recent_events && p.recent_events.length > 0)
      ? '<ul class="cx-events-list">' + p.recent_events.map(function(e) {
          return '<li><strong>' + (e.event_date || '—') + '</strong> — ' +
                 (e.event_description || 'No description') + '</li>';
        }).join('') + '</ul>'
      : '<p class="cx-na" style="margin-top:8px;">' +
          (p.events_error ? 'Could not load events: ' + p.events_error
                           : 'No events in the last ' + h.criteria.months_back + ' months.') +
        '</p>';

    return '<div class="cx-glass cx-highlight-card" style="margin-bottom:16px;padding:18px;">' +
      '<div style="display:flex;justify-content:space-between;align-items:flex-start;">' +
        '<div>' +
          '<div style="font-weight:600;font-size:15px;">' + (p.project_name || '—') + '</div>' +
          '<div style="opacity:0.7;font-size:13px;">' + (p.promoter_name || '—') +
            ' · ' + (p.state || '—') + ' · ' + (p.industry || '—') + '</div>' +
        '</div>' +
        '<div style="text-align:right;">' +
          '<span class="cx-cost">' + fmtMillionToDisplay(p.project_cost) + '</span><br>' +
          statusBadge(p.project_status) +
        '</div>' +
      '</div>' +
      '<div style="margin-top:8px;font-size:12px;opacity:0.6;">Last updated: ' + (p.last_updated || '—') + '</div>' +
      '<div style="margin-top:10px;">' + eventsHtml + '</div>' +
    '</div>';
  }).join('');

  el.innerHTML =
    '<div style="margin-bottom:16px;">' +
      '<div class="cx-table-title">High-Value Recent Activity</div>' +
      '<div style="opacity:0.7;font-size:13px;">' +
        h.count + ' projects over ₹' + h.criteria.min_cost_crore + ' Cr, updated in the last ' +
        h.criteria.months_back + ' months' +
      '</div>' +
    '</div>' +
    cards;
}
