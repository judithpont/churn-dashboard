// Vercel serverless: fetch all churn companies from HubSpot
// GET /api/hs-churns
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const token = process.env.HUBSPOT_TOKEN;
  if (!token) return res.status(500).json({ error: 'HUBSPOT_TOKEN not configured' });

  const PROPS = [
    'name', 'churn_reason_saas', 'churn_status',
    'fecha_de_solicitud_de_baja', 'churn_subreason_saas__note',
    'churn_que_hemos_hecho_para_analizar_esta_baja',
    'churn_que_conclusion_sacamos', 'hubspot_owner_id'
  ];

  try {
    // Step 1: Fetch all owners for ID→name mapping
    const ownerMap = {};
    let ownerAfter;
    do {
      const oUrl = `https://api.hubapi.com/crm/v3/owners?limit=100${ownerAfter ? '&after=' + ownerAfter : ''}`;
      const oRes = await fetch(oUrl, { headers: { Authorization: `Bearer ${token}` } });
      if (!oRes.ok) break;
      const oData = await oRes.json();
      for (const o of oData.results || []) {
        const name = [o.firstName, o.lastName].filter(Boolean).join(' ').trim();
        if (name) ownerMap[o.id] = name;
      }
      ownerAfter = oData.paging?.next?.after || null;
    } while (ownerAfter);

    // Step 2: Search companies that have solicita_la_baja___saas_ = true OR have churn_reason_saas set
    const allResults = [];
    const seenIds = new Set();
    let after = 0;
    const PAGE = 100;

    do {
      const body = {
        filterGroups: [
          { filters: [{ propertyName: 'solicita_la_baja___saas_', operator: 'EQ', value: 'true' }] },
          { filters: [{ propertyName: 'churn_reason_saas', operator: 'HAS_PROPERTY' }] },
          { filters: [{ propertyName: 'fecha_de_solicitud_de_baja', operator: 'HAS_PROPERTY' }] }
        ],
        properties: PROPS,
        limit: PAGE,
        after
      };

      const searchRes = await fetch(
        'https://api.hubapi.com/crm/v3/objects/companies/search',
        {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(body)
        }
      );

      if (!searchRes.ok) {
        const err = await searchRes.text();
        return res.status(500).json({ error: `HubSpot API error: ${searchRes.status}`, detail: err });
      }

      const searchData = await searchRes.json();
      const results = searchData.results || [];

      for (const r of results) {
        if (seenIds.has(r.id)) continue;
        seenIds.add(r.id);
        const p = r.properties || {};
        const ownerId = p.hubspot_owner_id || '';
        allResults.push({
          id: r.id,
          name: p.name || '',
          churn_reason: p.churn_reason_saas || '',
          churn_status: p.churn_status || '',
          subreason: p.churn_subreason_saas__note || '',
          note: p.churn_subreason_saas__note || '',
          analysis: p.churn_que_hemos_hecho_para_analizar_esta_baja || '',
          conclusion: p.churn_que_conclusion_sacamos || '',
          churn_date: p.fecha_de_solicitud_de_baja || '',
          owner: ownerMap[ownerId] || ''
        });
      }

      after = searchData.paging?.next?.after ? Number(searchData.paging.next.after) : null;
    } while (after);

    return res.status(200).json({
      total: allResults.length,
      updated: new Date().toISOString(),
      data: allResults
    });

  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
