// Vercel serverless function: contact ID → company ID + client type
// GET /api/hs-lookup?contactId=123456
export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  const { contactId } = req.query;
  if (!contactId) return res.status(400).json({ error: 'Missing contactId' });

  const token = process.env.HUBSPOT_TOKEN;
  if (!token) return res.status(500).json({ error: 'HUBSPOT_TOKEN not configured' });

  try {
    // Step 1: get company associations for this contact
    const assocRes = await fetch(
      `https://api.hubapi.com/crm/v4/objects/contacts/${contactId}/associations/companies`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    if (!assocRes.ok) return res.status(200).json({ companyId: null, clientType: '' });

    const assocData = await assocRes.json();
    const companyId = assocData.results?.[0]?.toObjectId || null;
    if (!companyId) return res.status(200).json({ companyId: null, clientType: '' });

    // Step 2: get client type from company record
    const compRes = await fetch(
      `https://api.hubapi.com/crm/v3/objects/companies/${companyId}?properties=saas_client_type`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const compData = compRes.ok ? await compRes.json() : {};
    const clientType = compData.properties?.saas_client_type || '';

    return res.status(200).json({ companyId, clientType });
  } catch (e) {
    return res.status(200).json({ companyId: null, clientType: '' });
  }
}
