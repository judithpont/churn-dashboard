#!/usr/bin/env python3
"""
update_map.py — Detecta contactIds nuevos en el Google Sheet de Modjo y actualiza
CONTACT_TO_COMPANY en index.html consultando la API de HubSpot.

Uso:
    python3 update_map.py
    python3 update_map.py --dry-run   # ver qué haría sin modificar nada

Requiere:
    - HUBSPOT_TOKEN en variable de entorno o en archivo .env en esta carpeta
    - pip install requests python-dotenv
"""

import sys
import os
import re
import time
import csv
import io
import argparse
import requests

# URL del Google Sheet publicado como CSV (la misma que usa index.html)
SHEET_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSgZgcbRQZ0sa1BU7Pv1TKwqo_CDyHEPcdcGZF3DtUuH_vNmZsjbrO5puLarjMCDtCsiF0SjIFFk0Jy/pub?output=csv'

# ── Carga del token ─────────────────────────────────────────────────────────
def load_token():
    token = os.environ.get("HUBSPOT_TOKEN")
    if token:
        return token
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("HUBSPOT_TOKEN="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None

# ── Descarga y parseo del Google Sheet ──────────────────────────────────────
def get_contact_ids_from_sheet():
    print(f"📥 Descargando datos del Google Sheet...")
    r = requests.get(SHEET_URL, timeout=30)
    r.raise_for_status()
    r.encoding = 'utf-8'

    ids = set()
    col_candidates = [
        "HubSpot Contact ID", "Hubspot Contact ID", "hubspot contact id",
        "hubspot_contact_id", "Contact ID", "contact id", "ContactId", "contactid"
    ]
    reader = csv.DictReader(io.StringIO(r.text))
    original_headers = reader.fieldnames or []

    col = None
    for candidate in col_candidates:
        for h in original_headers:
            if h.strip().lower() == candidate.strip().lower():
                col = h  # guardamos el nombre ORIGINAL (con espacios si los tiene)
                break
        if col:
            break

    if not col:
        print(f"ERROR: No se encontró columna de contactId. Columnas: {original_headers}")
        sys.exit(1)

    for row in reader:
        val = row.get(col, "").strip()
        if val:
            ids.add(val)

    return ids

# ── Parseo de CONTACT_TO_COMPANY en index.html ───────────────────────────────
def get_current_map(html_path):
    with open(html_path) as f:
        html = f.read()
    m = re.search(r'const CONTACT_TO_COMPANY=\{(.*?)\};', html, re.DOTALL)
    if not m:
        print("ERROR: No se encontró CONTACT_TO_COMPANY en index.html")
        sys.exit(1)
    entries = {}
    for line in m.group(1).splitlines():
        line = line.strip().rstrip(",")
        line = re.sub(r'\s*//.*$', '', line)
        match = re.match(r'"(\d+)"\s*:\s*"(\d+)"', line)
        if match:
            entries[match.group(1)] = match.group(2)
    return entries

# ── Consulta HubSpot: contactId → companyId ──────────────────────────────────
def lookup_company(contact_id, token):
    url = f"https://api.hubapi.com/crm/v4/objects/contacts/{contact_id}/associations/companies"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        results = r.json().get("results", [])
        if results:
            return str(results[0]["toObjectId"])
        return None
    except Exception as e:
        print(f"  ⚠️  Error consultando {contact_id}: {e}")
        return None

# ── Actualización de index.html ──────────────────────────────────────────────
def update_html(html_path, full_map):
    with open(html_path) as f:
        html = f.read()
    entries = sorted(full_map.items())
    lines = []
    for i, (cid, compid) in enumerate(entries):
        comma = "," if i < len(entries) - 1 else ""
        lines.append(f'  "{cid}":"{compid}"{comma}')
    new_block = "const CONTACT_TO_COMPANY={\n" + "\n".join(lines) + "\n};"
    new_html = re.sub(r'const CONTACT_TO_COMPANY=\{.*?\};', new_block, html, flags=re.DOTALL)
    with open(html_path, "w") as f:
        f.write(new_html)

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Actualiza CONTACT_TO_COMPANY en index.html")
    parser.add_argument("--html", default=os.path.join(os.path.dirname(__file__), "index.html"),
                        help="Ruta a index.html (por defecto: junto al script)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo muestra qué haría, sin modificar index.html")
    args = parser.parse_args()

    token = load_token()
    if not token:
        print("ERROR: No se encontró HUBSPOT_TOKEN.")
        print("Crea un archivo .env en esta carpeta con: HUBSPOT_TOKEN=pat-eu1-xxx")
        sys.exit(1)

    sheet_ids = get_contact_ids_from_sheet()
    print(f"   {len(sheet_ids)} contactIds únicos en el Sheet")

    print(f"📄 Leyendo mapa actual en index.html...")
    current_map = get_current_map(args.html)
    print(f"   {len(current_map)} entradas actuales en CONTACT_TO_COMPANY")

    new_ids = sheet_ids - set(current_map.keys())
    # Ignorar IDs vacíos
    new_ids = {i for i in new_ids if i}
    print(f"\n🔍 {len(new_ids)} contactIds nuevos sin mapear")

    if not new_ids:
        print("✅ Nada que actualizar — el mapa ya está al día.")
        return

    added = {}
    no_company = []
    for i, cid in enumerate(sorted(new_ids), 1):
        print(f"  [{i}/{len(new_ids)}] {cid} ... ", end="", flush=True)
        company_id = lookup_company(cid, token)
        if company_id:
            added[cid] = company_id
            print(f"→ {company_id}")
        else:
            no_company.append(cid)
            print("sin empresa")
        if i % 10 == 0:
            time.sleep(0.5)

    print(f"\n📊 Resultado: {len(added)} nuevas entradas, {len(no_company)} sin empresa")

    if added:
        full_map = {**current_map, **added}
        if args.dry_run:
            print("\n[dry-run] Se añadirían estas entradas:")
            for cid, compid in sorted(added.items()):
                print(f"  {cid} → {compid}")
        else:
            update_html(args.html, full_map)
            print(f"\n✅ index.html actualizado con {len(full_map)} entradas totales")
            print("   Recuerda hacer git commit y push desde tu terminal.")
    else:
        print("\nℹ️  No se encontraron empresas nuevas.")

if __name__ == "__main__":
    main()
