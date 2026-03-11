import gspread
import pandas as pd
import json
import os
import requests
import time
from google.oauth2.service_account import Credentials
import anthropic
from datetime import datetime

# ============================================================
# CONFIG  — PRD v2.0
# ============================================================
SHEET_URL         = "https://docs.google.com/spreadsheets/d/16Tv2U164NammrDBe2u8b1_BuuHiWCkHaO1J5mFkl6OY"
SOURCE_TAB        = "Resultados_Churn"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HUBSPOT_API_KEY   = os.environ.get("HUBSPOT_API_KEY", "")
HUBSPOT_PORTAL_ID = "25808060"          # your HubSpot portal

# PRD v2: confidence ≥ 8/10 to classify; below → "Sin clasificar"
CONFIDENCE_THRESHOLD = 0.70

DUENOS_OBJETIVO = [
    "Victor Ortega", "Óscar Lopo", "Martina Benalcazar",
    "Kamila Jiménez", "Gonzalo Rosales", "Franco Ferretti"
]

# ============================================================
# PRD v2 TAXONOMY — categories + sub-motivos
# ============================================================
CATEGORIAS_CHURN = [
    "Fallo en la plataforma",
    "Cierre de venta forzado",
    "Mala comunicación en ventas",
    "Problemas de calidad",
    "Sin impacto real en su negocio",
    "Promesa irreal",
    "Negocio cerrado",
    "Fuera del alcance del plan",
    "Problema de soporte",
    "Fallo en el onboarding",
    "No justifica precio",
]

# PRD v2: up to 3 sub-motivos per call, ordered by weight in the churn decision
SUBCATEGORIAS = {
    # 🏺 Quality Issues
    "Problemas de calidad": [
        "baja-velocidad-entrega", "contenido-generico", "servicio-no-entregado",
        "contenido-ai-rechazado", "identidad-visual-no-respetada", "cambios-no-aplicados"
    ],
    # 🏗️ Technical Issues
    "Fallo en la plataforma": [
        "errores-tecnicos", "publicaciones-fallan-calendario", "fallo-conexion-rrss",
        "friccion-ux-app", "problemas-modulo-maya", "problemas-inicio-sesion"
    ],
    # 🫂 Support & Service
    "Problema de soporte": [
        "respuesta-soporte-lenta", "problemas-no-resueltos", "falta-soporte-dedicado"
    ],
    # 🪶 Product / Feature Gaps
    "Fuera del alcance del plan": [
        "funcionalidades-faltantes", "integracion-no-disponible", "automatizacion-insuficiente"
    ],
    # 🚤 Poor Adoption
    "Fallo en el onboarding": [
        "herramienta-no-adoptada", "herramienta-demasiado-compleja",
        "falta-recursos-formacion", "objetivos-iniciales-no-alcanzados"
    ],
    # 💰 Pricing & Value
    "Sin impacto real en su negocio": [
        "precio-demasiado-alto", "mejor-valor-en-competencia", "roi-no-justificado"
    ],
    "No justifica precio": [
        "precio-demasiado-alto", "mejor-valor-en-competencia", "roi-no-justificado"
    ],
    # 🔫 Forced Sales Closing
    "Cierre de venta forzado": [
        "arrepentimiento-compra", "cliente-fuera-icp",
        "desalineacion-facturacion", "cliente-solo-promocion"
    ],
    "Mala comunicación en ventas": [
        "cliente-fuera-icp", "desalineacion-facturacion"
    ],
    # ⰾ Unrealistic Promises
    "Promesa irreal": [
        "funcionalidad-prometida-no-disponible", "servicio-estilo-agencia-prometido",
        "precio-prometido-incorrecto"
    ],
    # 🏚️ Business Changes
    "Negocio cerrado": [
        "negocio-cerrado", "reduccion-personal"
    ],
}

# HubSpot reason → Modjo category
HS_TRADUCCIONES = {
    "Sales Miscommunication":                "Mala comunicación en ventas",
    "Poor CX / Platform Issues":             "Fallo en la plataforma",
    "Quality Issues":                        "Problemas de calidad",
    "Support or Service Issues":             "Problema de soporte",
    "Onboarding Failure":                    "Fallo en el onboarding",
    "Does not see impact on their business": "Sin impacto real en su negocio",
    "WTP < Cost":                            "No justifica precio",
    "Outside The Scope of the Plan":         "Fuera del alcance del plan",
    "Unrealistic promise":                   "Promesa irreal",
    "Forced Sales Closing":                  "Cierre de venta forzado",
    "Business Closed":                       "Negocio cerrado",
}


# ============================================================
# GOOGLE SHEETS
# ============================================================
def conectar_sheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    if os.environ.get("GOOGLE_CREDENTIALS"):
        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    return gspread.authorize(creds).open_by_url(SHEET_URL)


# ============================================================
# EXTRAER NOMBRE DE CLIENTE
# ============================================================
def extraer_cliente(participantes: str, dueno: str) -> str:
    if not participantes:
        return ""
    partes = [p.strip() for p in participantes.replace(";", ",").split(",") if p.strip()]
    if not partes:
        return participantes
    if len(partes) == 1:
        return partes[0]
    dueno_parts = [p for p in (dueno or "").lower().split() if len(p) > 2]
    clientes = [p for p in partes if not any(dp in p.lower() for dp in dueno_parts)]
    return clientes[0] if clientes else partes[0]


# ============================================================
# HUBSPOT — Sprint 2: SaaS Client Type + URL directa + fecha baja
# ============================================================
def buscar_hubspot(cliente: str) -> dict:
    """
    Returns:
      company_id, name, saas_client_type, churn_reason, churn_date,
      hs_url, sin_registro (bool)
    """
    if not HUBSPOT_API_KEY or not cliente:
        return {"sin_registro": True}

    headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "name", "operator": "CONTAINS_TOKEN", "value": cliente.strip().split()[0]}
        ]}],
        "properties": [
            "name", "churn_status", "churn_reason_saas",
            "fecha_de_solicitud_de_baja", "saas_client_type"
        ],
        "limit": 5
    }
    try:
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/companies/search",
            headers=headers, json=payload, timeout=10
        )
        if r.status_code != 200:
            return {"sin_registro": True}
        results = r.json().get("results", [])
        if not results:
            return {"sin_registro": True}

        # Best match
        cliente_lower = cliente.lower()
        best_result = None
        for company in results:
            props = company.get("properties", {})
            hs_name = (props.get("name") or "").lower()
            if cliente_lower in hs_name or hs_name in cliente_lower:
                best_result = company
                break
        if not best_result:
            best_result = results[0]

        company_id = best_result.get("id", "")
        props = best_result.get("properties", {})
        churn_reason_raw = props.get("churn_reason_saas", "")
        churn_reason_es  = HS_TRADUCCIONES.get(churn_reason_raw, churn_reason_raw)

        return {
            "sin_registro": False,
            "company_id":        company_id,
            "name":              props.get("name", ""),
            "saas_client_type":  props.get("saas_client_type", ""),
            "churn_reason":      churn_reason_es,
            "churn_reason_raw":  churn_reason_raw,
            "churn_date":        props.get("fecha_de_solicitud_de_baja", ""),
            "churn_status":      props.get("churn_status", ""),
            "hs_url": f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/company/{company_id}" if company_id else "",
        }
    except Exception as e:
        print(f"   ⚠️  Error HubSpot para '{cliente}': {e}")
        return {"sin_registro": True}


# ============================================================
# ANÁLISIS CON CLAUDE — PRD v2 two-step root cause reasoning
# ============================================================
def analizar_transcript(transcript: str, cliente: str, titulo: str) -> dict:
    """
    PRD v2:
    - Step 1: comprehension — what bothered the client, what they expected, what they didn't get
    - Step 2: root cause — choose category/sub-motivos based on root cause, not keywords
    - Confidence ≥ 8/10 to assign category; below → 'Sin clasificar'
    - Max 3 sub-motivos ordered by weight in churn decision
    """
    if not transcript or len(transcript.strip()) < 50:
        return {
            "churn_detectado": "sin datos", "categoria": "Sin clasificar",
            "subcategorias": [], "nivel_riesgo": "bajo",
            "motivo_principal": "Sin transcripción disponible",
            "resumen_ia": "No se encontró transcripción para esta llamada.",
            "confianza": 0.0
        }

    ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    transcript_truncado = transcript[:10000]

    cats_list = "\n".join(f"  - {c}" for c in CATEGORIAS_CHURN)
    subcats_text = "\n".join(
        f"  {cat}:\n" + "\n".join(f"    · {s}" for s in subs)
        for cat, subs in SUBCATEGORIAS.items()
    )

    prompt = f"""Eres un analista de Revenue Operations especializado en churn de SaaS.
Analiza la siguiente transcripción de llamada con el cliente "{cliente}" (título: "{titulo}").

TRANSCRIPCIÓN:
{transcript_truncado}

---

INSTRUCCIONES — razona en DOS PASOS antes de responder:

PASO 1 — COMPRENSIÓN:
Lee la transcripción completa. Extrae mentalmente:
  a) ¿Qué le molestó específicamente al cliente?
  b) ¿Qué esperaba recibir y no recibió?
  c) ¿Qué fue lo que realmente lo empujó a irse o a estar insatisfecho?
  (No etiquetes todavía. Solo comprende la experiencia completa.)

PASO 2 — ROOT CAUSE:
Con ese contexto, identifica el root cause REAL del churn o insatisfacción.
Elige la categoría cuya causa raíz encaje mejor con la EXPERIENCIA DEL CLIENTE,
no con las palabras literales que usó.

Ejemplo: si el cliente menciona 'la app es lenta' y 'nadie me respondió' pero
su queja real es que nunca recibió el servicio prometido en ventas →
root cause: Mala comunicación en ventas, no Fallo en la plataforma.

---

Categorías válidas:
{cats_list}

Sub-motivos válidos por categoría:
{subcats_text}

---

Responde ÚNICAMENTE en JSON válido con este formato exacto:

{{
  "paso1_comprension": "2-3 frases resumiendo qué le molestó, qué esperaba, qué no recibió",
  "churn_detectado": "sí | no | riesgo",
  "categoria": "una de las categorías listadas arriba, o null si no aplica",
  "subcategorias": [],
  "nivel_riesgo": "alto | medio | bajo",
  "motivo_principal": "frase concisa en español explicando el root cause real",
  "resumen_ia": "resumen ejecutivo de 2-3 frases en español",
  "confianza": 8
}}

Reglas de salida:
- "subcategorias": elige ÚNICAMENTE sub-motivos de la lista de la categoría que hayas asignado. NO mezcles sub-motivos de otras categorías. Incluye solo los que estén claramente respaldados por la transcripción. Si ninguno encaja, devuelve []. Máximo 3, ordenados de mayor a menor peso en la decisión del cliente.
- "confianza": número del 1 al 10 (no entre 0 y 1). 9-10 si la causa es muy clara; 5-6 si hay señales pero ambiguas; 3-4 si el transcript es vago; 1-2 si es ininteligible o no hay datos.
- "categoria": null si confianza < 8 (la llamada quedará como 'Sin clasificar' para revisión manual).
- "churn_detectado": "sí" si el cliente ya solicitó baja o claramente se va; "riesgo" si hay señales sin confirmar; "no" si la llamada es normal.
"""

    try:
        msg = ai.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}]
        )
        texto = msg.content[0].text.strip()
        parsed = json.loads(texto[texto.find("{"):texto.rfind("}")+1])

        # Normalize confianza: PRD uses 1-10 scale; store as 0-1 internally
        raw_conf = parsed.get("confianza", 5)
        if isinstance(raw_conf, (int, float)) and raw_conf > 1:
            confianza = raw_conf / 10.0
        else:
            confianza = float(raw_conf)

        # PRD rule: confidence < 8/10 → "Sin clasificar"
        if confianza < CONFIDENCE_THRESHOLD:
            parsed["categoria"] = "Sin clasificar"
            parsed["subcategorias"] = []

        # Validate and filter subcategories: must belong to the assigned category
        cat_assigned = parsed.get("categoria", "Sin clasificar") or "Sin clasificar"
        subs = parsed.get("subcategorias", [])
        if not isinstance(subs, list):
            subs = [str(subs)] if subs else []
        # Filter to only valid sub-motivos for this category (safety net)
        valid_subs = set(SUBCATEGORIAS.get(cat_assigned, []))
        if valid_subs:
            subs = [s for s in subs if s in valid_subs]
        parsed["subcategorias"] = subs[:3]

        parsed.setdefault("churn_detectado", "no")
        parsed.setdefault("categoria", "Sin clasificar")
        parsed.setdefault("nivel_riesgo", "bajo")
        parsed.setdefault("motivo_principal", "")
        parsed.setdefault("resumen_ia", "")
        parsed["confianza"] = confianza
        return parsed

    except Exception as e:
        print(f"   ⚠️  Error Claude para '{cliente}': {e}")
        return {
            "churn_detectado": "sin datos", "categoria": "Sin clasificar",
            "subcategorias": [], "nivel_riesgo": "bajo",
            "motivo_principal": "Error en análisis", "resumen_ia": "",
            "confianza": 0.0
        }


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print(f"🚀 Análisis PRD v2 iniciado — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"   Modelo: claude-sonnet-4-6  |  Confianza mínima: {CONFIDENCE_THRESHOLD:.0%}")
    print(f"   HubSpot API: {'✅ configurado' if HUBSPOT_API_KEY else '⚠️  no configurado'}")

    spreadsheet = conectar_sheets()
    ws_source   = spreadsheet.worksheet(SOURCE_TAB)
    df          = pd.DataFrame(ws_source.get_all_records())
    print(f"✅ {len(df)} filas cargadas de '{SOURCE_TAB}'")

    # Detect columns
    def find_col(df, *keywords):
        for c in df.columns:
            cl = c.lower()
            if any(kw in cl for kw in keywords):
                return c
        return None

    col_llamada       = find_col(df, "llamada", "call")
    col_titulo        = find_col(df, "título", "titulo", "title")
    col_participantes = find_col(df, "participante")
    col_duracion      = find_col(df, "duración", "duracion", "duration")
    col_fecha         = find_col(df, "fecha", "date")
    col_dueno         = find_col(df, "dueño", "dueno", "owner")
    col_transcript    = next((c for c in df.columns if c.lower() == "transcript"), None) or \
                        find_col(df, "transcripción", "transcripcion", "notas", "contenido", "texto")
    col_resumen_exist = find_col(df, "resumen")
    col_cat_exist     = find_col(df, "categoría", "categoria")
    col_conf_exist    = find_col(df, "confianza")

    print(f"   Transcript: {col_transcript}  |  Resumen: {col_resumen_exist}")

    # Filter by target owners
    df_filtrado = df[df[col_dueno].isin(DUENOS_OBJETIVO)].copy() if col_dueno else df.copy()

    # --------------------------------------------------------
    # PRD v2 RULE: No re-analysis of already classified rows
    # Skip rows that already have: category assigned AND confidence ≥ 8/10
    # Only process: (1) new rows without category, (2) "Sin clasificar" rows
    # --------------------------------------------------------
    rows_to_analyze = df_filtrado
    skipped = 0

    if col_cat_exist and col_conf_exist:
        def parse_conf(val):
            s = str(val).rstrip('%').strip()
            try:
                f = float(s)
                return f / 100.0 if f > 1 else f
            except Exception:
                return 0.0

        mask_already_done = (
            df_filtrado[col_cat_exist].notna() &
            (df_filtrado[col_cat_exist] != "") &
            (df_filtrado[col_cat_exist] != "Sin clasificar") &
            (df_filtrado[col_cat_exist] != "Sin categoría") &
            (df_filtrado[col_conf_exist].apply(parse_conf) >= CONFIDENCE_THRESHOLD)
        )
        already_done = df_filtrado[mask_already_done]
        rows_to_analyze = df_filtrado[~mask_already_done]
        skipped = len(already_done)
        print(f"   ⏭️  {skipped} filas ya clasificadas (confianza ≥ {CONFIDENCE_THRESHOLD:.0%}) → saltadas")

    print(f"✅ {len(rows_to_analyze)} filas para analizar")

    if rows_to_analyze.empty:
        print("✅ Sin filas nuevas para procesar.")
        exit()

    resultados = []
    total = len(rows_to_analyze)

    for idx, (_, row) in enumerate(rows_to_analyze.iterrows(), 1):
        call_id       = str(row.get(col_llamada, "")).strip()        if col_llamada       else ""
        titulo        = str(row.get(col_titulo, "")).strip()         if col_titulo        else ""
        participantes = str(row.get(col_participantes, "")).strip()  if col_participantes else ""
        fecha         = str(row.get(col_fecha, "")).strip()          if col_fecha         else ""
        dueno         = str(row.get(col_dueno, "")).strip()          if col_dueno         else ""
        duracion      = str(row.get(col_duracion, "")).strip()       if col_duracion      else ""
        transcript    = str(row.get(col_transcript, "")).strip()     if col_transcript    else ""
        resumen_exist = str(row.get(col_resumen_exist, "")).strip()  if col_resumen_exist else ""

        cliente = extraer_cliente(participantes, dueno)
        print(f"\n[{idx}/{total}] {dueno or 'N/A'} — {cliente or titulo or call_id}")

        fuente = "transcript"
        if transcript:
            print(f"   📄 Fuente: transcript ({len(transcript)} chars)")
        elif resumen_exist:
            transcript = resumen_exist
            fuente     = "resumen_existente"
            print(f"   📄 Fuente: resumen existente ({len(transcript)} chars) — sin transcript disponible")
        else:
            print(f"   ⚠️  Sin transcript ni resumen — llamada ID: {call_id}")

        # ---- STEP 1+2: Two-step root cause analysis ----
        analisis  = analizar_transcript(transcript, cliente, titulo)
        confianza = float(analisis.get("confianza", 0.0))
        subcats   = analisis.get("subcategorias", [])
        subcats_str = " | ".join(subcats) if subcats else ""

        print(f"   → Churn: {analisis['churn_detectado']} | Cat: {analisis['categoria']} | "
              f"Conf: {confianza:.0%} | Subs: {subcats_str or '–'}")

        if analisis["categoria"] == "Sin clasificar":
            print(f"   ⚠️  Confianza < {CONFIDENCE_THRESHOLD:.0%} → marcado como 'Sin clasificar' para revisión manual")

        # ---- Sprint 2: HubSpot enrichment ----
        hs = {}
        if HUBSPOT_API_KEY:
            hs = buscar_hubspot(cliente)
            if hs.get("sin_registro"):
                print(f"   🔍 HubSpot: Sin registro para '{cliente}'")
            else:
                print(f"   ✅ HubSpot: {hs.get('name','')} | Type: {hs.get('saas_client_type','')} | Reason: {hs.get('churn_reason','')}")
                # If category is "Sin clasificar" and HubSpot has a reason, use it as fallback
                if analisis["categoria"] == "Sin clasificar" and hs.get("churn_reason"):
                    analisis["categoria"] = hs["churn_reason"]
                    print(f"   → Categoría de HubSpot aplicada: {analisis['categoria']}")

        resultados.append({
            "Dueño":              dueno,
            "Fecha":              fecha,
            "Cliente":            cliente,
            "Participantes":      participantes,
            "Duración (min)":     duracion,
            "Llamada ID":         call_id,
            "Churn detectado":    analisis["churn_detectado"],
            "Categoría":          analisis["categoria"],
            "Subcategoría 1":     subcats[0] if len(subcats) > 0 else "",
            "Subcategoría 2":     subcats[1] if len(subcats) > 1 else "",
            "Subcategoría 3":     subcats[2] if len(subcats) > 2 else "",
            "Nivel de riesgo":    analisis["nivel_riesgo"],
            "Motivo principal":   analisis["motivo_principal"],
            "Paso 1 comprensión": analisis.get("paso1_comprension", ""),
            "Resumen IA":         analisis["resumen_ia"],
            # Sprint 2 fields
            "SaaS Client Type":   hs.get("saas_client_type", ""),
            "HS Churn Reason":    hs.get("churn_reason_raw", ""),
            "HS Churn Date":      hs.get("churn_date", ""),
            "HS URL":             hs.get("hs_url", ""),
            "Sin registro HS":    "Sí" if hs.get("sin_registro") else "No",
            "Fuente análisis":    fuente,
            "Confianza IA":       f"{confianza:.0%}",
            "Actualizado":        datetime.now().strftime("%Y-%m-%d %H:%M")
        })
        time.sleep(0.5)

    # ---- Save results to Análisis_Churn ----
    output_tab = "Análisis_Churn"
    try:
        ws_res = spreadsheet.worksheet(output_tab)
        ws_res.clear()
    except Exception:
        ws_res = spreadsheet.add_worksheet(title=output_tab, rows=2000, cols=25)

    if resultados:
        df_res = pd.DataFrame(resultados)
        ws_res.update([df_res.columns.tolist()] + df_res.values.tolist())
        print(f"\n✅ {len(resultados)} resultados guardados en '{output_tab}'")

    # ---- Write AI columns back to Resultados_Churn so the dashboard can read them ----
    if resultados:
        print(f"\n📝 Escribiendo categorías IA de vuelta a '{SOURCE_TAB}'...")
        import gspread.utils as gu

        # Get current headers (row 1)
        headers = ws_source.row_values(1)

        # AI columns to sync back
        ai_col_names = [
            'Categoría IA', 'Subcategoría 1', 'Subcategoría 2',
            'Subcategoría 3', 'Churn IA', 'Confianza IA'
        ]

        # Find or create column positions (1-indexed)
        col_positions = {}
        next_col_idx = len(headers) + 1
        header_updates = []

        for col_name in ai_col_names:
            found = None
            for i, h in enumerate(headers):
                if h.strip() == col_name:
                    found = i + 1  # 1-indexed
                    break
            if found:
                col_positions[col_name] = found
            else:
                col_positions[col_name] = next_col_idx
                header_updates.append({'range': gu.rowcol_to_a1(1, next_col_idx), 'values': [[col_name]]})
                next_col_idx += 1

        if header_updates:
            ws_source.batch_update(header_updates)
            print(f"   ➕ Añadidas {len(header_updates)} columnas AI a '{SOURCE_TAB}'")

        # Batch update each analyzed row
        cell_updates = []
        for orig_idx, res in zip(rows_to_analyze.index, resultados):
            sheet_row = int(orig_idx) + 2  # +1 header, +1 for 0-to-1 indexing
            vals = {
                'Categoría IA':   res['Categoría'],
                'Subcategoría 1': res.get('Subcategoría 1', ''),
                'Subcategoría 2': res.get('Subcategoría 2', ''),
                'Subcategoría 3': res.get('Subcategoría 3', ''),
                'Churn IA':       res['Churn detectado'],
                'Confianza IA':   res['Confianza IA'],
            }
            for col_name, value in vals.items():
                cell_updates.append({
                    'range': gu.rowcol_to_a1(sheet_row, col_positions[col_name]),
                    'values': [[str(value) if value is not None else '']]
                })

        if cell_updates:
            # gspread batch_update accepts max ~1000 ranges per call; chunk if needed
            chunk = 500
            for i in range(0, len(cell_updates), chunk):
                ws_source.batch_update(cell_updates[i:i+chunk])
            print(f"✅ {len(rows_to_analyze)} filas actualizadas en '{SOURCE_TAB}' con categorías IA")

        n_churn      = len(df_res[df_res["Churn detectado"] == "sí"])
        n_riesgo     = len(df_res[df_res["Churn detectado"] == "riesgo"])
        n_sin_cls    = len(df_res[df_res["Categoría"] == "Sin clasificar"])
        n_sin_hs     = len(df_res[df_res["Sin registro HS"] == "Sí"])
        cats         = df_res["Categoría"].value_counts().to_dict()

        print(f"\n📊 Resumen PRD v2:")
        print(f"   Total procesadas : {len(resultados)}")
        print(f"   Saltadas (ya OK) : {skipped}")
        print(f"   Churn detectado  : {n_churn}")
        print(f"   Riesgo           : {n_riesgo}")
        print(f"   Sin clasificar   : {n_sin_cls}  ← revisión manual")
        print(f"   Sin registro HS  : {n_sin_hs}")
        print(f"\n   Categorías:")
        for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
            print(f"   - {cat}: {cnt}")

    print("\n✅ Análisis PRD v2 completado.")
