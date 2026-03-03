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
# CONFIG
# ============================================================
SHEET_URL       = "https://docs.google.com/spreadsheets/d/16Tv2U164NammrDBe2u8b1_BuuHiWCkHaO1J5mFkl6OY"
SOURCE_TAB      = "Resultados_Churn"   # pestaña donde Modjo escribe los datos + transcripts
RESULTS_TAB     = "Resultados_Churn"   # misma pestaña — el script añade columnas de análisis

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HUBSPOT_API_KEY   = os.environ.get("HUBSPOT_API_KEY", "")
# Modjo sincroniza transcripciones directamente al Google Sheet vía Google credentials
# No se necesita API key separada de Modjo

# Umbral de confianza: si Claude devuelve confianza < esto, consultamos HubSpot
CONFIDENCE_THRESHOLD = 0.65

DUENOS_OBJETIVO = [
    "Victor Ortega",
    "Óscar Lopo",
    "Martina Benalcazar",
    "Kamila Jiménez",
    "Gonzalo Rosales",
    "Franco Ferretti"
]

CATEGORIAS_CHURN = [
    "Fallos técnicos",
    "Onboarding deficiente",
    "Expectativas no cumplidas",
    "Precio / ROI bajo",
    "Soporte deficiente",
    "Competencia",
    "Cierre de negocio",
    "Sin categoría"
]

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
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)


# ============================================================
# EXTRAER NOMBRE DE CLIENTE DE PARTICIPANTES
# ============================================================
def extraer_cliente(participantes: str, dueno: str) -> str:
    """
    De la columna 'Participantes' (ej: "Carla Blanch, Ruby Estefany Labrador Rosales"),
    filtra el nombre del owner y devuelve el nombre del cliente.
    """
    if not participantes:
        return ""

    partes = [p.strip() for p in participantes.replace(";", ",").split(",") if p.strip()]
    if not partes:
        return participantes

    if len(partes) == 1:
        return partes[0]

    dueno_lower = (dueno or "").lower()
    dueno_parts = [p for p in dueno_lower.split() if len(p) > 2]

    clientes = []
    for parte in partes:
        parte_lower = parte.lower()
        es_dueno = any(dp in parte_lower for dp in dueno_parts)
        if not es_dueno:
            clientes.append(parte)

    return clientes[0] if clientes else partes[0]


# ============================================================
# HUBSPOT — buscar motivo de churn por nombre de empresa
# ============================================================
def buscar_motivo_hubspot(cliente: str) -> dict:
    """
    Consulta HubSpot por el nombre del cliente para obtener churn_reason_saas.
    Solo se llama cuando la confianza del análisis de transcript es baja.
    """
    if not HUBSPOT_API_KEY or not cliente:
        return {}

    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "name",
                "operator": "CONTAINS_TOKEN",
                "value": cliente.strip().split()[0]
            }]
        }],
        "properties": ["name", "churn_status", "churn_reason_saas", "fecha_de_solicitud_de_baja"],
        "limit": 5
    }

    try:
        r = requests.post(
            "https://api.hubapi.com/crm/v3/objects/companies/search",
            headers=headers,
            json=payload,
            timeout=10
        )
        if r.status_code != 200:
            return {}

        results = r.json().get("results", [])
        if not results:
            return {}

        cliente_lower = cliente.lower()
        best = None
        for company in results:
            props = company.get("properties", {})
            hs_name = (props.get("name") or "").lower()
            if cliente_lower in hs_name or hs_name in cliente_lower:
                best = props
                break
        if not best:
            best = results[0].get("properties", {})

        traducciones = {
            "Sales Miscommunication":                 "Expectativas no cumplidas",
            "Poor CX / Platform Issues":              "Fallos técnicos",
            "Quality Issues":                         "Fallos técnicos",
            "Support or Service Issues":              "Soporte deficiente",
            "Onboarding Failure":                     "Onboarding deficiente",
            "Does not see impact on their business":  "Precio / ROI bajo",
            "WTP < Cost":                             "Precio / ROI bajo",
            "Outside The Scope of the Plan":          "Expectativas no cumplidas",
            "Unrealistic promise":                    "Expectativas no cumplidas",
            "Forced Sales Closing":                   "Expectativas no cumplidas",
            "Business Closed":                        "Cierre de negocio",
        }

        churn_reason_raw = best.get("churn_reason_saas", "")
        churn_reason_es  = traducciones.get(churn_reason_raw, churn_reason_raw)

        return {
            "churn_status":  best.get("churn_status", ""),
            "churn_reason":  churn_reason_es,
            "fecha_baja":    best.get("fecha_de_solicitud_de_baja", "")
        }

    except Exception as e:
        print(f"   ⚠️  Error HubSpot para '{cliente}': {e}")
        return {}


# ============================================================
# ANÁLISIS PRINCIPAL CON CLAUDE (sobre transcripción completa)
# ============================================================
def analizar_transcript(transcript: str, cliente: str, titulo: str) -> dict:
    """
    Envía la transcripción completa a Claude para extraer churn, categoría,
    motivo, resumen y nivel de confianza.
    """
    if not transcript or len(transcript.strip()) < 50:
        return {
            "churn_detectado":  "sin datos",
            "categoria":        "Sin categoría",
            "nivel_riesgo":     "bajo",
            "motivo_principal": "Sin transcripción disponible",
            "resumen_ia":       "No se encontró transcripción para esta llamada.",
            "confianza":        0.0
        }

    ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    transcript_truncado = transcript[:8000] if len(transcript) > 8000 else transcript

    prompt = f"""Eres un analista de Revenue Operations especializado en churn de SaaS.

Analiza esta transcripción de llamada con el cliente "{cliente}" (título: "{titulo}").

TRANSCRIPCIÓN:
{transcript_truncado}

---

Extrae la siguiente información respondiendo ÚNICAMENTE en JSON válido:

{{
  "churn_detectado": "sí | no | riesgo",
  "categoria": "una de: Fallos técnicos | Onboarding deficiente | Expectativas no cumplidas | Precio / ROI bajo | Soporte deficiente | Competencia | Cierre de negocio | Sin categoría",
  "nivel_riesgo": "alto | medio | bajo",
  "motivo_principal": "Frase concisa en español explicando el motivo principal detectado en la llamada",
  "resumen_ia": "Resumen ejecutivo de 2-3 frases en español sobre lo más importante de la llamada",
  "confianza": 0.0
}}

Criterios:
- "churn_detectado": "sí" si el cliente ya solicitó baja o claramente se va; "riesgo" si hay señales de insatisfacción sin confirmar; "no" si la llamada es normal.
- "categoria": razón principal del churn o riesgo. Si no hay señal clara, usa "Sin categoría".
- "confianza": entre 0 y 1. Pon 0.9 si la causa es muy clara en el transcript. Pon 0.4 si es vaga o dudosa. Pon 0.1 si el transcript está vacío o es ininteligible.
"""

    try:
        msg = ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )
        texto = msg.content[0].text.strip()
        parsed = json.loads(texto[texto.find("{"):texto.rfind("}")+1])
        parsed.setdefault("churn_detectado",  "no")
        parsed.setdefault("categoria",        "Sin categoría")
        parsed.setdefault("nivel_riesgo",     "bajo")
        parsed.setdefault("motivo_principal", "")
        parsed.setdefault("resumen_ia",       "")
        parsed.setdefault("confianza",        0.5)
        return parsed
    except Exception as e:
        print(f"   ⚠️  Error Claude para '{cliente}': {e}")
        return {
            "churn_detectado":  "sin datos",
            "categoria":        "Sin categoría",
            "nivel_riesgo":     "bajo",
            "motivo_principal": "Error en análisis",
            "resumen_ia":       "",
            "confianza":        0.0
        }


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print(f"🚀 Análisis iniciado — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"   Modjo → Google Sheets: ✅ transcripciones leídas directamente del sheet")
    print(f"   HubSpot API: {'✅ configurado (fallback activo)' if HUBSPOT_API_KEY else '⚠️  no configurado (sin fallback)'}")

    # ── Conectar a Google Sheets ──────────────────────────────
    spreadsheet = conectar_sheets()
    ws_source   = spreadsheet.worksheet(SOURCE_TAB)
    df          = pd.DataFrame(ws_source.get_all_records())
    print(f"✅ {len(df)} filas cargadas de '{SOURCE_TAB}'")

    # ── Detectar columnas ─────────────────────────────────────
    col_llamada       = next((c for c in df.columns if "llamada" in c.lower() or "call" in c.lower()), None)
    col_titulo        = next((c for c in df.columns if "título" in c.lower() or "titulo" in c.lower() or "title" in c.lower()), None)
    col_participantes = next((c for c in df.columns if "participante" in c.lower()), None)
    col_duracion      = next((c for c in df.columns if "duración" in c.lower() or "duracion" in c.lower() or "duration" in c.lower()), None)
    col_fecha         = next((c for c in df.columns if "fecha" in c.lower() or "date" in c.lower()), None)
    col_dueno         = next((c for c in df.columns if "dueño" in c.lower() or "dueno" in c.lower() or "owner" in c.lower()), None)
    col_transcript    = next((c for c in df.columns if c.lower() == "transcript"), None) or \
                        next((c for c in df.columns if any(kw in c.lower() for kw in
                              ["transcripción", "transcripcion", "notas", "contenido", "texto"])), None)
    col_resumen_exist = next((c for c in df.columns if "resumen" in c.lower()), None)

    print(f"\n   Columnas detectadas:")
    print(f"   - Llamada:        {col_llamada}")
    print(f"   - Título:         {col_titulo}")
    print(f"   - Participantes:  {col_participantes}")
    print(f"   - Transcript:     {col_transcript}  ← fuente principal")
    print(f"   - Fecha:          {col_fecha}")
    print(f"   - Dueño:          {col_dueno}")
    print(f"   - Duración:       {col_duracion}")

    if not col_transcript:
        print(f"\n   ⚠️  No se encontró columna 'transcript'.")
        print(f"   Columnas disponibles: {df.columns.tolist()}")

    # ── Filtrar por owners objetivo ───────────────────────────
    if col_dueno:
        df_filtrado = df[df[col_dueno].isin(DUENOS_OBJETIVO)].copy()
    else:
        df_filtrado = df.copy()
        print("   ⚠️  No se encontró columna de Dueño — se procesarán todas las filas")

    print(f"\n✅ {len(df_filtrado)} filas para analizar")

    if df_filtrado.empty:
        print("⚠️  Sin filas para procesar. Columnas disponibles:", df.columns.tolist())
        exit()

    # ── Procesar cada llamada ─────────────────────────────────
    resultados = []
    total = len(df_filtrado)

    for idx, (_, row) in enumerate(df_filtrado.iterrows(), 1):
        call_id       = str(row.get(col_llamada, "")).strip()        if col_llamada       else ""
        titulo        = str(row.get(col_titulo, "")).strip()         if col_titulo        else ""
        participantes = str(row.get(col_participantes, "")).strip()  if col_participantes else ""
        fecha         = str(row.get(col_fecha, "")).strip()          if col_fecha         else ""
        dueno         = str(row.get(col_dueno, "")).strip()          if col_dueno         else ""
        duracion      = str(row.get(col_duracion, "")).strip()       if col_duracion      else ""
        transcript    = str(row.get(col_transcript, "")).strip()     if col_transcript    else ""
        resumen_exist = str(row.get(col_resumen_exist, "")).strip()  if col_resumen_exist else ""

        # Extraer nombre del cliente desde Participantes
        cliente = extraer_cliente(participantes, dueno)

        print(f"\n[{idx}/{total}] {dueno or 'N/A'} — {cliente or titulo or call_id}")

        # Fuente del texto a analizar
        fuente = "transcript"
        if not transcript and resumen_exist:
            transcript = resumen_exist
            fuente = "resumen_existente"
            print(f"   ℹ️  Sin transcript — usando resumen pre-existente")
        elif not transcript:
            print(f"   ⚠️  Sin transcript — llamada ID: {call_id}")

        # 1. Analizar con Claude
        analisis  = analizar_transcript(transcript, cliente, titulo)
        confianza = float(analisis.get("confianza", 0.5))

        print(f"   → Churn: {analisis['churn_detectado']} | Cat: {analisis['categoria']} | "
              f"Riesgo: {analisis['nivel_riesgo']} | Confianza: {confianza:.0%}")

        # 2. Fallback a HubSpot si la categoría es incierta
        motivo_hubspot  = ""
        fecha_baja_hs   = ""
        churn_status_hs = ""

        if (confianza < CONFIDENCE_THRESHOLD or analisis["categoria"] == "Sin categoría") and HUBSPOT_API_KEY:
            print(f"   🔍 Confianza baja ({confianza:.0%}) → consultando HubSpot...")
            hs = buscar_motivo_hubspot(cliente)
            if hs:
                if hs.get("churn_reason"):
                    analisis["categoria"] = hs["churn_reason"]
                if hs.get("churn_status") == "Churn":
                    analisis["churn_detectado"] = "sí"
                    analisis["nivel_riesgo"]    = "alto"
                motivo_hubspot  = hs.get("churn_reason", "")
                fecha_baja_hs   = hs.get("fecha_baja", "")
                churn_status_hs = hs.get("churn_status", "")
                print(f"   ✅ HubSpot → {motivo_hubspot} | Status: {churn_status_hs}")
            else:
                print(f"   ℹ️  No encontrado en HubSpot")

        resultados.append({
            "Dueño":            dueno,
            "Fecha":            fecha,
            "Cliente":          cliente,
            "Participantes":    participantes,
            "Duración (min)":   duracion,
            "Llamada ID":       call_id,
            "Churn detectado":  analisis["churn_detectado"],
            "Categoría":        analisis["categoria"],
            "Nivel de riesgo":  analisis["nivel_riesgo"],
            "Motivo principal": analisis["motivo_principal"],
            "Resumen IA":       analisis["resumen_ia"],
            "Motivo HubSpot":   motivo_hubspot,
            "Fecha baja HS":    fecha_baja_hs,
            "HubSpot status":   churn_status_hs,
            "Fuente análisis":  fuente,
            "Confianza IA":     f"{confianza:.0%}",
            "Actualizado":      datetime.now().strftime("%Y-%m-%d %H:%M")
        })

        time.sleep(0.5)

    # ── Guardar resultados en nueva pestaña ───────────────────
    output_tab = "Análisis_Churn"
    try:
        ws_res = spreadsheet.worksheet(output_tab)
        ws_res.clear()
    except:
        ws_res = spreadsheet.add_worksheet(title=output_tab, rows=2000, cols=20)

    if resultados:
        df_res = pd.DataFrame(resultados)
        ws_res.update([df_res.columns.tolist()] + df_res.values.tolist())
        print(f"\n✅ {len(resultados)} resultados guardados en '{output_tab}'")

        # Estadísticas
        n_churn   = len(df_res[df_res["Churn detectado"] == "sí"])
        n_riesgo  = len(df_res[df_res["Churn detectado"] == "riesgo"])
        n_hs      = len(df_res[df_res["Motivo HubSpot"] != ""])
        n_sin_cat = len(df_res[df_res["Categoría"] == "Sin categoría"])
        cats      = df_res["Categoría"].value_counts().to_dict()

        print(f"\n📊 Resumen:")
        print(f"   Total:          {len(resultados)}")
        print(f"   Churn:          {n_churn} ({round(n_churn/len(resultados)*100)}%)")
        print(f"   Riesgo:         {n_riesgo}")
        print(f"   Fallback HS:    {n_hs}")
        print(f"   Sin categoría:  {n_sin_cat}")
        print(f"\n   Categorías:")
        for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
            print(f"   - {cat}: {cnt}")

    print("\n✅ Análisis completado.")
