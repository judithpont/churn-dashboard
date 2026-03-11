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
HUBSPOT_PORTAL_ID = "25808060"

# Más estricto: 8/10 real
CONFIDENCE_THRESHOLD = 0.80

# False = no re-analiza todo el histórico, solo lo necesario
FORCE_REANALYZE = False

# Límite manual temporal para este mes
MAX_FILAS_MES_ACTUAL = 150

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
    "Problemas de calidad",
    "Sin impacto real en su negocio",
    "Promesa irreal",
    "Negocio cerrado",
    "Fuera del alcance del plan",
    "Problema de soporte",
    "Fallo en el onboarding",
    "No justifica precio",
]

SUBCATEGORIAS = {
    "Problemas de calidad": [
        "baja-velocidad-entrega", "contenido-generico", "servicio-no-entregado",
        "contenido-ai-rechazado", "identidad-visual-no-respetada", "cambios-no-aplicados"
    ],
    "Fallo en la plataforma": [
        "errores-tecnicos", "publicaciones-fallan-calendario", "fallo-conexion-rrss",
        "friccion-ux-app", "problemas-modulo-maya", "problemas-inicio-sesion"
    ],
    "Problema de soporte": [
        "respuesta-soporte-lenta", "problemas-no-resueltos", "falta-soporte-dedicado"
    ],
    "Fuera del alcance del plan": [
        "funcionalidades-faltantes", "integracion-no-disponible", "automatizacion-insuficiente"
    ],
    "Fallo en el onboarding": [
        "herramienta-no-adoptada", "herramienta-demasiado-compleja",
        "falta-recursos-formacion", "objetivos-iniciales-no-alcanzados"
    ],
    "Sin impacto real en su negocio": [
        "precio-demasiado-alto", "mejor-valor-en-competencia", "roi-no-justificado"
    ],
    "No justifica precio": [
        "precio-demasiado-alto", "mejor-valor-en-competencia", "roi-no-justificado"
    ],
    "Cierre de venta forzado": [
        "arrepentimiento-compra", "cliente-fuera-icp",
        "desalineacion-facturacion", "cliente-solo-promocion"
    ],
    "Promesa irreal": [
        "funcionalidad-prometida-no-disponible", "servicio-estilo-agencia-prometido",
        "precio-prometido-incorrecto"
    ],
    "Negocio cerrado": [
        "negocio-cerrado", "reduccion-personal"
    ],
}

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
# HUBSPOT
# ============================================================
def buscar_hubspot(cliente: str) -> dict:
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
        print(f"   ⚠️  Error HubSpot para '{cliente}': {e}", flush=True)
        return {"sin_registro": True}


# ============================================================
# ANÁLISIS CON CLAUDE
# ============================================================
def analizar_transcript(transcript: str, cliente: str, titulo: str, fuente: str = "transcript") -> dict:
    if not transcript or len(transcript.strip()) < 50:
        return {
            "churn_detectado": "sin datos", "categoria": "Sin clasificar",
            "subcategorias": [], "nivel_riesgo": "bajo",
            "motivo_principal": "Sin transcripción disponible",
            "resumen_ia": "No se encontró transcripción para esta llamada.",
            "confianza": 0.0
        }

    ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    transcript_truncado = transcript[:8000]

    subcats_text = "\n".join(
        f"  {cat}:\n" + "\n".join(f"    · {s}" for s in subs)
        for cat, subs in SUBCATEGORIAS.items()
    )

    system_prompt = """Eres un analista senior de Revenue Operations en Plinng, empresa SaaS española de gestión y creación de contenido para redes sociales con IA.

SOBRE PLINNG:
Plinng es una plataforma SaaS de suscripción mensual/anual que ayuda a pymes y agencias a gestionar y hacer crecer su presencia digital. Los servicios que ofrece incluyen:
- Creación de contenido asistida por IA (posts, copy, imágenes) para redes sociales
- Módulo de calendario editorial y publicación automática en RRSS (Instagram, Facebook, LinkedIn, etc.)
- Módulo Maya (funcionalidades avanzadas de gestión visual/diseño)
- Conexión directa con cuentas de redes sociales
- Gestión de reputación online: monitorización y respuesta a reseñas en plataformas como Google Business, Trustpilot, etc.
- Posicionamiento SEO: optimización para buscadores, contenido SEO, visibilidad en Google
- Creación y mantenimiento de páginas web (landing pages, webs corporativas)
- Soporte y onboarding incluido según plan
El cliente contrata un plan y espera que Plinng gestione uno o varios de estos servicios de forma autónoma o asistida.

TAXONOMÍA OFICIAL DE CHURN — definiciones precisas:

1. Fallo en la plataforma
   CUÁNDO: El producto tiene errores técnicos. Posts que no se publican, la app falla, conexión con RRSS rota, calendario no funciona, login imposible.
   EL PROBLEMA ES: el software no funciona correctamente.

2. Problemas de calidad
   CUÁNDO: La plataforma funciona, pero el CONTENIDO entregado no cumple expectativas. Posts genéricos, identidad visual no respetada, cambios no aplicados, contenido IA rechazado.
   EL PROBLEMA ES: lo que producen no es bueno, no lo que falla técnicamente.

3. Problema de soporte
   CUÁNDO: El cliente tuvo un problema (técnico o de calidad) pero el equipo de soporte tardó demasiado, no lo resolvió, o el cliente siente abandono.
   EL PROBLEMA ES: la atención al cliente fue deficiente.

4. Fuera del alcance del plan
   CUÁNDO: El cliente necesita una funcionalidad, integración o nivel de automatización que Plinng NO ofrece en su plan (o en ningún plan).
   EL PROBLEMA ES: el producto no tiene lo que el cliente necesita.

5. Fallo en el onboarding
   CUÁNDO: El cliente nunca llegó a usar bien la herramienta. La adoptó poco, le pareció demasiado compleja, no recibió formación suficiente, o los objetivos de arranque no se cumplieron.
   EL PROBLEMA ES: el cliente no se incorporó bien al producto.

6. Sin impacto real en su negocio
   CUÁNDO: El cliente usó el producto durante un tiempo, pero NO vio resultados ni retorno (más seguidores, más clientes, más ventas). El producto funcionó técnicamente pero no generó valor de negocio percibido.
   EL PROBLEMA ES: ROI no demostrado o percepción de que "no les sirve de nada".

7. No justifica precio
   CUÁNDO: El cliente reconoce que el producto funciona o tiene valor, pero considera que el precio es demasiado alto para lo que recibe, o hay alternativas más baratas.
   EL PROBLEMA ES: relación precio/valor subjetiva, no ausencia de impacto.

8. Cierre de venta forzado
   CUÁNDO: El cliente no era el cliente ideal desde el principio: firmó solo por una promoción, no encaja en el ICP, hay una desalineación en la facturación (pagó más de lo esperado), o claramente se arrepintió al poco de contratar.
   EL PROBLEMA ES: el cliente nunca debió haber sido vendido.

9. Promesa irreal
    CUÁNDO: El vendedor prometió activamente algo que NO existe o que Plinng NO puede cumplir: una funcionalidad inexistente, un nivel de servicio de agencia, un precio que no corresponde.
    EL PROBLEMA ES: promesa deliberada o sistémica que el producto no puede cumplir.

10. Negocio cerrado
    CUÁNDO: El cliente cierra su empresa, reduce drásticamente personal o tiene razones externas completamente ajenas al producto o servicio de Plinng.
    EL PROBLEMA ES: causa externa, no relacionada con Plinng.

GUÍA DE DESEMPATE — cuando dos categorías parezcan similares:

▸ "Sin impacto real" vs "No justifica precio":
  → ¿El cliente dice "no funciona" / "no nos ha traído resultados"? → Sin impacto real
  → ¿El cliente dice "funciona pero es muy caro" / "hay algo más barato"? → No justifica precio

▸ "Fallo en la plataforma" vs "Problemas de calidad":
  → ¿El fallo es técnico (app, conexión, publicación automática)? → Fallo en la plataforma
  → ¿El fallo es en el contenido producido (calidad, estilo, cambios)? → Problemas de calidad

▸ "Problema de soporte" vs otras categorías con soporte mencionado:
  → ¿El soporte deficiente ES el motivo principal del churn? → Problema de soporte
  → ¿El soporte fue mencionado de pasada pero el problema real es otro? → Usa la categoría del problema real"""

    if fuente == "resumen_existente":
        fuente_contexto = """FORMATO DEL TEXTO: Es un RESUMEN generado automáticamente por Modjo (IA), escrito en 3ª persona.
No contiene turnos de conversación reales. Describe lo que ocurrió en la llamada de forma condensada.
Trata todo el contenido como la visión del cliente, extrayendo sus problemas, expectativas y frustraciones."""
    else:
        fuente_contexto = f"""FORMATO DEL TEXTO: Es la TRANSCRIPCIÓN COMPLETA de la llamada entre un representante de Plinng y el cliente "{cliente}".
Contiene turnos de conversación alternos. IMPORTANTE:
- Identifica qué speaker es el CLIENTE (quien llama o quien tiene el problema).
- Ignora las frases del representante de Plinng al analizar el motivo de churn.
- Basa el análisis EXCLUSIVAMENTE en lo que dice el cliente: sus quejas, sus expectativas, sus frustraciones.
- Si el rep de Plinng menciona un problema ("veo que has tenido problemas con X"), no lo uses como evidencia a menos que el cliente lo confirme."""

    prompt = f"""Analiza el siguiente texto de llamada con el cliente "{cliente}" (título: "{titulo}").

{fuente_contexto}

---

TEXTO:
{transcript_truncado}

---

INSTRUCCIONES — razona en DOS PASOS antes de responder:

PASO 1 — COMPRENSIÓN:
Lee el texto completo. Extrae basándote SOLO en lo que expresa el cliente:
  a) ¿Qué le molestó específicamente al cliente?
  b) ¿Qué esperaba recibir y no recibió?
  c) ¿Qué fue lo que realmente lo empujó a irse o a estar insatisfecho?
  (No etiquetes todavía. Solo comprende la experiencia completa del cliente.)

PASO 2 — ROOT CAUSE:
Con ese contexto, usa las definiciones y la guía de desempate del sistema para identificar el root cause REAL.
Elige la categoría cuya definición encaje con la experiencia del cliente, no con las palabras que usó.

Sub-motivos válidos por categoría (usa SOLO los de la categoría que elijas):
{subcats_text}

---

Responde ÚNICAMENTE en JSON válido con este formato exacto:

{{
  "paso1_comprension": "2-3 frases: qué le molestó, qué esperaba, qué no recibió",
  "razon_categoria": "1 frase explicando por qué elegiste ESA categoría y no otra similar",
  "churn_detectado": "sí | no | riesgo",
  "categoria": "una de las 10 categorías del sistema, o null si no aplica",
  "subcategorias": [],
  "nivel_riesgo": "alto | medio | bajo",
  "motivo_principal": "frase concisa en español del root cause real",
  "resumen_ia": "resumen ejecutivo de 2-3 frases en español",
  "confianza": 8
}}

Reglas de salida:
- "subcategorias": ÚNICAMENTE sub-motivos de la categoría elegida, respaldados por la transcripción. Array vacío si ninguno encaja. Máximo 3, de mayor a menor peso.
- "confianza": 1-10 (no 0-1). 9-10 = causa muy clara; 7-8 = señales claras con algo de ambigüedad; 5-6 = señales pero ambiguas; 3-4 = transcript vago; 1-2 = ininteligible o sin datos.
- "categoria": null si confianza < 8 → quedará como "Sin clasificar" para revisión manual.
- "churn_detectado": "sí" si el cliente ya solicitó baja o claramente se va; "riesgo" si hay señales sin confirmar; "no" si la llamada es normal.
"""

    try:
        msg = ai.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1200,
            temperature=0,
            system=system_prompt,
            messages=[{"role": "user", "content": prompt}]
        )
        texto = msg.content[0].text.strip()
        parsed = json.loads(texto[texto.find("{"):texto.rfind("}")+1])

        raw_conf = parsed.get("confianza", 5)
        if isinstance(raw_conf, (int, float)) and raw_conf > 1:
            confianza = raw_conf / 10.0
        else:
            confianza = float(raw_conf)

        if confianza < CONFIDENCE_THRESHOLD:
            parsed["categoria"] = "Sin clasificar"
            parsed["subcategorias"] = []

        cat_assigned = parsed.get("categoria", "Sin clasificar") or "Sin clasificar"
        subs = parsed.get("subcategorias", [])
        if not isinstance(subs, list):
            subs = [str(subs)] if subs else []

        valid_subs = set(SUBCATEGORIAS.get(cat_assigned, []))
        subs_normalizadas = []
        for s in subs:
            s_norm = str(s).strip().lower().replace(" ", "-")
            if s_norm in valid_subs:
                subs_normalizadas.append(s_norm)
        parsed["subcategorias"] = subs_normalizadas[:3]

        parsed.setdefault("churn_detectado", "no")
        parsed.setdefault("categoria", "Sin clasificar")
        parsed.setdefault("nivel_riesgo", "bajo")
        parsed.setdefault("motivo_principal", "")
        parsed.setdefault("resumen_ia", "")
        parsed.setdefault("razon_categoria", "")
        parsed.setdefault("paso1_comprension", "")
        parsed["confianza"] = confianza
        return parsed

    except Exception as e:
        print(f"   ⚠️  Error Claude para '{cliente}': {e}", flush=True)
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
    print(f"🚀 Análisis PRD v2 iniciado — {datetime.now().strftime('%Y-%m-%d %H:%M')}", flush=True)
    print(f"   Modelo: claude-sonnet-4-6  |  Confianza mínima: {CONFIDENCE_THRESHOLD:.0%}", flush=True)
    print(f"   HubSpot API: {'✅ configurado' if HUBSPOT_API_KEY else '⚠️  no configurado'}", flush=True)

    spreadsheet = conectar_sheets()
    ws_source   = spreadsheet.worksheet(SOURCE_TAB)
    df          = pd.DataFrame(ws_source.get_all_records())
    print(f"✅ {len(df)} filas cargadas de '{SOURCE_TAB}'", flush=True)

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

    print(f"   Transcript: {col_transcript}  |  Resumen: {col_resumen_exist}", flush=True)

    # Filter by target owners
    df_filtrado = df[df[col_dueno].isin(DUENOS_OBJETIVO)].copy() if col_dueno else df.copy()
    print(f"✅ {len(df_filtrado)} filas tras filtrar owners objetivo", flush=True)

    # Filtrar solo llamadas del mes actual
    if col_fecha:
        df_filtrado[col_fecha] = pd.to_datetime(df_filtrado[col_fecha], errors="coerce", dayfirst=True)
        hoy = datetime.now()
        inicio_mes = datetime(hoy.year, hoy.month, 1)
        df_filtrado = df_filtrado[df_filtrado[col_fecha] >= inicio_mes].copy()
        df_filtrado = df_filtrado.sort_values(by=col_fecha, ascending=False).copy()
        print(f"📅 Filtrado por mes actual desde {inicio_mes.strftime('%Y-%m-%d')} → {len(df_filtrado)} filas", flush=True)
    else:
        print("⚠️  No se encontró columna de fecha; no se aplica filtro de mes actual", flush=True)

    # Límite manual de filas para controlar gasto
    if MAX_FILAS_MES_ACTUAL is not None:
        df_filtrado = df_filtrado.head(MAX_FILAS_MES_ACTUAL).copy()
        print(f"🔒 Límite manual aplicado: {len(df_filtrado)} filas", flush=True)

    rows_to_analyze = df_filtrado
    skipped = 0

    if FORCE_REANALYZE:
        print("   🔄 FORCE_REANALYZE=True → se re-analizarán TODAS las filas", flush=True)
    elif col_cat_exist and col_conf_exist:
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
        print(f"   ⏭️  {skipped} filas ya clasificadas (confianza ≥ {CONFIDENCE_THRESHOLD:.0%}) → saltadas", flush=True)

    print(f"✅ {len(rows_to_analyze)} filas para analizar", flush=True)

    if rows_to_analyze.empty:
        print("✅ Sin filas nuevas para procesar.", flush=True)
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
        print(f"\n[{idx}/{total}] {dueno or 'N/A'} — {cliente or titulo or call_id}", flush=True)

        fuente = "transcript"
        if transcript:
            print(f"   📄 Fuente: transcript ({len(transcript)} chars)", flush=True)
        elif resumen_exist:
            transcript = resumen_exist
            fuente     = "resumen_existente"
            print(f"   📄 Fuente: resumen existente ({len(transcript)} chars) — sin transcript disponible", flush=True)
        else:
            print(f"   ⚠️  Sin transcript ni resumen — llamada ID: {call_id}", flush=True)

        analisis  = analizar_transcript(transcript, cliente, titulo, fuente)
        confianza = float(analisis.get("confianza", 0.0))
        subcats   = analisis.get("subcategorias", [])
        subcats_str = " | ".join(subcats) if subcats else ""

        print(
            f"   → Churn: {analisis['churn_detectado']} | Cat: {analisis['categoria']} | "
            f"Conf: {confianza:.0%} | Subs: {subcats_str or '–'}",
            flush=True
        )

        if analisis["categoria"] == "Sin clasificar":
            print(
                f"   ⚠️  Confianza < {CONFIDENCE_THRESHOLD:.0%} → marcado como 'Sin clasificar' para revisión manual",
                flush=True
            )

        hs = {}
        if HUBSPOT_API_KEY:
            hs = buscar_hubspot(cliente)
            if hs.get("sin_registro"):
                print(f"   🔍 HubSpot: Sin registro para '{cliente}'", flush=True)
            else:
                print(
                    f"   ✅ HubSpot: {hs.get('name','')} | Type: {hs.get('saas_client_type','')} | Reason: {hs.get('churn_reason','')}",
                    flush=True
                )
                if analisis["categoria"] == "Sin clasificar" and hs.get("churn_reason"):
                    analisis["categoria"] = hs["churn_reason"]
                    print(f"   → Categoría de HubSpot aplicada: {analisis['categoria']}", flush=True)

        fecha_dt = pd.to_datetime(fecha, errors="coerce", dayfirst=True)
        mes_valor = fecha_dt.strftime("%Y-%m") if pd.notna(fecha_dt) else ""

        resultados.append({
            "owner":             dueno,
            "cliente":           cliente,
            "fecha":             fecha,
            "mes":               mes_valor,
            "participantes":     participantes,
            "duracion":          duracion,
            "llamada_id":        call_id,
            "displayText":       analisis["resumen_ia"] or resumen_exist or transcript[:500] if transcript else "",
            "contactId":         "",
            "esChurn":           analisis["churn_detectado"],
            "categoria":         analisis["categoria"],
            "subcategorias":     [s for s in subcats if s],
            "confianza":         confianza,
            "nivel_riesgo":      analisis["nivel_riesgo"],
            "motivo_principal":  analisis["motivo_principal"],
            "resumen_ia":        analisis["resumen_ia"],
            "paso1_comprension": analisis.get("paso1_comprension", ""),
            "razon_categoria":   analisis.get("razon_categoria", ""),
            "hs_churn_reason":   hs.get("churn_reason_raw", ""),
            "hs_churn_date":     hs.get("churn_date", ""),
            "hs_url":            hs.get("hs_url", ""),
            "sin_registro_hs":   bool(hs.get("sin_registro")),
            "fuente_analisis":   fuente,
            "actualizado":       datetime.now().strftime("%Y-%m-%d %H:%M")
        })
        time.sleep(0.5)

    json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "modjo_data.json")
    existing_data = {}
    if not FORCE_REANALYZE and os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                existing_json = json.load(f)
            for rec in existing_json.get("data", []):
                key = rec.get("llamada_id", "")
                if key:
                    existing_data[key] = rec
            print(f"\n📂 {len(existing_data)} registros previos cargados de 'modjo_data.json'", flush=True)
        except Exception as e:
            print(f"\n⚠️  Error leyendo modjo_data.json: {e}", flush=True)

    for rec in resultados:
        key = rec.get("llamada_id", "")
        if key:
            existing_data[key] = rec

    all_records = list(existing_data.values()) if existing_data else resultados

    if all_records:
        df_all = pd.DataFrame(all_records)

        top_categorias = df_all["categoria"].value_counts().head(10).to_dict() if "categoria" in df_all.columns else {}

        all_subs = []
        if "subcategorias" in df_all.columns:
            for subs in df_all["subcategorias"]:
                if isinstance(subs, list):
                    all_subs.extend(subs)

        top_subcategorias = pd.Series(all_subs).value_counts().head(10).to_dict() if all_subs else {}

        resumen_json = {
            "churn_detectado": int(len(df_all[df_all["esChurn"] == "sí"])) if "esChurn" in df_all.columns else 0,
            "riesgo": int(len(df_all[df_all["esChurn"] == "riesgo"])) if "esChurn" in df_all.columns else 0,
            "sin_clasificar": int(len(df_all[df_all["categoria"] == "Sin clasificar"])) if "categoria" in df_all.columns else 0,
            "top_categorias": top_categorias,
            "top_subcategorias": top_subcategorias,
        }
    else:
        resumen_json = {
            "churn_detectado": 0,
            "riesgo": 0,
            "sin_clasificar": 0,
            "top_categorias": {},
            "top_subcategorias": {},
        }

    json_output = {
        "generated_at": datetime.now().isoformat(),
        "total": len(all_records),
        "resumen": resumen_json,
        "data": all_records
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_output, f, indent=2, ensure_ascii=False)

    print(
        f"\n✅ {len(all_records)} registros guardados en 'modjo_data.json' ({len(resultados)} nuevos/actualizados)",
        flush=True
    )

    if resultados:
        df_res = pd.DataFrame(resultados)
        n_churn      = len(df_res[df_res["esChurn"] == "sí"])
        n_riesgo     = len(df_res[df_res["esChurn"] == "riesgo"])
        n_sin_cls    = len(df_res[df_res["categoria"] == "Sin clasificar"])
        n_sin_hs     = len(df_res[df_res["sin_registro_hs"] == True])
        cats         = df_res["categoria"].value_counts().to_dict()

        print(f"\n📊 Resumen PRD v2:", flush=True)
        print(f"   Total procesadas : {len(resultados)}", flush=True)
        print(f"   Saltadas (ya OK) : {skipped}", flush=True)
        print(f"   Churn detectado  : {n_churn}", flush=True)
        print(f"   Riesgo           : {n_riesgo}", flush=True)
        print(f"   Sin clasificar   : {n_sin_cls}  ← revisión manual", flush=True)
        print(f"   Sin registro HS  : {n_sin_hs}", flush=True)
        print(f"\n   Categorías:", flush=True)
        for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
            print(f"   - {cat}: {cnt}", flush=True)

    print("\n✅ Análisis PRD v2 completado.", flush=True)
