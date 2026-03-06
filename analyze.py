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
SOURCE_TAB      = "Resultados_Churn"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HUBSPOT_API_KEY   = os.environ.get("HUBSPOT_API_KEY", "")
CONFIDENCE_THRESHOLD = 0.65

DUENOS_OBJETIVO = [
    "Victor Ortega", "Óscar Lopo", "Martina Benalcazar",
    "Kamila Jiménez", "Gonzalo Rosales", "Franco Ferretti"
]

# Unified categories — consistent with index.html CAT_KW
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
    "Sin categoría"
]

# Subcategories per category — consistent with index.html SUB_CAT_KW
SUBCATEGORIAS = {
    "Fallo en la plataforma": [
        "Error conexión Instagram", "Error conexión Facebook",
        "Problemas dominio/hosting/DNS", "Errores de plugins",
        "Configuración email fallida", "Google Maps/Business",
        "Errores en la app / verificación", "Sistema reservas/calendario"
    ],
    "Problemas de calidad": [
        "Contenido genérico / no personalizado", "Publicaciones con errores",
        "Respuestas a reseñas automáticas", "Imágenes IA no representan el negocio",
        "Calidad baja de imágenes", "Diseño web insatisfactorio",
        "Menú/carta desactualizada", "Frecuencia de publicaciones insuficiente"
    ],
    "Sin impacto real en su negocio": [
        "No incrementa reservas/pedidos", "Sin mejora en SEO/Google",
        "Sin aumento de visibilidad/seguidores", "No genera ventas ni clientes",
        "Sin tráfico web"
    ],
    "Fallo en el onboarding": [
        "No entiende la plataforma", "Especificaciones incompletas",
        "Retraso en activación", "Expectativas mal alineadas",
        "Primera llamada deficiente"
    ],
    "Problema de soporte": [
        "Falta de seguimiento proactivo", "Tiempos de respuesta lentos",
        "Sin respuesta del equipo"
    ],
    "Fuera del alcance del plan": [
        "Campañas publicitarias no incluidas", "Mantenimiento web no incluido",
        "Más frecuencia de la incluida", "Contenido en idiomas no cubiertos",
        "Funcionalidad no soportada"
    ],
    "No justifica precio": [
        "Limitaciones de presupuesto", "Relación calidad-precio insuficiente",
        "Dudas sobre renovación/permanencia", "Errores de cobro/facturación",
        "Competencia ofrece mejor precio", "Percepción de precio alto"
    ],
    "Cierre de venta forzado": [
        "Prueba gratuita no explicada", "Condiciones de contrato no claras",
        "Cargo sin consentimiento"
    ],
    "Mala comunicación en ventas": [
        "Expectativas vs. servicio desalineadas", "Promesas no cumplidas",
        "Confusión sobre frecuencia"
    ],
    "Promesa irreal": [
        "Resultados garantizados", "Plazos irreales"
    ],
    "Negocio cerrado": [
        "Cierre definitivo", "Cambio de actividad"
    ]
}

# HubSpot reason → Modjo category (unified mapping)
HS_TRADUCCIONES = {
    "Sales Miscommunication":                 "Mala comunicación en ventas",
    "Poor CX / Platform Issues":              "Fallo en la plataforma",
    "Quality Issues":                         "Problemas de calidad",
    "Support or Service Issues":              "Problema de soporte",
    "Onboarding Failure":                     "Fallo en el onboarding",
    "Does not see impact on their business":  "Sin impacto real en su negocio",
    "WTP < Cost":                             "No justifica precio",
    "Outside The Scope of the Plan":          "Fuera del alcance del plan",
    "Unrealistic promise":                    "Promesa irreal",
    "Forced Sales Closing":                   "Cierre de venta forzado",
    "Business Closed":                        "Negocio cerrado",
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
# HUBSPOT — buscar motivo de churn
# ============================================================
def buscar_motivo_hubspot(cliente: str) -> dict:
    if not HUBSPOT_API_KEY or not cliente:
        return {}
    headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "filterGroups": [{"filters": [{"propertyName": "name", "operator": "CONTAINS_TOKEN", "value": cliente.strip().split()[0]}]}],
        "properties": ["name", "churn_status", "churn_reason_saas", "fecha_de_solicitud_de_baja"],
        "limit": 5
    }
    try:
        r = requests.post("https://api.hubapi.com/crm/v3/objects/companies/search", headers=headers, json=payload, timeout=10)
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
        churn_reason_raw = best.get("churn_reason_saas", "")
        churn_reason_es = HS_TRADUCCIONES.get(churn_reason_raw, churn_reason_raw)
        return {
            "churn_status": best.get("churn_status", ""),
            "churn_reason": churn_reason_es,
            "fecha_baja": best.get("fecha_de_solicitud_de_baja", "")
        }
    except Exception as e:
        print(f"   ⚠️  Error HubSpot para '{cliente}': {e}")
        return {}


# ============================================================
# ANÁLISIS CON CLAUDE — categoría + subcategoría en un solo prompt
# ============================================================
def analizar_transcript(transcript: str, cliente: str, titulo: str) -> dict:
    if not transcript or len(transcript.strip()) < 50:
        return {
            "churn_detectado": "sin datos", "categoria": "Sin categoría",
            "subcategoria": "", "nivel_riesgo": "bajo",
            "motivo_principal": "Sin transcripción disponible",
            "resumen_ia": "No se encontró transcripción para esta llamada.",
            "confianza": 0.0
        }

    ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    transcript_truncado = transcript[:8000]

    # Build subcategory list for the prompt
    subcats_text = "\n".join(
        f'  - {cat}: {", ".join(subs)}'
        for cat, subs in SUBCATEGORIAS.items()
    )
    cats_list = " | ".join(CATEGORIAS_CHURN)

    prompt = f"""Eres un analista de Revenue Operations especializado en churn de SaaS.

Analiza esta transcripción de llamada con el cliente "{cliente}" (título: "{titulo}").

TRANSCRIPCIÓN:
{transcript_truncado}

---

Extrae la siguiente información respondiendo ÚNICAMENTE en JSON válido:

{{
  "churn_detectado": "sí | no | riesgo",
  "categoria": "una de: {cats_list}",
  "subcategoria": "subcategoría específica dentro de la categoría elegida (ver lista abajo)",
  "nivel_riesgo": "alto | medio | bajo",
  "motivo_principal": "Frase concisa en español explicando el motivo principal",
  "resumen_ia": "Resumen ejecutivo de 2-3 frases en español",
  "confianza": 0.0
}}

Subcategorías válidas por categoría:
{subcats_text}

Criterios:
- "churn_detectado": "sí" si el cliente ya solicitó baja o claramente se va; "riesgo" si hay señales de insatisfacción sin confirmar; "no" si la llamada es normal.
- "categoria": razón principal del churn o riesgo. "Sin categoría" solo si no hay señal alguna.
- "subcategoria": la subcategoría más específica dentro de la categoría elegida. Deja vacío si no aplica.
- "confianza": entre 0 y 1. 0.9 si la causa es muy clara, 0.4 si es vaga, 0.1 si el transcript es vacío o ininteligible.
"""

    try:
        msg = ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )
        texto = msg.content[0].text.strip()
        parsed = json.loads(texto[texto.find("{"):texto.rfind("}")+1])
        parsed.setdefault("churn_detectado", "no")
        parsed.setdefault("categoria", "Sin categoría")
        parsed.setdefault("subcategoria", "")
        parsed.setdefault("nivel_riesgo", "bajo")
        parsed.setdefault("motivo_principal", "")
        parsed.setdefault("resumen_ia", "")
        parsed.setdefault("confianza", 0.5)
        return parsed
    except Exception as e:
        print(f"   ⚠️  Error Claude para '{cliente}': {e}")
        return {
            "churn_detectado": "sin datos", "categoria": "Sin categoría",
            "subcategoria": "", "nivel_riesgo": "bajo",
            "motivo_principal": "Error en análisis", "resumen_ia": "",
            "confianza": 0.0
        }


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print(f"🚀 Análisis iniciado — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"   HubSpot API: {'✅ configurado' if HUBSPOT_API_KEY else '⚠️  no configurado'}")

    spreadsheet = conectar_sheets()
    ws_source = spreadsheet.worksheet(SOURCE_TAB)
    df = pd.DataFrame(ws_source.get_all_records())
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

    # Check for existing analysis columns (skip already-analyzed rows)
    col_cat_exist     = find_col(df, "categoría", "categoria")
    col_conf_exist    = find_col(df, "confianza")

    print(f"   Transcript: {col_transcript}  |  Resumen: {col_resumen_exist}")

    # Filter by target owners
    if col_dueno:
        df_filtrado = df[df[col_dueno].isin(DUENOS_OBJETIVO)].copy()
    else:
        df_filtrado = df.copy()

    # Skip rows already analyzed with high confidence (optimization)
    rows_to_analyze = df_filtrado
    if col_cat_exist and col_conf_exist:
        already_done = df_filtrado[
            (df_filtrado[col_cat_exist].notna()) &
            (df_filtrado[col_cat_exist] != "") &
            (df_filtrado[col_cat_exist] != "Sin categoría") &
            (df_filtrado[col_conf_exist].astype(str).str.rstrip('%').apply(
                lambda x: float(x) if x.replace('.','',1).isdigit() else 0
            ) >= CONFIDENCE_THRESHOLD * 100)
        ]
        rows_to_analyze = df_filtrado.drop(already_done.index)
        print(f"   ⏭️  {len(already_done)} filas ya analizadas (confianza ≥ {CONFIDENCE_THRESHOLD:.0%}), saltando")

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
        if not transcript and resumen_exist:
            transcript = resumen_exist
            fuente = "resumen_existente"
        elif not transcript:
            print(f"   ⚠️  Sin transcript — llamada ID: {call_id}")

        # Analyze with Claude (category + subcategory in single call)
        analisis = analizar_transcript(transcript, cliente, titulo)
        confianza = float(analisis.get("confianza", 0.5))
        print(f"   → Churn: {analisis['churn_detectado']} | Cat: {analisis['categoria']} | Sub: {analisis.get('subcategoria','')} | Conf: {confianza:.0%}")

        # HubSpot fallback
        motivo_hubspot = fecha_baja_hs = churn_status_hs = ""
        if (confianza < CONFIDENCE_THRESHOLD or analisis["categoria"] == "Sin categoría") and HUBSPOT_API_KEY:
            print(f"   🔍 Confianza baja → consultando HubSpot...")
            hs = buscar_motivo_hubspot(cliente)
            if hs:
                if hs.get("churn_reason"):
                    analisis["categoria"] = hs["churn_reason"]
                if hs.get("churn_status") == "Churn":
                    analisis["churn_detectado"] = "sí"
                    analisis["nivel_riesgo"] = "alto"
                motivo_hubspot  = hs.get("churn_reason", "")
                fecha_baja_hs   = hs.get("fecha_baja", "")
                churn_status_hs = hs.get("churn_status", "")
                print(f"   ✅ HubSpot → {motivo_hubspot}")

        resultados.append({
            "Dueño":            dueno,
            "Fecha":            fecha,
            "Cliente":          cliente,
            "Participantes":    participantes,
            "Duración (min)":   duracion,
            "Llamada ID":       call_id,
            "Churn detectado":  analisis["churn_detectado"],
            "Categoría":        analisis["categoria"],
            "Subcategoría":     analisis.get("subcategoria", ""),
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

    # Save results
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

        n_churn = len(df_res[df_res["Churn detectado"] == "sí"])
        n_riesgo = len(df_res[df_res["Churn detectado"] == "riesgo"])
        n_hs = len(df_res[df_res["Motivo HubSpot"] != ""])
        n_sin_cat = len(df_res[df_res["Categoría"] == "Sin categoría"])
        cats = df_res["Categoría"].value_counts().to_dict()
        subcats = df_res["Subcategoría"].value_counts().to_dict()

        print(f"\n📊 Resumen:")
        print(f"   Total: {len(resultados)} | Churn: {n_churn} | Riesgo: {n_riesgo} | HS fallback: {n_hs} | Sin cat: {n_sin_cat}")
        print(f"\n   Categorías:")
        for cat, cnt in sorted(cats.items(), key=lambda x: -x[1]):
            print(f"   - {cat}: {cnt}")
        print(f"\n   Top subcategorías:")
        for sub, cnt in sorted(subcats.items(), key=lambda x: -x[1])[:10]:
            if sub:
                print(f"   - {sub}: {cnt}")

    print("\n✅ Análisis completado.")
