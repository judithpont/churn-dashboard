import gspread
import pandas as pd
import json
import os
from google.oauth2.service_account import Credentials
import anthropic
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/16Tv2U164NammrDBe2u8b1_BuuHiWCkHaO1J5mFkl6OY"
RESULTS_TAB = "Resultados_Churn"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "sk-ant-...")

DUENOS_OBJETIVO = ["Victor Ortega", "Oscar Lopo", "Kamila", "Martina", "Gonzalo", "Franco"]

CATEGORIAS_CHURN = [
    "Fallos técnicos", "Onboarding deficiente", "Bajo valor percibido",
    "Contenido irrelevante", "Soporte deficiente", "Sorpresas en facturación",
    "Limitaciones del producto", "Razones externas"
]

# ============================================================
# CONEXIÓN
# ============================================================
def conectar():
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
# ANÁLISIS INDIVIDUAL CON CLAUDE
# ============================================================
def analizar_churn(resumen: str, cliente: str) -> dict:
    if not resumen or len(resumen.strip()) < 20:
        return {"churn_detectado": "sin datos", "categoria": "N/A",
                "nivel_riesgo": "N/A", "motivo_principal": "Sin resumen"}
    ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = f"""
Analiza este resumen de llamada del cliente "{cliente}":
1. ¿Hay señales de churn? → "sí", "no", o "riesgo"
2. Categoría principal (elige una): {", ".join(CATEGORIAS_CHURN)}
3. Nivel de riesgo: "alto", "medio" o "bajo"
4. Motivo principal en 1 frase en español

Resumen: {resumen[:3000]}

Responde SOLO en JSON: churn_detectado, categoria, nivel_riesgo, motivo_principal
"""
    msg = ai.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}]
    )
    texto = msg.content[0].text.strip()
    return json.loads(texto[texto.find("{"):texto.rfind("}")+1])

# ============================================================
# GENERAR INSIGHTS DETALLADOS POR OWNER
# ============================================================
def generar_insights(df_res: pd.DataFrame, todos_resumenes: list) -> dict:
    ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    insights = {}

    # Insights globales
    total = len(df_res)
    churns = len(df_res[df_res["Churn detectado"] == "sí"])
    riesgo_alto = len(df_res[df_res["Nivel de riesgo"] == "alto"])
    cats = df_res["Categoría de churn"].value_counts().to_dict()
    por_dueno = df_res[df_res["Churn detectado"].isin(["sí", "riesgo"])].groupby("Dueño").size().to_dict()
    motivos = [r["Motivo principal"] for r in todos_resumenes if r["Churn detectado"] in ["sí", "riesgo"]][:20]

    prompt_global = f"""
Eres un analista de Revenue Operations. Analiza estos datos de llamadas con clientes y genera un informe ejecutivo detallado en español con estas secciones exactas:

DATOS:
- Total llamadas: {total}
- Churn confirmado: {churns} ({round(churns/total*100) if total else 0}%)
- Riesgo alto: {riesgo_alto}
- Categorías de churn: {cats}
- Owners con más churn/riesgo: {por_dueno}
- Motivos detectados: {motivos}

Genera el informe con estas secciones en formato JSON:
{{
  "resumen_ejecutivo": "2-3 frases resumiendo la situación global",
  "categorias": [
    {{"nombre": "nombre categoría", "frecuencia": "muy alta/alta/media/baja", "descripcion": "qué está pasando en 2-3 frases", "clientes_afectados": "lista de clientes mencionados"}}
  ],
  "patron_critico": "descripción del ciclo de deterioro detectado en 2-3 frases",
  "recomendaciones_corto": "2-3 acciones inmediatas (0-30 días) en texto corrido",
  "recomendaciones_medio": "2-3 acciones a medio plazo (30-90 días) en texto corrido",
  "recomendaciones_estrategico": "1-2 reflexiones estratégicas en texto corrido",
  "fecha": "{datetime.now().strftime('%Y-%m-%d %H:%M')}"
}}
"""
    msg = ai.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt_global}]
    )
    texto = msg.content[0].text.strip()
    insights["global"] = json.loads(texto[texto.find("{"):texto.rfind("}")+1])

    # Insights por owner
    for dueno in DUENOS_OBJETIVO:
        df_d = df_res[df_res["Dueño"].str.contains(dueno, case=False, na=False)]
        if df_d.empty:
            continue
        total_d = len(df_d)
        churns_d = len(df_d[df_d["Churn detectado"] == "sí"])
        riesgo_d = len(df_d[df_d["Churn detectado"] == "riesgo"])
        cats_d = df_d["Categoría de churn"].value_counts().head(3).to_dict()
        motivos_d = df_d["Motivo principal"].tolist()

        prompt_owner = f"""
Analiza los datos de llamadas del owner "{dueno}" y genera un resumen ejecutivo en JSON:
- Total llamadas: {total_d}
- Churn confirmado: {churns_d}
- En riesgo: {riesgo_d}
- Top categorías: {cats_d}
- Motivos: {motivos_d}

Responde en JSON:
{{
  "resumen": "2 frases sobre la situación de este owner",
  "principal_problema": "el problema más crítico que tiene este owner",
  "accion_inmediata": "qué debería hacer este owner esta semana"
}}
"""
        msg_d = ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt_owner}]
        )
        texto_d = msg_d.content[0].text.strip()
        insights[dueno] = json.loads(texto_d[texto_d.find("{"):texto_d.rfind("}")+1])
        print(f"   📝 Insights generados para {dueno}")

    return insights

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print(f"🚀 Análisis iniciado — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    spreadsheet = conectar()
    df = pd.DataFrame(spreadsheet.sheet1.get_all_records())
    print(f"✅ {len(df)} filas cargadas")

    resultados = []
    for dueno in DUENOS_OBJETIVO:
        df_d = df[df["Dueño"].str.contains(dueno, case=False, na=False)]
        if df_d.empty:
            print(f"⚠️  Sin datos: {dueno}")
            continue
        print(f"\n🔍 {dueno} ({len(df_d)} llamadas)...")
        for _, row in df_d.iterrows():
            analisis = analizar_churn(str(row.get("Resumen IA", "")), str(row.get("Participante", "")))
            resultados.append({
                "Dueño": dueno,
                "Fecha": row.get("Fecha", ""),
                "Cliente": row.get("Participante", ""),
                "Churn detectado": analisis.get("churn_detectado", ""),
                "Categoría de churn": analisis.get("categoria", ""),
                "Nivel de riesgo": analisis.get("nivel_riesgo", ""),
                "Motivo principal": analisis.get("motivo_principal", ""),
                "Actualizado": datetime.now().strftime("%Y-%m-%d %H:%M")
            })
            print(f"   ✓ {row.get('Participante','')} → {analisis.get('churn_detectado','')} | {analisis.get('nivel_riesgo','')}")

    # Guardar resultados individuales
    try:
        ws = spreadsheet.worksheet(RESULTS_TAB)
        ws.clear()
    except:
        ws = spreadsheet.add_worksheet(title=RESULTS_TAB, rows=1000, cols=10)

    if resultados:
        df_res = pd.DataFrame(resultados)
        ws.update([df_res.columns.tolist()] + df_res.values.tolist())
        print(f"\n✅ {len(resultados)} resultados en '{RESULTS_TAB}'")

        # Generar insights detallados
        print("\n🧠 Generando insights con Claude...")
        insights = generar_insights(df_res, resultados)

        # Guardar insights en pestaña
        try:
            ws_ins = spreadsheet.worksheet("Insights_Churn")
            ws_ins.clear()
        except:
            ws_ins = spreadsheet.add_worksheet(title="Insights_Churn", rows=100, cols=3)

        filas_insights = [["Seccion", "Owner", "Contenido"]]
        g = insights.get("global", {})
        filas_insights.append(["resumen_ejecutivo", "global", g.get("resumen_ejecutivo", "")])
        filas_insights.append(["patron_critico", "global", g.get("patron_critico", "")])
        filas_insights.append(["recomendaciones_corto", "global", g.get("recomendaciones_corto", "")])
        filas_insights.append(["recomendaciones_medio", "global", g.get("recomendaciones_medio", "")])
        filas_insights.append(["recomendaciones_estrategico", "global", g.get("recomendaciones_estrategico", "")])
        filas_insights.append(["fecha", "global", g.get("fecha", "")])

        for cat in g.get("categorias", []):
            filas_insights.append(["categoria", cat.get("nombre",""), json.dumps(cat, ensure_ascii=False)])

        for dueno in DUENOS_OBJETIVO:
            if dueno in insights:
                d = insights[dueno]
                filas_insights.append(["owner_resumen", dueno, d.get("resumen", "")])
                filas_insights.append(["owner_problema", dueno, d.get("principal_problema", "")])
                filas_insights.append(["owner_accion", dueno, d.get("accion_inmediata", "")])

        ws_ins.update(filas_insights)
        print(f"📊 Insights guardados en 'Insights_Churn'")

    print("\n✅ Análisis completado.")
