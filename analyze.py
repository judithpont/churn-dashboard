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
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "sk-ant-...")  # usa variable de entorno

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
    # En GitHub Actions usa variable de entorno GOOGLE_CREDENTIALS
    if os.environ.get("GOOGLE_CREDENTIALS"):
        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)

    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(SHEET_URL)
    return spreadsheet

# ============================================================
# ANÁLISIS CON CLAUDE
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

    # Escribir resultados en pestaña Resultados_Churn
    try:
        ws = spreadsheet.worksheet(RESULTS_TAB)
        ws.clear()
    except:
        ws = spreadsheet.add_worksheet(title=RESULTS_TAB, rows=1000, cols=10)

    if resultados:
        df_res = pd.DataFrame(resultados)
        ws.update([df_res.columns.tolist()] + df_res.values.tolist())
        print(f"\n✅ {len(resultados)} resultados escritos en pestaña '{RESULTS_TAB}'")

    # ============================================================
    # GENERAR RESUMEN EJECUTIVO CON CLAUDE
    # ============================================================
    if resultados:
        df_res = pd.DataFrame(resultados)
        total = len(df_res)
        churns = len(df_res[df_res["Churn detectado"] == "sí"])
        riesgo_alto = len(df_res[df_res["Nivel de riesgo"] == "alto"])
        cats = df_res["Categoría de churn"].value_counts().head(3).to_dict()
        por_dueno = df_res[df_res["Churn detectado"] == "sí"].groupby("Dueño").size().to_dict()

        ai = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        prompt_resumen = f"""
Eres un analista de Revenue Operations. Con estos datos de llamadas con clientes, escribe un resumen ejecutivo en español de máximo 4 párrafos cortos explicando:
1. Por qué estamos perdiendo clientes (motivos principales)
2. Qué owners tienen más casos críticos
3. Qué deberíamos hacer de forma inmediata para reducir el churn

Datos del análisis:
- Total llamadas analizadas: {total}
- Clientes con churn confirmado: {churns} ({round(churns/total*100) if total else 0}%)
- Clientes con riesgo alto: {riesgo_alto}
- Top 3 categorías de churn: {cats}
- Churn confirmado por owner: {por_dueno}
- Motivos individuales: {[r['Motivo principal'] for r in resultados if r['Churn detectado'] == 'sí'][:15]}

Escribe el resumen de forma directa, sin bullets, como un análisis ejecutivo. Sé específico con los datos.
"""
        msg_resumen = ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt_resumen}]
        )
        resumen_ejecutivo = msg_resumen.content[0].text.strip()

        # Guardar resumen en pestaña separada
        try:
            ws_resumen = spreadsheet.worksheet("Resumen_Ejecutivo")
            ws_resumen.clear()
        except:
            ws_resumen = spreadsheet.add_worksheet(title="Resumen_Ejecutivo", rows=20, cols=2)

        ws_resumen.update("A1", [
            ["Actualizado", datetime.now().strftime("%Y-%m-%d %H:%M")],
            ["Resumen", resumen_ejecutivo]
        ])
        print(f"\n📝 Resumen ejecutivo generado y guardado en 'Resumen_Ejecutivo'")

    print("✅ Análisis completado.")
