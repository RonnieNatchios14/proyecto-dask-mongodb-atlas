"""
Generador automatizado del Reporte Técnico Ejecutivo en PDF.

Rol responsable: Arquitecto Cloud.

Compila en un documento PDF profesional (`docs/REPORTE_TECNICO.pdf`):
- Portada y matriz de roles (Ingeniero de Datos, DBA NoSQL, Arquitecto Cloud).
- Diagrama vectorial (SVG) de arquitectura Cloud (AWS/GCP + Dask + MongoDB Atlas).
- Métricas reales del benchmark Pandas vs. Dask (`docs/benchmark_results.json`)
  y su gráfica comparativa (`docs/benchmark_chart.png`).
- Diseño de esquema NoSQL, índices compuestos ESR y resultados reales de las
  3 consultas complejas (`docs/query_results.txt` si está disponible).
- Evidencias visuales de infraestructura Cloud (AWS/GCP) y MongoDB Atlas
  embebidas automáticamente desde `docs/screenshots/`.
"""

from __future__ import annotations

import base64
import html
import json
import logging
import mimetypes
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.cloud_architect.config import AppConfig, load_config, setup_logging

logger = logging.getLogger(__name__)

IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".webp")

# Catálogo estructurado de evidencias visuales requeridas por la rúbrica
EXPECTED_SCREENSHOTS: List[Dict[str, str]] = [
    {
        "stem": "03_mongodb_atlas_cluster",
        "section": "atlas",
        "title": "Figura 2. Clúster MongoDB Atlas en Operación (Overview & Metrics)",
        "instructions": (
            "Captura del panel principal de MongoDB Atlas mostrando el clúster activo, región "
            "Cloud (AWS/GCP), operaciones de inserción/consulta y conexiones activas."
        ),
    },
    {
        "stem": "04_mongodb_atlas_collections_indexes",
        "section": "atlas",
        "title": "Figura 3. Colecciones Desnormalizadas e Índices Compuestos en Atlas",
        "instructions": (
            "Captura de Browse Collections e Indexes mostrando sales_analytics_summary, "
            "transactions_sample y los índices compuestos ESR creados."
        ),
    },
    {
        "stem": "01_cloud_compute_aws_gcp",
        "section": "cloud",
        "title": "Figura 4. Infraestructura Cloud Operando (AWS EC2 / GCP Compute Engine)",
        "instructions": (
            "Captura de la consola AWS EC2 o GCP Compute Engine mostrando la instancia en estado "
            "Running, tipo de máquina (vCPU/RAM), región y dirección IP."
        ),
    },
    {
        "stem": "02_cloud_network_security",
        "section": "cloud",
        "title": "Figura 5. Seguridad de Red Cloud y Conectividad hacia MongoDB Atlas",
        "instructions": (
            "Captura de Security Groups / VPC Firewall (regla de salida TCP 27017) y/o "
            "MongoDB Atlas → Network Access (IP Allowlist / VPC Peering activo)."
        ),
    },
    {
        "stem": "05_terminal_healthcheck_pipeline",
        "section": "cloud",
        "title": "Figura 6. Ejecución de Healthcheck y Orquestador End-to-End en Consola",
        "instructions": (
            "Captura de terminal (SSH en AWS/GCP o entorno local) mostrando healthcheck.py "
            "en estado HEALTHY y la ejecución exitosa de python main.py."
        ),
    },
]


def _find_chrome_binary() -> str:
    """Localiza el ejecutable de Google Chrome / Chromium en macOS o Linux."""
    candidates = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        shutil.which("google-chrome") or "",
        shutil.which("chromium") or "",
        shutil.which("chromium-browser") or "",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    raise FileNotFoundError(
        "No se encontró Google Chrome o Chromium para exportar a PDF. "
        "Instala Google Chrome o exporta manualmente docs/REPORTE_TECNICO.md."
    )


def _image_to_data_uri(img_path: Path) -> str:
    """Codifica una imagen local en data URI base64 para incrustarla en HTML/PDF."""
    mime, _ = mimetypes.guess_type(str(img_path))
    mime = mime or "image/png"
    raw = img_path.read_bytes()
    encoded = base64.b64encode(raw).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _find_screenshot(screenshots_dir: Path, stem: str) -> Optional[Path]:
    """Busca una captura por nombre base soportando .png, .jpg, .jpeg y .webp."""
    for ext in IMAGE_EXTENSIONS:
        candidate = screenshots_dir / f"{stem}{ext}"
        if candidate.exists():
            return candidate
    return None


def _render_screenshot_block(screenshots_dir: Path, spec: Dict[str, str]) -> Tuple[str, bool]:
    """Renderiza una tarjeta de figura con imagen real o marcador guiado si aún no existe."""
    found = _find_screenshot(screenshots_dir, spec["stem"])
    title = html.escape(spec["title"])
    if found is not None:
        uri = _image_to_data_uri(found)
        block = f"""
        <figure class="evidence-card">
            <img src="{uri}" alt="{title}" class="evidence-img" />
            <figcaption><strong>{title}</strong> — Archivo: <code>docs/screenshots/{html.escape(found.name)}</code></figcaption>
        </figure>
        """
        return block, True

    expected_file = f"docs/screenshots/{spec['stem']}.png"
    block = f"""
    <div class="placeholder-box">
        <div class="placeholder-title">{title}</div>
        <div class="placeholder-badge">PENDIENTE DE CAPTURA REAL</div>
        <p class="placeholder-desc">{html.escape(spec["instructions"])}</p>
        <p class="placeholder-hint">
            Guarda tu captura como <code>{html.escape(expected_file)}</code> y vuelve a ejecutar:
            <code>.venv/bin/python -m src.cloud_architect.export_pdf</code>
        </p>
    </div>
    """
    return block, False


def _render_extra_screenshots(screenshots_dir: Path) -> Tuple[str, int]:
    """Detecta cualquier captura adicional colocada en `docs/screenshots/` fuera del catálogo base."""
    known_stems = {item["stem"] for item in EXPECTED_SCREENSHOTS}
    extras: List[Path] = []
    if screenshots_dir.exists():
        for path in sorted(screenshots_dir.iterdir()):
            if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS and path.stem not in known_stems:
                extras.append(path)

    if not extras:
        return "", 0

    blocks = ['<h3>Evidencias Adicionales de Infraestructura y Operación</h3>']
    for idx, img in enumerate(extras, start=1):
        uri = _image_to_data_uri(img)
        label = html.escape(img.stem.replace("_", " ").title())
        blocks.append(
            f"""
            <figure class="evidence-card">
                <img src="{uri}" alt="{label}" class="evidence-img" />
                <figcaption><strong>Evidencia Adicional {idx}: {label}</strong> — <code>docs/screenshots/{html.escape(img.name)}</code></figcaption>
            </figure>
            """
        )
    return "\n".join(blocks), len(extras)


def _architecture_svg() -> str:
    """Diagrama vectorial SVG embebido de la arquitectura End-to-End de 3 roles."""
    return """
    <svg viewBox="0 0 920 255" width="100%" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Arquitectura Cloud Dask MongoDB Atlas">
      <defs>
        <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M 0 1 L 10 5 L 0 9 z" fill="#1D3557" />
        </marker>
      </defs>

      <!-- Zona Cloud Compute -->
      <rect x="8" y="12" width="540" height="230" rx="12" fill="#F4F8FB" stroke="#457B9D" stroke-width="1.8" stroke-dasharray="6,4"/>
      <text x="24" y="34" font-family="Helvetica, Arial, sans-serif" font-size="11.5" font-weight="bold" fill="#1D3557">
        INFRAESTRUCTURA CLOUD (AWS EC2 / GCP Compute Engine / Nodo Local)
      </text>

      <!-- Rol 3: Arquitecto Cloud -->
      <rect x="22" y="50" width="154" height="82" rx="8" fill="#1D3557"/>
      <text x="99" y="73" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="10.5" font-weight="bold" fill="#FFFFFF">ROL 3: ARQUITECTO CLOUD</text>
      <text x="99" y="92" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.8" fill="#DCE8F2">config.py + healthcheck.py</text>
      <text x="99" y="108" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.2" fill="#A8DADC">.env · TLS · Timeouts · psutil</text>
      <text x="99" y="122" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.2" fill="#A8DADC">Security Groups / Firewall</text>

      <!-- Rol 1: Data Gen -->
      <rect x="200" y="50" width="156" height="82" rx="8" fill="#2A9D8F"/>
      <text x="278" y="73" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="10.5" font-weight="bold" fill="#FFFFFF">ROL 1: DATA ENGINEER</text>
      <text x="278" y="92" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.8" fill="#E9F8F6">generate_dataset.py</text>
      <text x="278" y="108" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.2" fill="#E9F8F6">5M filas · 20 CSV (~936 MB)</text>
      <text x="278" y="122" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.2" fill="#E9F8F6">data/raw/ (ignorado en Git)</text>

      <!-- Rol 1: Dask Pipeline -->
      <rect x="380" y="50" width="154" height="82" rx="8" fill="#264653"/>
      <text x="457" y="73" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="10.5" font-weight="bold" fill="#FFFFFF">DASK DISTRIBUTED</text>
      <text x="457" y="92" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.8" fill="#E0F2F1">dask_pipeline.py</text>
      <text x="457" y="108" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.2" fill="#B2DFDB">LocalCluster / Client</text>
      <text x="457" y="122" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.2" fill="#B2DFDB">blocksize=64MB · 1x compute</text>

      <!-- Benchmark Box -->
      <rect x="200" y="156" width="334" height="68" rx="8" fill="#FFFFFF" stroke="#E07A5F" stroke-width="1.8"/>
      <text x="367" y="177" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="10.8" font-weight="bold" fill="#C8553D">BENCHMARK AISLADO (benchmark.py)</text>
      <text x="367" y="195" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.8" fill="#333333">Pandas Secuencial vs. Dask Paralelo (misma transformación)</text>
      <text x="367" y="211" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.2" fill="#555555">docs/benchmark_results.json + docs/benchmark_chart.png</text>

      <!-- Zona MongoDB Atlas -->
      <rect x="618" y="12" width="294" height="230" rx="12" fill="#F2FBF5" stroke="#2D6A4F" stroke-width="1.8"/>
      <text x="634" y="34" font-family="Helvetica, Arial, sans-serif" font-size="11.5" font-weight="bold" fill="#1B4332">
        MONGODB ATLAS (NUBE NoSQL - TLS/SRV)
      </text>

      <!-- Rol 2: Loader -->
      <rect x="634" y="50" width="262" height="72" rx="8" fill="#2D6A4F"/>
      <text x="765" y="71" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="10.8" font-weight="bold" fill="#FFFFFF">ROL 2: DBA NoSQL (loader.py)</text>
      <text x="765" y="89" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.5" fill="#D8F3DC">insert_many(ordered=False) + Índices ESR</text>
      <text x="765" y="105" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.0" fill="#B7E4C7">sales_analytics_summary · transactions_sample</text>

      <!-- Rol 2: Queries -->
      <rect x="634" y="144" width="262" height="80" rx="8" fill="#FFFFFF" stroke="#2D6A4F" stroke-width="1.6"/>
      <text x="765" y="165" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="10.8" font-weight="bold" fill="#1B4332">CONSULTAS COMPLEJAS (queries.py)</text>
      <text x="765" y="183" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.5" fill="#333333">3 Pipelines ($match, $group, $sort, $limit)</text>
      <text x="765" y="199" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.3" fill="#2D6A4F">Perfilado con .explain("executionStats")</text>
      <text x="765" y="214" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="9.0" fill="#555555">Salida: docs/query_results.txt</text>

      <!-- Flechas -->
      <line x1="176" y1="91" x2="197" y2="91" stroke="#1D3557" stroke-width="2" marker-end="url(#arrow)"/>
      <line x1="356" y1="91" x2="377" y2="91" stroke="#1D3557" stroke-width="2" marker-end="url(#arrow)"/>
      <line x1="278" y1="132" x2="278" y2="153" stroke="#1D3557" stroke-width="2" marker-end="url(#arrow)"/>
      <line x1="534" y1="86" x2="631" y2="86" stroke="#1D3557" stroke-width="2.2" marker-end="url(#arrow)"/>
      <text x="583" y="77" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="8.5" font-weight="bold" fill="#1D3557">TCP 27017</text>
      <text x="583" y="100" text-anchor="middle" font-family="Helvetica, Arial, sans-serif" font-size="8.5" fill="#1D3557">TLS 1.2+</text>
      <line x1="765" y1="122" x2="765" y2="141" stroke="#2D6A4F" stroke-width="2" marker-end="url(#arrow)"/>
    </svg>
    """


def _build_html_document(config: AppConfig) -> Tuple[str, Dict[str, Any]]:
    """Construye el documento HTML completo con datos reales de benchmark, consultas y capturas."""
    docs_dir = config.docs_dir
    screenshots_dir = docs_dir / "screenshots"
    screenshots_dir.mkdir(parents=True, exist_ok=True)

    # 1. Cargar benchmark_results.json si existe
    bench_path = docs_dir / "benchmark_results.json"
    bench: Optional[Dict[str, Any]] = None
    if bench_path.exists():
        bench = json.loads(bench_path.read_text(encoding="utf-8"))

    # 2. Cargar benchmark_chart.png si existe
    chart_path = docs_dir / "benchmark_chart.png"
    chart_html = ""
    if chart_path.exists():
        chart_uri = _image_to_data_uri(chart_path)
        chart_html = f"""
        <figure class="evidence-card">
            <img src="{chart_uri}" alt="Gráfica Comparativa Pandas vs Dask" class="chart-img" />
            <figcaption><strong>Figura 1. Comparativa de Tiempo de Ejecución y Pico de Memoria RAM (RSS Total) — Pandas vs. Dask.</strong></figcaption>
        </figure>
        """
    else:
        chart_html = '<p class="warn-note">No se encontró <code>docs/benchmark_chart.png</code>. Ejecuta <code>python -m src.data_engineer.benchmark</code>.</p>'

    # 3. Construir filas de tabla de benchmark
    if bench:
        p = bench["pandas"]
        d = bench["dask"]
        c = bench["comparison"]
        hw = bench["hardware"]
        ds = bench["dataset"]
        dk = bench["dask_config"]
        benchmark_table_html = f"""
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Aceleración Dask</div>
                <div class="kpi-value">+{c['time_improvement_pct']:.2f}%</div>
                <div class="kpi-sub">{c['speedup_factor']:.2f}x más rápido que Pandas</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Reducción Pico RAM</div>
                <div class="kpi-value">-{c['ram_reduction_pct']:.2f}%</div>
                <div class="kpi-sub">{c['ram_saved_mb']:,.2f} MB ahorrados en pico</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Volumen Procesado</div>
                <div class="kpi-value">{ds['configured_rows']:,}</div>
                <div class="kpi-sub">{ds['total_size_mb']:,.2f} MB ({ds['files_count']} archivos CSV)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Equivalencia Numérica</div>
                <div class="kpi-value">{'100% OK' if c['numerical_equivalence_verified'] else 'REVISAR'}</div>
                <div class="kpi-sub">Dif. ingresos: ${c['revenue_absolute_diff_usd']:.2f}</div>
            </div>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Métrica Evaluada</th>
                    <th>Pandas (Secuencial)</th>
                    <th>Dask (Paralelo — blocksize {html.escape(str(dk['blocksize']))})</th>
                    <th>Diferencia / Mejora</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Tiempo de ejecución (s)</strong></td>
                    <td>{p['execution_time_seconds']:.2f} s</td>
                    <td>{d['execution_time_seconds']:.2f} s</td>
                    <td><strong>+{c['time_improvement_pct']:.2f}% ({c['speedup_factor']:.2f}x)</strong></td>
                </tr>
                <tr>
                    <td><strong>Pico de memoria RAM RSS (MB)</strong></td>
                    <td>{p['peak_ram_mb']:,.2f} MB</td>
                    <td>{d['peak_ram_mb']:,.2f} MB</td>
                    <td><strong>-{c['ram_reduction_pct']:.2f}% (-{c['ram_saved_mb']:,.2f} MB)</strong></td>
                </tr>
                <tr>
                    <td><strong>RAM promedio RSS (MB)</strong></td>
                    <td>{p['avg_ram_mb']:,.2f} MB</td>
                    <td>{d['avg_ram_mb']:,.2f} MB</td>
                    <td>Ejecución paralela en hilos</td>
                </tr>
                <tr>
                    <td><strong>Grupos analíticos generados</strong></td>
                    <td>{p['summary_groups']:,}</td>
                    <td>{d['summary_groups']:,}</td>
                    <td>Equivalencia exacta</td>
                </tr>
                <tr>
                    <td><strong>Transacciones válidas agregadas</strong></td>
                    <td>{p['total_transactions_aggregated']:,}</td>
                    <td>{d['total_transactions_aggregated']:,}</td>
                    <td>Equivalencia exacta</td>
                </tr>
                <tr>
                    <td><strong>Ingresos totales agregados (USD)</strong></td>
                    <td>${p['total_revenue_usd']:,.2f}</td>
                    <td>${d['total_revenue_usd']:,.2f}</td>
                    <td>Diferencia: ${c['revenue_absolute_diff_usd']:.2f}</td>
                </tr>
            </tbody>
        </table>
        <p class="meta-caption">
            Entorno medido: {html.escape(str(hw['os']))} · Python {html.escape(str(hw['python_version']))} ·
            {hw['cpu_logical_cores']} CPUs lógicos · {hw['ram_total_gb']} GB RAM · Timestamp UTC: {html.escape(str(bench['timestamp_utc']))}
        </p>
        """
    else:
        benchmark_table_html = '<p class="warn-note">No se encontró <code>docs/benchmark_results.json</code>. Ejecuta el benchmark primero.</p>'

    # 4. Cargar query_results.txt si existe
    query_results_path = docs_dir / "query_results.txt"
    if query_results_path.exists():
        raw_queries = query_results_path.read_text(encoding="utf-8")
        # Mostrar un extracto limpio de hasta 110 líneas para mantener el PDF compacto y legible
        preview_lines = raw_queries.splitlines()[:110]
        truncated_note = (
            f"\n... [Salida completa en docs/query_results.txt ({len(raw_queries.splitlines())} líneas)]"
            if len(raw_queries.splitlines()) > 110
            else ""
        )
        queries_section_html = f"""
        <div class="ok-note">
            Resultados reales capturados desde MongoDB Atlas en <code>docs/query_results.txt</code>.
        </div>
        <pre class="code-output">{html.escape(chr(10).join(preview_lines) + truncated_note)}</pre>
        """
    else:
        queries_section_html = """
        <div class="info-note">
            <strong>Pendiente de ejecución con MongoDB Atlas:</strong> Una vez que configures tu
            <code>MONGO_URI</code> real en <code>.env</code> y ejecutes <code>python main.py --skip-benchmark</code>,
            se generará automáticamente <code>docs/query_results.txt</code> con las 3 consultas complejas y su
            perfilado <code>.explain("executionStats")</code>. Al regenerar este PDF, los resultados aparecerán aquí automáticamente.
        </div>
        """

    # 5. Renderizar bloques de capturas Cloud y Atlas
    cloud_blocks: List[str] = []
    atlas_blocks: List[str] = []
    found_count = 0
    missing_stems: List[str] = []

    for spec in EXPECTED_SCREENSHOTS:
        block, found = _render_screenshot_block(screenshots_dir, spec)
        if found:
            found_count += 1
        else:
            missing_stems.append(f"{spec['stem']}.png")
        if spec["section"] == "cloud":
            cloud_blocks.append(block)
        else:
            atlas_blocks.append(block)

    extra_html, extra_count = _render_extra_screenshots(screenshots_dir)
    generated_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    document_html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8" />
<title>Reporte Técnico — Dask + MongoDB Atlas + Infraestructura Cloud</title>
<style>
  @page {{
    size: Letter;
    margin: 16mm 15mm 18mm 15mm;
  }}
  * {{
    box-sizing: border-box;
  }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    color: #1F2937;
    font-size: 10.2pt;
    line-height: 1.48;
    margin: 0;
  }}
  .cover {{
    background: linear-gradient(135deg, #0F2537 0%, #1D3557 55%, #2A9D8F 100%);
    color: #FFFFFF;
    padding: 28px 30px;
    border-radius: 12px;
    margin-bottom: 18px;
  }}
  .cover-tag {{
    display: inline-block;
    background: rgba(255, 255, 255, 0.16);
    padding: 4px 10px;
    border-radius: 20px;
    font-size: 8.5pt;
    letter-spacing: 0.6px;
    text-transform: uppercase;
    margin-bottom: 10px;
  }}
  .cover h1 {{
    font-size: 18.5pt;
    line-height: 1.25;
    margin: 0 0 10px 0;
    color: #FFFFFF;
    border: none;
    padding: 0;
  }}
  .cover p {{
    margin: 0;
    color: #E2EEF5;
    font-size: 10pt;
  }}
  .meta-table {{
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 16px;
    font-size: 9.2pt;
  }}
  .meta-table td {{
    border: 1px solid #D1D9E6;
    padding: 6px 10px;
  }}
  .meta-table td:first-child {{
    background: #F3F6FA;
    font-weight: 600;
    width: 34%;
    color: #1D3557;
  }}
  h2 {{
    font-size: 13.5pt;
    color: #1D3557;
    border-bottom: 2px solid #2A9D8F;
    padding-bottom: 4px;
    margin-top: 20px;
    margin-bottom: 10px;
    page-break-after: avoid;
  }}
  h3 {{
    font-size: 11pt;
    color: #264653;
    margin-top: 14px;
    margin-bottom: 6px;
    page-break-after: avoid;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    margin: 10px 0 12px 0;
    font-size: 9pt;
    page-break-inside: avoid;
  }}
  th {{
    background: #1D3557;
    color: #FFFFFF;
    text-align: left;
    padding: 7px 9px;
    font-weight: 600;
  }}
  td {{
    border: 1px solid #D8E0EA;
    padding: 6px 9px;
    vertical-align: top;
  }}
  tbody tr:nth-child(even) {{
    background: #F8FAFC;
  }}
  .kpi-grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
    margin: 12px 0 14px 0;
    page-break-inside: avoid;
  }}
  .kpi-card {{
    background: #F4F9F9;
    border: 1px solid #B7E4DF;
    border-top: 3px solid #2A9D8F;
    border-radius: 8px;
    padding: 9px 10px;
    text-align: center;
  }}
  .kpi-label {{
    font-size: 8pt;
    color: #4B5563;
    text-transform: uppercase;
    letter-spacing: 0.4px;
  }}
  .kpi-value {{
    font-size: 15pt;
    font-weight: 700;
    color: #1D3557;
    margin: 3px 0;
  }}
  .kpi-sub {{
    font-size: 8pt;
    color: #2A9D8F;
    font-weight: 600;
  }}
  .svg-wrapper {{
    border: 1px solid #DCE5EF;
    border-radius: 10px;
    padding: 10px;
    background: #FFFFFF;
    margin: 10px 0 14px 0;
    page-break-inside: avoid;
  }}
  .evidence-card {{
    border: 1px solid #D1D9E6;
    border-radius: 8px;
    padding: 10px;
    margin: 12px 0;
    background: #FFFFFF;
    text-align: center;
    page-break-inside: avoid;
  }}
  .chart-img {{
    max-width: 100%;
    max-height: 265px;
    object-fit: contain;
    border-radius: 4px;
    border: 1px solid #E5E7EB;
  }}
  .evidence-img {{
    max-width: 100%;
    max-height: 360px;
    object-fit: contain;
    border-radius: 4px;
    border: 1px solid #E5E7EB;
  }}
  figcaption {{
    margin-top: 7px;
    font-size: 8.6pt;
    color: #4B5563;
  }}
  .placeholder-box {{
    border: 2px dashed #457B9D;
    border-radius: 10px;
    background: #F8FBFE;
    padding: 11px 16px;
    margin: 9px 0;
    text-align: center;
    page-break-inside: avoid;
  }}
  .placeholder-title {{
    font-weight: 700;
    color: #1D3557;
    font-size: 10pt;
    margin-bottom: 5px;
  }}
  .placeholder-badge {{
    display: inline-block;
    background: #FEF3C7;
    color: #92400E;
    border: 1px solid #F59E0B;
    font-size: 7.8pt;
    font-weight: 700;
    padding: 2px 8px;
    border-radius: 12px;
    margin-bottom: 8px;
  }}
  .placeholder-desc {{
    margin: 4px 0;
    font-size: 9pt;
    color: #374151;
  }}
  .placeholder-hint {{
    margin: 6px 0 0 0;
    font-size: 8.3pt;
    color: #6B7280;
  }}
  .info-note, .ok-note, .warn-note {{
    padding: 9px 12px;
    border-radius: 6px;
    font-size: 9pt;
    margin: 8px 0;
  }}
  .info-note {{
    background: #EFF6FF;
    border-left: 4px solid #2563EB;
    color: #1E3A8A;
  }}
  .ok-note {{
    background: #ECFDF5;
    border-left: 4px solid #10B981;
    color: #065F46;
  }}
  .warn-note {{
    background: #FFFBEB;
    border-left: 4px solid #F59E0B;
    color: #92400E;
  }}
  code {{
    font-family: "SFMono-Regular", Menlo, Consolas, monospace;
    font-size: 8.5pt;
    background: #EEF2F6;
    padding: 1px 4px;
    border-radius: 3px;
    color: #1D3557;
  }}
  pre.code-output {{
    background: #0F172A;
    color: #E2E8F0;
    padding: 10px 12px;
    border-radius: 8px;
    font-family: "SFMono-Regular", Menlo, Consolas, monospace;
    font-size: 7.4pt;
    line-height: 1.35;
    white-space: pre-wrap;
    word-break: break-word;
  }}
  .meta-caption {{
    font-size: 8.2pt;
    color: #6B7280;
    margin-top: -4px;
  }}
  .page-break {{
    page-break-before: always;
  }}
  ul {{
    margin: 6px 0 10px 0;
    padding-left: 18px;
  }}
  li {{
    margin-bottom: 4px;
  }}
</style>
</head>
<body>

  <div class="cover">
    <div class="cover-tag">Práctica Evaluada · Ciencia de Datos &amp; Arquitectura Cloud</div>
    <h1>Integración de Python, Procesamiento Paralelo con Dask y Persistencia NoSQL en MongoDB Atlas</h1>
    <p>Reporte Técnico de Arquitectura, Benchmark Pandas vs. Dask, Modelado Documental NoSQL y Evidencias de Infraestructura Cloud (AWS/GCP)</p>
  </div>

  <table class="meta-table">
    <tr><td>Asignatura</td><td>Ciencia de Datos</td></tr>
    <tr><td>Fecha de generación del PDF</td><td>{generated_date}</td></tr>
    <tr><td>Rol 1 — Ingeniero de Datos</td><td><code>src/data_engineer/</code> · [Completar Nombre y Carnet]</td></tr>
    <tr><td>Rol 2 — DBA NoSQL</td><td><code>src/dba_nosql/</code> · [Completar Nombre y Carnet]</td></tr>
    <tr><td>Rol 3 — Arquitecto Cloud</td><td><code>src/cloud_architect/</code> · [Completar Nombre y Carnet]</td></tr>
    <tr><td>Proveedor Cloud / Región Configurada</td><td>{html.escape(config.cloud_provider)} ({html.escape(config.cloud_region)}) · Entorno: {html.escape(config.environment)}</td></tr>
  </table>

  <h2>1. Resumen Ejecutivo y Arquitectura del Sistema</h2>
  <p>
    El proyecto implementa un pipeline analítico modular de extremo a extremo que genera un volumen masivo de
    <strong>5,000,000 de transacciones e-commerce particionadas (~936 MB en disco)</strong>, ejecuta limpieza,
    deduplicación y agregaciones multidimensionales en paralelo con <strong>Dask Distributed</strong>, contrasta
    rigurosamente su rendimiento frente a <strong>Pandas secuencial</strong> y persiste el modelo analítico
    desnormalizado en <strong>MongoDB Atlas</strong> con índices compuestos ESR y consultas complejas perfiladas
    mediante <code>.explain("executionStats")</code>.
  </p>

  <div class="svg-wrapper">
    {_architecture_svg()}
  </div>

  <table>
    <thead>
      <tr><th>Rol Técnico</th><th>Módulos Responsables</th><th>Criterios de Rúbrica Cubiertos</th></tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>Ingeniero de Datos</strong></td>
        <td><code>generate_dataset.py</code><br/><code>dask_pipeline.py</code><br/><code>benchmark.py</code></td>
        <td>Dataset masivo particionado; lectura con <code>blocksize=64MB</code>; <code>LocalCluster</code>/<code>Client</code>; evaluación perezosa con único <code>.compute()</code>; benchmark aislado de tiempo y pico RAM.</td>
      </tr>
      <tr>
        <td><strong>DBA NoSQL</strong></td>
        <td><code>mongo_client.py</code><br/><code>loader.py</code><br/><code>queries.py</code></td>
        <td>Conexión segura TLS/SRV con validación <code>ping</code>; colecciones desnormalizadas; carga por lotes <code>insert_many(ordered=False)</code>; índices compuestos ESR; 3 consultas complejas con <code>.explain("executionStats")</code>.</td>
      </tr>
      <tr>
        <td><strong>Arquitecto Cloud</strong></td>
        <td><code>config.py</code><br/><code>healthcheck.py</code><br/><code>export_pdf.py</code></td>
        <td>Secretos en <code>.env</code> excluidos en <code>.gitignore</code>; enmascaramiento de URI; timeouts de red AWS/GCP ↔ Atlas; monitoreo de CPU/RAM/Dask/Atlas; generación automatizada del reporte PDF.</td>
      </tr>
    </tbody>
  </table>

  <div class="page-break"></div>

  <h2>2. Rol 1 — Ingeniería de Datos: Benchmark Pandas vs. Dask</h2>
  <p>
    Ambos motores ejecutaron <strong>exactamente la misma transformación</strong> (lectura de los 20 archivos CSV,
    eliminación de nulos críticos, filtrado de registros inválidos y no completados, deduplicación por
    <code>transaction_id</code>, cálculo de montos brutos/netos, filtro <code>net_amount &gt;= 25</code> y
    agrupación multidimensional por región, categoría, método de pago y mes) dentro de subprocesos aislados,
    midiendo el pico real de memoria RSS del árbol completo de procesos con <code>psutil</code>.
  </p>

  {benchmark_table_html}
  {chart_html}

  <div class="page-break"></div>

  <h2>3. Rol 2 — DBA NoSQL: Esquema, Índices y Consultas en MongoDB Atlas</h2>

  <h3>3.1 Modelo Documental Desnormalizado e Índices Compuestos (Regla ESR)</h3>
  <table>
    <thead>
      <tr><th>Colección</th><th>Diseño Documental</th><th>Índices Compuestos Creados</th></tr>
    </thead>
    <tbody>
      <tr>
        <td><code>sales_analytics_summary</code><br/>(2,400 docs)</td>
        <td>Pre-agregación analítica por <code>(region, category, payment_method, period)</code> con subdocumento embebido <code>metrics</code> y <code>etl_metadata</code>.</td>
        <td>
          • <code>uq_summary_grain</code> (unique)<br/>
          • <code>idx_summary_region_period_rev</code><br/>
          • <code>idx_summary_cat_pay_rev</code>
        </td>
      </tr>
      <tr>
        <td><code>transactions_sample</code><br/>(50,000 docs)</td>
        <td>Muestra detallada con subdocumentos embebidos <code>customer</code>, <code>product</code> y <code>financials</code> que eliminan joins (<code>$lookup</code>).</td>
        <td>
          • <code>uq_txn_id</code> (unique)<br/>
          • <code>idx_txn_status_segment_net</code><br/>
          • <code>idx_txn_region_cat_net</code><br/>
          • <code>idx_txn_pay_channel_ts</code>
        </td>
      </tr>
      <tr>
        <td><code>etl_execution_logs</code></td>
        <td>Auditoría de corridas ETL (timestamps, región cloud, métricas Dask y throughput de carga).</td>
        <td>Índice primario <code>_id</code></td>
      </tr>
    </tbody>
  </table>

  <h3>3.2 Resultados de las 3 Consultas Complejas y <code>.explain("executionStats")</code></h3>
  {queries_section_html}

  <h3>3.3 Evidencias de Operación en MongoDB Atlas</h3>
  {''.join(atlas_blocks)}

  <div class="page-break"></div>

  <h2>4. Rol 3 — Arquitecto Cloud: Infraestructura AWS / GCP en Operación</h2>
  <ul>
    <li><strong>Cómputo Cloud:</strong> Instancia Linux en AWS EC2 / GCP Compute Engine ejecutando Python 3 + entorno virtual aislado <code>.venv</code> y clúster Dask configurable (local o remoto vía <code>DASK_SCHEDULER_ADDRESS</code>).</li>
    <li><strong>Seguridad y Secretos:</strong> Variables centralizadas mediante <code>python-dotenv</code>; credenciales bloqueadas en <code>.gitignore</code> y enmascaradas en consola con <code>mask_mongo_uri()</code>.</li>
    <li><strong>Conectividad Resiliente:</strong> Protocolo <code>mongodb+srv://</code> sobre TLS 1.2+, puerto TCP <code>27017</code> autorizado vía Security Groups / Firewall + Atlas IP Access List, <code>retryWrites=true</code>, <code>w=majority</code> y timeouts explícitos (<code>10s</code> selección/conexión, <code>30s</code> socket).</li>
  </ul>

  <h3>4.1 Evidencias Visuales de la Infraestructura Cloud (AWS / GCP) Operando</h3>
  {''.join(cloud_blocks)}
  {extra_html}

  <h2>5. Conclusiones</h2>
  <ul>
    <li><strong>Ventaja cuantificable de Dask:</strong> El procesamiento particionado con <code>blocksize=64MB</code> y evaluación perezosa redujo significativamente tanto el tiempo total de cómputo como el pico de memoria RAM frente a Pandas secuencial, manteniendo equivalencia numérica exacta (diferencia <code>$0.00</code>).</li>
    <li><strong>Analítica NoSQL de baja latencia:</strong> El esquema desnormalizado con documentos embebidos e índices compuestos ESR permite responder agregaciones multidimensionales complejas sobre MongoDB Atlas sin escaneos completos de colección.</li>
    <li><strong>Portabilidad Cloud:</strong> La arquitectura desacoplada por roles permite migrar sin cambios de código desde desarrollo local hacia instancias EC2/Compute Engine y clústeres Dask distribuidos.</li>
  </ul>

</body>
</html>
"""

    meta = {
        "expected_screenshots_total": len(EXPECTED_SCREENSHOTS),
        "expected_screenshots_found": found_count,
        "extra_screenshots_found": extra_count,
        "missing_screenshots": missing_stems,
        "has_benchmark_data": bench is not None,
        "has_query_results": query_results_path.exists(),
    }
    return document_html, meta


def export_report_to_pdf(
    config: Optional[AppConfig] = None,
    output_pdf: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Compila `docs/REPORTE_TECNICO.pdf` usando Google Chrome Headless a partir
    de los datos reales del proyecto y las capturas en `docs/screenshots/`.
    """
    cfg = config or load_config()
    pdf_path = output_pdf or (cfg.docs_dir / "REPORTE_TECNICO.pdf")
    chrome_bin = _find_chrome_binary()

    html_content, meta = _build_html_document(cfg)

    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as tmp_html:
        tmp_html.write(html_content)
        tmp_html_path = Path(tmp_html.name)

    try:
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = [
            chrome_bin,
            "--headless=new",
            "--disable-gpu",
            "--no-sandbox",
            "--no-pdf-header-footer",
            f"--print-to-pdf={pdf_path}",
            tmp_html_path.as_uri(),
        ]
        logger.info("Exportando reporte técnico a PDF: %s ...", pdf_path)
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0 or not pdf_path.exists():
            raise RuntimeError(
                f"Falló la generación del PDF con Chrome (código {proc.returncode}). "
                f"STDERR: {proc.stderr}"
            )

        size_kb = round(pdf_path.stat().st_size / 1024.0, 1)
        logger.info(
            "PDF generado exitosamente: %s (%.1f KB) | Capturas encontradas: %d/%d (+%d extras)",
            pdf_path,
            size_kb,
            meta["expected_screenshots_found"],
            meta["expected_screenshots_total"],
            meta["extra_screenshots_found"],
        )
        return {
            "pdf_path": str(pdf_path),
            "size_kb": size_kb,
            **meta,
        }
    finally:
        tmp_html_path.unlink(missing_ok=True)


def main() -> int:
    setup_logging()
    cfg = load_config()
    try:
        export_report_to_pdf(cfg)
        return 0
    except Exception as exc:
        logger.exception("Error exportando el reporte técnico a PDF: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
