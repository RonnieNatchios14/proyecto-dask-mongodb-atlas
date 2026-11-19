"""
Benchmark comparativo Pandas secuencial vs. Dask paralelo (tiempo y pico de RAM).

Rol responsable: Ingeniero de Datos.

Ejecuta exactamente la misma transformación analítica (lectura particionada,
limpieza de nulos/inválidos, filtrado, deduplicación, cálculo de montos netos
y agregación multidimensional) en ambos motores dentro de subprocesos aislados
para obtener mediciones limpias de tiempo de ejecución y pico real de memoria
RAM (RSS del proceso padre + todos los workers hijos vía `psutil`).

Genera:
- `docs/benchmark_results.json`: Evidencia numérica completa con % de mejora.
- `docs/benchmark_chart.png`: Gráfica comparativa lista para el reporte técnico.
"""

from __future__ import annotations

import argparse
import json
import logging
import platform
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import psutil

from src.cloud_architect.config import AppConfig, load_config, setup_logging
from src.data_engineer.dask_pipeline import (
    ANALYTICS_COLUMNS,
    CRITICAL_NOT_NULL_COLS,
    GROUP_BY_COLS,
    OPTIMIZED_DTYPES,
    normalize_summary_dataframe,
    run_dask_analytics_only,
)

logger = logging.getLogger(__name__)


def run_pandas_analytics_only(config: AppConfig) -> pd.DataFrame:
    """
    Ejecuta en Pandas secuencial exactamente la misma transformación que
    `run_dask_analytics_only`, cargando las particiones secuencialmente en memoria.
    """
    csv_files = sorted(config.raw_data_dir.glob("transactions_part_*.csv"))
    parquet_files = sorted(config.raw_data_dir.glob("transactions_part_*.parquet"))

    dtypes = {col: OPTIMIZED_DTYPES[col] for col in ANALYTICS_COLUMNS if col in OPTIMIZED_DTYPES}

    if csv_files:
        frames = [
            pd.read_csv(f, usecols=ANALYTICS_COLUMNS, dtype=dtypes)
            for f in csv_files
        ]
        df = pd.concat(frames, ignore_index=True)
        del frames
    elif parquet_files:
        frames = [
            pd.read_parquet(f, columns=ANALYTICS_COLUMNS, engine="pyarrow")
            for f in parquet_files
        ]
        df = pd.concat(frames, ignore_index=True)
        del frames
    else:
        raise FileNotFoundError(
            f"No se encontraron particiones en {config.raw_data_dir}. "
            "Ejecuta primero src/data_engineer/generate_dataset.py."
        )

    # 1. Limpieza de nulos en columnas críticas
    df = df.dropna(subset=CRITICAL_NOT_NULL_COLS)

    # 2. Filtros de dominio y calidad de datos
    df = df[
        (df["status"] == "COMPLETED")
        & (df["quantity"] > 0)
        & (df["unit_price"] > 0)
        & (df["discount_pct"] >= 0.0)
        & (df["discount_pct"] <= 1.0)
    ].copy()

    # 3. Tipos optimizados y deduplicación por transaction_id
    df["quantity"] = df["quantity"].astype("int16")
    df["shipping_cost"] = df["shipping_cost"].fillna(0.0).astype("float32")
    df["discount_pct"] = df["discount_pct"].fillna(0.0).astype("float32")
    df = df.drop_duplicates(subset=["transaction_id"])

    # 4. Ingeniería de características
    gross_amount = df["quantity"].astype("float64") * df["unit_price"].astype("float64")
    net_amount = gross_amount * (1.0 - df["discount_pct"].astype("float64")) + df[
        "shipping_cost"
    ].astype("float64")

    df["gross_amount"] = gross_amount.round(2)
    df["net_amount"] = net_amount.round(2)
    df["year_month"] = df["timestamp"].str.slice(0, 7)

    # 5. Filtro de negocio
    df = df[df["net_amount"] >= 25.0]

    # 6. Agregación multidimensional idéntica a Dask
    grouped = (
        df.groupby(GROUP_BY_COLS, observed=True)
        .agg(
            total_revenue=("net_amount", "sum"),
            avg_ticket=("net_amount", "mean"),
            max_ticket=("net_amount", "max"),
            total_units=("quantity", "sum"),
            transaction_count=("transaction_id", "count"),
            avg_discount=("discount_pct", "mean"),
        )
        .reset_index()
    )
    return normalize_summary_dataframe(grouped)


def _measure_process_tree_rss_mb(pid: int) -> float:
    """Retorna la memoria RSS combinada (en MB) de un proceso y todos sus hijos."""
    try:
        parent = psutil.Process(pid)
        total_bytes = parent.memory_info().rss
        for child in parent.children(recursive=True):
            try:
                total_bytes += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return total_bytes / (1024 ** 2)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0.0


def _run_isolated_engine_benchmark(
    engine: str,
    project_root: Path,
    sample_interval_s: float = 0.05,
) -> Dict[str, Any]:
    """
    Ejecuta un motor (`pandas` o `dask`) en un subproceso Python aislado
    midiendo el tiempo real de pared y el pico de RAM RSS del árbol completo
    de procesos (fundamental para contabilizar los workers de Dask con honestidad).
    """
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        result_file = Path(tmp.name)

    cmd = [
        sys.executable,
        "-m",
        "src.data_engineer.benchmark",
        "--worker-engine",
        engine,
        "--worker-output",
        str(result_file),
    ]

    peak_rss_mb = 0.0
    rss_samples: List[float] = []
    stop_event = threading.Event()

    t0 = time.perf_counter()
    proc = subprocess.Popen(
        cmd,
        cwd=str(project_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    def _monitor() -> None:
        nonlocal peak_rss_mb
        while not stop_event.is_set():
            if proc.poll() is not None:
                break
            rss = _measure_process_tree_rss_mb(proc.pid)
            if rss > 0:
                rss_samples.append(rss)
                if rss > peak_rss_mb:
                    peak_rss_mb = rss
            time.sleep(sample_interval_s)

    monitor_thread = threading.Thread(target=_monitor, daemon=True)
    monitor_thread.start()

    stdout_data, stderr_data = proc.communicate()
    stop_event.set()
    monitor_thread.join(timeout=2.0)
    wall_elapsed_s = time.perf_counter() - t0

    if proc.returncode != 0:
        result_file.unlink(missing_ok=True)
        raise RuntimeError(
            f"Falló la ejecución aislada del motor '{engine}' (código {proc.returncode}).\n"
            f"STDOUT:\n{stdout_data}\nSTDERR:\n{stderr_data}"
        )

    worker_payload = json.loads(result_file.read_text(encoding="utf-8"))
    result_file.unlink(missing_ok=True)

    avg_rss_mb = sum(rss_samples) / len(rss_samples) if rss_samples else peak_rss_mb

    return {
        "engine": engine,
        "execution_time_seconds": round(float(worker_payload["compute_seconds"]), 3),
        "wall_time_with_startup_seconds": round(wall_elapsed_s, 3),
        "peak_ram_mb": round(peak_rss_mb, 2),
        "avg_ram_mb": round(avg_rss_mb, 2),
        "summary_groups": int(worker_payload["summary_groups"]),
        "total_transactions_aggregated": int(worker_payload["total_transactions"]),
        "total_revenue_usd": round(float(worker_payload["total_revenue_usd"]), 2),
    }


def _generate_benchmark_chart(results: Dict[str, Any], output_path: Path) -> None:
    """Genera una gráfica comparativa profesional Pandas vs. Dask (tiempo y pico RAM)."""
    pandas_res = results["pandas"]
    dask_res = results["dask"]
    comp = results["comparison"]

    engines = ["Pandas\n(Secuencial)", "Dask\n(Paralelo)"]
    times = [pandas_res["execution_time_seconds"], dask_res["execution_time_seconds"]]
    rams = [pandas_res["peak_ram_mb"], dask_res["peak_ram_mb"]]
    colors = ["#E07A5F", "#2A9D8F"]

    fig, axes = plt.subplots(1, 2, figsize=(12.5, 5.5), dpi=160)
    fig.patch.set_facecolor("#FFFFFF")

    # Panel 1: Tiempo de ejecución
    ax1 = axes[0]
    bars1 = ax1.bar(engines, times, color=colors, width=0.52, edgecolor="#264653", linewidth=1.2)
    ax1.set_title("Tiempo de Ejecución (segundos)\n(Menor es mejor)", fontsize=12, fontweight="bold", pad=12)
    ax1.set_ylabel("Segundos (s)", fontsize=10.5)
    ax1.grid(axis="y", linestyle="--", alpha=0.45)
    ax1.set_axisbelow(True)
    ax1.set_ylim(0, max(times) * 1.25)

    for bar, val in zip(bars1, times):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            val + max(times) * 0.03,
            f"{val:.2f} s",
            ha="center",
            va="bottom",
            fontsize=11,
            fontweight="bold",
            color="#1D3557",
        )

    time_imp = comp["time_improvement_pct"]
    time_badge = (
        f"Mejora Dask: +{time_imp:.1f}% más rápido ({comp['speedup_factor']:.2f}x)"
        if time_imp >= 0
        else f"Diferencia Dask: {time_imp:.1f}% ({comp['speedup_factor']:.2f}x)"
    )
    ax1.annotate(
        time_badge,
        xy=(0.5, 0.92),
        xycoords="axes fraction",
        ha="center",
        fontsize=9.5,
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.35", facecolor="#E9F5F3", edgecolor="#2A9D8F"),
    )

    # Panel 2: Pico de RAM
    ax2 = axes[1]
    bars2 = ax2.bar(engines, rams, color=colors, width=0.52, edgecolor="#264653", linewidth=1.2)
    ax2.set_title("Pico de Memoria RAM - RSS Total (MB)\n(Menor es mejor)", fontsize=12, fontweight="bold", pad=12)
    ax2.set_ylabel("Megabytes (MB)", fontsize=10.5)
    ax2.grid(axis="y", linestyle="--", alpha=0.45)
    ax2.set_axisbelow(True)
    ax2.set_ylim(0, max(rams) * 1.25)

    for bar, val in zip(bars2, rams):
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            val + max(rams) * 0.03,
            f"{val:,.1f} MB",
            ha="center",
            va="bottom",
            fontsize=11,
            fontweight="bold",
            color="#1D3557",
        )

    ram_imp = comp["ram_reduction_pct"]
    ram_badge = (
        f"Ahorro RAM Dask: {ram_imp:.1f}% menos pico"
        if ram_imp >= 0
        else f"Diferencia RAM Dask: {ram_imp:.1f}%"
    )
    ax2.annotate(
        ram_badge,
        xy=(0.5, 0.92),
        xycoords="axes fraction",
        ha="center",
        fontsize=9.5,
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.35", facecolor="#E9F5F3", edgecolor="#2A9D8F"),
    )

    ds_info = results["dataset"]
    fig.suptitle(
        f"Benchmark Big Data: Pandas Secuencial vs. Dask Paralelo\n"
        f"Dataset: {ds_info['total_size_mb']:,.1f} MB ({ds_info['files_count']} particiones) | "
        f"Dask: {results['dask_config']['n_workers']} workers, blocksize={results['dask_config']['blocksize']}",
        fontsize=13,
        fontweight="bold",
        y=0.99,
    )
    plt.tight_layout(rect=[0, 0, 1, 0.91])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    logger.info("Gráfico comparativo guardado en: %s", output_path)


def run_benchmark(config: Optional[AppConfig] = None) -> Dict[str, Any]:
    """
    Orquesta el benchmark comparativo Pandas vs. Dask y persiste los resultados
    en `docs/benchmark_results.json` y `docs/benchmark_chart.png`.
    """
    cfg = config or load_config()
    data_files = sorted(cfg.raw_data_dir.glob("transactions_part_*.csv"))
    file_format = "csv"
    if not data_files:
        data_files = sorted(cfg.raw_data_dir.glob("transactions_part_*.parquet"))
        file_format = "parquet"

    if not data_files:
        raise FileNotFoundError(
            f"No hay archivos de dataset en {cfg.raw_data_dir}. Ejecuta generate_dataset.py primero."
        )

    total_bytes = sum(f.stat().st_size for f in data_files)
    total_size_mb = round(total_bytes / (1024 ** 2), 2)

    logger.info(
        "=== Iniciando Benchmark Pandas vs. Dask (%d archivos %s, %.2f MB) ===",
        len(data_files),
        file_format.upper(),
        total_size_mb,
    )

    logger.info("[1/2] Ejecutando benchmark aislado: Pandas secuencial...")
    pandas_metrics = _run_isolated_engine_benchmark("pandas", cfg.project_root)
    logger.info(
        "-> Pandas finalizado | Tiempo: %.2f s | Pico RAM: %.1f MB | Grupos: %d | Revenue: $%s",
        pandas_metrics["execution_time_seconds"],
        pandas_metrics["peak_ram_mb"],
        pandas_metrics["summary_groups"],
        f"{pandas_metrics['total_revenue_usd']:,.2f}",
    )

    logger.info("[2/2] Ejecutando benchmark aislado: Dask paralelo...")
    dask_metrics = _run_isolated_engine_benchmark("dask", cfg.project_root)
    logger.info(
        "-> Dask finalizado   | Tiempo: %.2f s | Pico RAM: %.1f MB | Grupos: %d | Revenue: $%s",
        dask_metrics["execution_time_seconds"],
        dask_metrics["peak_ram_mb"],
        dask_metrics["summary_groups"],
        f"{dask_metrics['total_revenue_usd']:,.2f}",
    )

    # Cálculos de mejora porcentual
    p_time = pandas_metrics["execution_time_seconds"]
    d_time = dask_metrics["execution_time_seconds"]
    p_ram = pandas_metrics["peak_ram_mb"]
    d_ram = dask_metrics["peak_ram_mb"]

    time_improvement_pct = round(((p_time - d_time) / p_time) * 100.0, 2) if p_time > 0 else 0.0
    speedup_factor = round(p_time / d_time, 2) if d_time > 0 else 0.0
    ram_reduction_pct = round(((p_ram - d_ram) / p_ram) * 100.0, 2) if p_ram > 0 else 0.0

    # Validación de equivalencia numérica exacta entre ambos motores
    rev_diff = abs(pandas_metrics["total_revenue_usd"] - dask_metrics["total_revenue_usd"])
    equivalent = (
        pandas_metrics["summary_groups"] == dask_metrics["summary_groups"]
        and pandas_metrics["total_transactions_aggregated"]
        == dask_metrics["total_transactions_aggregated"]
        and rev_diff <= 1.0
    )

    vm = psutil.virtual_memory()
    results: Dict[str, Any] = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "hardware": {
            "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
            "python_version": platform.python_version(),
            "cpu_logical_cores": psutil.cpu_count(logical=True),
            "ram_total_gb": round(vm.total / (1024 ** 3), 2),
        },
        "dataset": {
            "raw_dir": str(cfg.raw_data_dir),
            "format": file_format,
            "files_count": len(data_files),
            "total_size_mb": total_size_mb,
            "configured_rows": cfg.dataset_rows,
        },
        "dask_config": {
            "n_workers": cfg.dask_n_workers,
            "threads_per_worker": cfg.dask_threads_per_worker,
            "memory_limit": cfg.dask_memory_limit,
            "blocksize": cfg.dask_blocksize,
        },
        "pandas": pandas_metrics,
        "dask": dask_metrics,
        "comparison": {
            "time_improvement_pct": time_improvement_pct,
            "speedup_factor": speedup_factor,
            "ram_reduction_pct": ram_reduction_pct,
            "ram_saved_mb": round(p_ram - d_ram, 2),
            "numerical_equivalence_verified": equivalent,
            "revenue_absolute_diff_usd": round(rev_diff, 4),
        },
    }

    json_path = cfg.docs_dir / "benchmark_results.json"
    chart_path = cfg.docs_dir / "benchmark_chart.png"
    json_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Resultados numéricos guardados en: %s", json_path)

    _generate_benchmark_chart(results, chart_path)

    logger.info(
        "=== Resumen Benchmark === Tiempo: %+.2f%% (%.2fx) | Pico RAM: %+.2f%% | Equivalencia: %s",
        time_improvement_pct,
        speedup_factor,
        ram_reduction_pct,
        "OK" if equivalent else "REVISAR",
    )
    return results


def _run_worker_entrypoint(engine: str, output_file: Path) -> int:
    """Punto de entrada interno para subprocesos aislados de benchmark."""
    setup_logging("WARNING")
    cfg = load_config()
    t0 = time.perf_counter()
    if engine == "pandas":
        summary_df = run_pandas_analytics_only(cfg)
    elif engine == "dask":
        summary_df = run_dask_analytics_only(cfg)
    else:
        raise ValueError(f"Motor no soportado: {engine}")
    elapsed = time.perf_counter() - t0

    payload = {
        "engine": engine,
        "compute_seconds": elapsed,
        "summary_groups": int(len(summary_df)),
        "total_transactions": int(summary_df["transaction_count"].sum()),
        "total_revenue_usd": float(summary_df["total_revenue"].sum()),
    }
    output_file.write_text(json.dumps(payload), encoding="utf-8")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark comparativo Pandas vs. Dask (Rol: Ingeniero de Datos)."
    )
    parser.add_argument(
        "--worker-engine",
        choices=["pandas", "dask"],
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--worker-output",
        type=str,
        default=None,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    if args.worker_engine and args.worker_output:
        return _run_worker_entrypoint(args.worker_engine, Path(args.worker_output))

    setup_logging()
    cfg = load_config()
    try:
        run_benchmark(cfg)
        return 0
    except Exception as exc:
        logger.exception("Error ejecutando benchmark Pandas vs. Dask: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
