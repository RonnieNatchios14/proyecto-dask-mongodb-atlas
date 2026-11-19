"""
Pipeline ETL distribuido con Dask DataFrame y dask.distributed.

Rol responsable: Ingeniero de Datos.

Implementa lectura particionada con `blocksize`, limpieza de valores nulos,
registros inválidos y duplicados, optimización de tipos de datos, ingeniería de
características y agregaciones multidimensionales de forma perezosa (lazy),
ejecutando `.compute()` únicamente una vez al final del grafo DAG sobre un
clúster real (`LocalCluster` + `Client` o scheduler remoto AWS/GCP).
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import dask
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster

from src.cloud_architect.config import AppConfig, load_config, setup_logging

logger = logging.getLogger(__name__)

# Columnas críticas requeridas para el análisis agregado (pruning de columnas)
ANALYTICS_COLUMNS: List[str] = [
    "transaction_id",
    "timestamp",
    "region",
    "category",
    "payment_method",
    "status",
    "quantity",
    "unit_price",
    "discount_pct",
    "shipping_cost",
]

# Columnas completas para la muestra detallada que se persiste en MongoDB Atlas
DETAILED_COLUMNS: List[str] = [
    "transaction_id",
    "timestamp",
    "customer_id",
    "region",
    "country",
    "category",
    "product_sku",
    "payment_method",
    "channel",
    "status",
    "quantity",
    "unit_price",
    "discount_pct",
    "shipping_cost",
    "customer_segment",
]

# Tipos optimizados para minimizar huella de memoria RAM en Pandas y Dask
OPTIMIZED_DTYPES: Dict[str, Any] = {
    "transaction_id": "string",
    "timestamp": "string",
    "customer_id": "string",
    "region": "category",
    "country": "category",
    "category": "category",
    "product_sku": "string",
    "payment_method": "category",
    "channel": "category",
    "status": "category",
    "quantity": "float32",  # float32 tolera posibles NaN antes de limpieza
    "unit_price": "float32",
    "discount_pct": "float32",
    "shipping_cost": "float32",
    "customer_segment": "category",
}

CRITICAL_NOT_NULL_COLS: List[str] = [
    "transaction_id",
    "timestamp",
    "region",
    "category",
    "payment_method",
    "status",
    "quantity",
    "unit_price",
]

GROUP_BY_COLS: List[str] = ["region", "category", "payment_method", "year_month"]


@contextmanager
def dask_cluster_session(config: AppConfig) -> Iterator[Client]:
    """
    Context manager que administra el ciclo de vida del clúster Dask
    (`LocalCluster` local o conexión a scheduler remoto en AWS/GCP).
    """
    cluster = None
    client = None
    try:
        if config.dask_scheduler_address:
            logger.info("Conectando a Dask Scheduler remoto: %s", config.dask_scheduler_address)
            client = Client(config.dask_scheduler_address, timeout="15s")
        else:
            logger.info(
                "Iniciando Dask LocalCluster (workers=%d, threads/worker=%d, processes=%s, memory_limit=%s)...",
                config.dask_n_workers if config.dask_use_processes else 1,
                config.dask_threads_per_worker if config.dask_use_processes else config.dask_n_workers,
                config.dask_use_processes,
                config.dask_memory_limit,
            )
            cluster = LocalCluster(
                n_workers=config.dask_n_workers if config.dask_use_processes else 1,
                threads_per_worker=(
                    config.dask_threads_per_worker
                    if config.dask_use_processes
                    else config.dask_n_workers
                ),
                processes=config.dask_use_processes,
                memory_limit=config.dask_memory_limit,
                dashboard_address=None,
                silence_logs=logging.ERROR,
            )
            client = Client(cluster)
        yield client
    finally:
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()


def _dtypes_for_columns(columns: List[str]) -> Dict[str, Any]:
    """Filtra el mapa de dtypes optimizados a un subconjunto de columnas."""
    return {col: OPTIMIZED_DTYPES[col] for col in columns if col in OPTIMIZED_DTYPES}


def clean_and_enrich_dask(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Aplica limpieza, deduplicación, filtros avanzados y cálculo de métricas
    de negocio de forma 100% perezosa (sin invocar `.compute()`).
    """
    # 1. Eliminación de valores nulos en columnas críticas
    cleaned = ddf.dropna(subset=CRITICAL_NOT_NULL_COLS)

    # 2. Filtros de dominio y calidad de datos (excluye cantidades/precios inválidos y no completadas)
    cleaned = cleaned[
        (cleaned["status"] == "COMPLETED")
        & (cleaned["quantity"] > 0)
        & (cleaned["unit_price"] > 0)
        & (cleaned["discount_pct"] >= 0.0)
        & (cleaned["discount_pct"] <= 1.0)
    ]

    # 3. Optimización de tipos tras eliminar nulos y deduplicación por transaction_id
    cleaned = cleaned.assign(
        quantity=cleaned["quantity"].astype("int16"),
        shipping_cost=cleaned["shipping_cost"].fillna(0.0).astype("float32"),
        discount_pct=cleaned["discount_pct"].fillna(0.0).astype("float32"),
    )
    # Deduplicación particionada (streaming sin shuffle global costoso: cada archivo
    # particionado posee un rango disjunto de transaction_id con blocksize=64MB).
    cleaned = cleaned.map_partitions(
        lambda part: part.drop_duplicates(subset=["transaction_id"])
    )

    # 4. Ingeniería de características (montos brutos/netos y periodo YYYY-MM)
    gross_amount = (cleaned["quantity"].astype("float64") * cleaned["unit_price"].astype("float64"))
    net_amount = gross_amount * (1.0 - cleaned["discount_pct"].astype("float64")) + cleaned[
        "shipping_cost"
    ].astype("float64")

    enriched = cleaned.assign(
        gross_amount=gross_amount.round(2),
        net_amount=net_amount.round(2),
        year_month=cleaned["timestamp"].str.slice(0, 7),
    )

    # 5. Filtro avanzado de negocio: transacciones relevantes con ticket neto >= $25
    enriched = enriched[enriched["net_amount"] >= 25.0]
    return enriched


def aggregate_dask(enriched_ddf: dd.DataFrame) -> dd.DataFrame:
    """Construye el grafo perezoso de agregación analítica multidimensional."""
    grouped = (
        enriched_ddf.groupby(GROUP_BY_COLS, observed=True)
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
    return grouped


def normalize_summary_dataframe(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Estandariza tipos, redondeo y ordenamiento determinístico del resumen agregado."""
    df = summary_df.copy()
    for col in GROUP_BY_COLS:
        df[col] = df[col].astype(str)

    df["total_revenue"] = df["total_revenue"].astype("float64").round(2)
    df["avg_ticket"] = df["avg_ticket"].astype("float64").round(2)
    df["max_ticket"] = df["max_ticket"].astype("float64").round(2)
    df["total_units"] = df["total_units"].astype("int64")
    df["transaction_count"] = df["transaction_count"].astype("int64")
    df["avg_discount"] = df["avg_discount"].astype("float64").round(4)

    df = df.sort_values(by=GROUP_BY_COLS, ascending=True).reset_index(drop=True)
    return df


def read_raw_dask(
    raw_dir: Path,
    blocksize: str = "64MB",
    columns: Optional[List[str]] = None,
) -> dd.DataFrame:
    """
    Lee archivos particionados CSV (o Parquet si no hay CSV) usando `blocksize`
    optimizado para paralelismo controlado en memoria.
    """
    csv_files = sorted(raw_dir.glob("transactions_part_*.csv"))
    parquet_files = sorted(raw_dir.glob("transactions_part_*.parquet"))

    usecols = columns or ANALYTICS_COLUMNS
    dtypes = _dtypes_for_columns(usecols)

    if csv_files:
        pattern = str(raw_dir / "transactions_part_*.csv")
        return dd.read_csv(
            pattern,
            blocksize=blocksize,
            usecols=usecols,
            dtype=dtypes,
            assume_missing=True,
        )
    if parquet_files:
        pattern = str(raw_dir / "transactions_part_*.parquet")
        return dd.read_parquet(pattern, columns=usecols, engine="pyarrow")

    raise FileNotFoundError(
        f"No se encontraron archivos 'transactions_part_*.csv' o '*.parquet' en {raw_dir}. "
        "Ejecuta primero src/data_engineer/generate_dataset.py."
    )


def run_dask_analytics_only(config: Optional[AppConfig] = None) -> pd.DataFrame:
    """
    Ejecuta únicamente el pipeline analítico en Dask (misma transformación usada
    en el benchmark Pandas vs. Dask) llamando a `.compute()` una sola vez.
    """
    cfg = config or load_config()
    with dask_cluster_session(cfg):
        ddf = read_raw_dask(
            raw_dir=cfg.raw_data_dir,
            blocksize=cfg.dask_blocksize,
            columns=ANALYTICS_COLUMNS,
        )
        enriched = clean_and_enrich_dask(ddf)
        summary_ddf = aggregate_dask(enriched)
        # ÚNICA materialización del grafo DAG al final del flujo
        summary_pdf = summary_ddf.compute()
    return normalize_summary_dataframe(summary_pdf)


def run_full_etl_pipeline(
    config: Optional[AppConfig] = None,
    sample_size: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Ejecuta el pipeline ETL completo con Dask para alimentar MongoDB Atlas:
    - Produce la tabla agregada multidimensional (`summary_df`).
    - Produce una muestra determinística curada de transacciones detalladas (`sample_df`).
    - Ejecuta `dask.compute(...)` una sola vez para materializar todas las salidas en un único pase.
    """
    cfg = config or load_config()
    target_sample = sample_size or cfg.mongo_sample_size

    logger.info(
        "Iniciando pipeline ETL distribuido Dask (blocksize=%s, muestra detallada=%s)...",
        cfg.dask_blocksize,
        f"{target_sample:,}",
    )
    t0 = time.perf_counter()

    with dask_cluster_session(cfg):
        ddf = read_raw_dask(
            raw_dir=cfg.raw_data_dir,
            blocksize=cfg.dask_blocksize,
            columns=DETAILED_COLUMNS,
        )
        raw_count_task = ddf.shape[0]
        n_partitions = ddf.npartitions

        enriched = clean_and_enrich_dask(ddf)
        cleaned_count_task = enriched.shape[0]
        summary_task = aggregate_dask(enriched)

        # Fracción aproximada para obtener al menos `target_sample` filas sin materializar todo el dataset
        approx_frac = min(1.0, max(0.015, (target_sample * 1.35) / max(1, cfg.dataset_rows)))
        sample_task = enriched.sample(frac=approx_frac, random_state=42)

        # Única llamada a dask.compute() para ejecutar todo el grafo en paralelo
        raw_count, cleaned_count, summary_pdf, sample_pdf = dask.compute(
            raw_count_task,
            cleaned_count_task,
            summary_task,
            sample_task,
        )

    summary_df = normalize_summary_dataframe(summary_pdf)

    # Recortar exactamente a target_sample registros detallados ordenados determinísticamente
    if len(sample_pdf) > target_sample:
        sample_df = sample_pdf.sample(n=target_sample, random_state=42).reset_index(drop=True)
    else:
        sample_df = sample_pdf.reset_index(drop=True)

    elapsed = round(time.perf_counter() - t0, 2)
    metrics = {
        "n_partitions": int(n_partitions),
        "raw_rows": int(raw_count),
        "cleaned_valid_rows": int(cleaned_count),
        "discarded_rows": int(raw_count - cleaned_count),
        "summary_documents": int(len(summary_df)),
        "detailed_sample_documents": int(len(sample_df)),
        "total_revenue_usd": round(float(summary_df["total_revenue"].sum()), 2),
        "elapsed_seconds": elapsed,
    }
    logger.info(
        "Pipeline Dask finalizado en %.2f s | Filas brutas: %s -> Limpias: %s | Docs resumen: %d | Muestra: %s",
        elapsed,
        f"{metrics['raw_rows']:,}",
        f"{metrics['cleaned_valid_rows']:,}",
        metrics["summary_documents"],
        f"{metrics['detailed_sample_documents']:,}",
    )
    return summary_df, sample_df, metrics


if __name__ == "__main__":
    setup_logging()
    cfg = load_config()
    summary, sample, stats = run_full_etl_pipeline(cfg)
    logger.info("Métricas ETL: %s", stats)
    logger.info("Top 5 filas del resumen agregado:\n%s", summary.head(5).to_string(index=False))
