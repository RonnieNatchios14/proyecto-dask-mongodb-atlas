"""
Cargador masivo (Bulk Loader) e indexación óptima en MongoDB Atlas.

Rol responsable: DBA NoSQL.

Diseña un esquema documental desnormalizado orientado a consultas analíticas
de baja latencia:
1. `sales_analytics_summary`: Documentos pre-agregados por región, categoría,
   método de pago y periodo (`YYYY-MM`) con subdocumento `metrics`.
2. `transactions_sample`: Muestra curada de transacciones detalladas con
   subdocumentos embebidos (`customer`, `product`, `financials`) que evitan
   joins costosos (`$lookup`).
3. `etl_execution_logs`: Auditoría operativa de cada carga ejecutada.

Utiliza `insert_many(..., ordered=False)` por lotes y crea índices compuestos
estratégicos antes de ejecutar las consultas complejas.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from pymongo import ASCENDING, DESCENDING, IndexModel
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError, PyMongoError

from src.cloud_architect.config import AppConfig, load_config

logger = logging.getLogger(__name__)

COLLECTION_SUMMARY = "sales_analytics_summary"
COLLECTION_SAMPLE = "transactions_sample"
COLLECTION_ETL_LOGS = "etl_execution_logs"


def _chunked(records: List[Dict[str, Any]], batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    """Divide una lista de documentos en lotes de tamaño fijo."""
    for start in range(0, len(records), batch_size):
        yield records[start : start + batch_size]


def build_summary_documents(summary_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transforma el DataFrame agregado de Dask en documentos desnormalizados
    óptimos para consultas analíticas en `sales_analytics_summary`.
    """
    loaded_at = datetime.now(timezone.utc)
    docs: List[Dict[str, Any]] = []

    for row in summary_df.itertuples(index=False):
        docs.append(
            {
                "period": str(row.year_month),
                "region": str(row.region),
                "category": str(row.category),
                "payment_method": str(row.payment_method),
                "metrics": {
                    "total_revenue": round(float(row.total_revenue), 2),
                    "avg_ticket": round(float(row.avg_ticket), 2),
                    "max_ticket": round(float(row.max_ticket), 2),
                    "total_units": int(row.total_units),
                    "transaction_count": int(row.transaction_count),
                    "avg_discount_pct": round(float(row.avg_discount), 4),
                },
                "etl_metadata": {
                    "loaded_at": loaded_at,
                    "source_engine": "dask_distributed",
                    "schema_version": "1.0",
                },
            }
        )
    return docs


def build_detailed_sample_documents(sample_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Transforma la muestra detallada de transacciones en documentos embebidos
    desnormalizados (`customer`, `product`, `financials`) para `transactions_sample`.
    """
    loaded_at = datetime.now(timezone.utc)
    docs: List[Dict[str, Any]] = []

    # Parseo vectorizado de timestamps para almacenar tipo nativo ISODate en MongoDB
    parsed_ts = pd.to_datetime(sample_df["timestamp"], errors="coerce", utc=True)

    for row, ts_val in zip(sample_df.itertuples(index=False), parsed_ts):
        ts_dt = ts_val.to_pydatetime() if pd.notna(ts_val) else loaded_at
        docs.append(
            {
                "transaction_id": str(row.transaction_id),
                "timestamp": ts_dt,
                "year_month": str(row.year_month),
                "status": str(row.status),
                "channel": str(row.channel),
                "payment_method": str(row.payment_method),
                "customer": {
                    "customer_id": str(row.customer_id),
                    "segment": str(row.customer_segment),
                    "region": str(row.region),
                    "country": str(row.country),
                },
                "product": {
                    "sku": str(row.product_sku),
                    "category": str(row.category),
                },
                "financials": {
                    "quantity": int(row.quantity),
                    "unit_price": round(float(row.unit_price), 2),
                    "discount_pct": round(float(row.discount_pct), 4),
                    "shipping_cost": round(float(row.shipping_cost), 2),
                    "gross_amount": round(float(row.gross_amount), 2),
                    "net_amount": round(float(row.net_amount), 2),
                },
                "loaded_at": loaded_at,
            }
        )
    return docs


def create_optimal_indexes(db: Database) -> Dict[str, List[str]]:
    """
    Crea índices compuestos estratégicos (regla ESR: Equality, Sort, Range)
    en ambas colecciones analíticas antes de ejecutar las consultas complejas.
    """
    logger.info("Creando índices compuestos óptimos en MongoDB Atlas...")

    summary_indexes = [
        IndexModel(
            [
                ("region", ASCENDING),
                ("category", ASCENDING),
                ("payment_method", ASCENDING),
                ("period", ASCENDING),
            ],
            name="uq_summary_grain",
            unique=True,
        ),
        IndexModel(
            [
                ("region", ASCENDING),
                ("period", ASCENDING),
                ("metrics.total_revenue", DESCENDING),
            ],
            name="idx_summary_region_period_rev",
        ),
        IndexModel(
            [
                ("category", ASCENDING),
                ("payment_method", ASCENDING),
                ("metrics.total_revenue", DESCENDING),
            ],
            name="idx_summary_cat_pay_rev",
        ),
    ]

    sample_indexes = [
        IndexModel(
            [("transaction_id", ASCENDING)],
            name="uq_txn_id",
            unique=True,
        ),
        IndexModel(
            [
                ("status", ASCENDING),
                ("customer.segment", ASCENDING),
                ("financials.net_amount", DESCENDING),
            ],
            name="idx_txn_status_segment_net",
        ),
        IndexModel(
            [
                ("customer.region", ASCENDING),
                ("product.category", ASCENDING),
                ("financials.net_amount", DESCENDING),
            ],
            name="idx_txn_region_cat_net",
        ),
        IndexModel(
            [
                ("payment_method", ASCENDING),
                ("channel", ASCENDING),
                ("timestamp", DESCENDING),
            ],
            name="idx_txn_pay_channel_ts",
        ),
    ]

    created_summary = db[COLLECTION_SUMMARY].create_indexes(summary_indexes)
    created_sample = db[COLLECTION_SAMPLE].create_indexes(sample_indexes)

    logger.info(
        "Índices listos | %s: %s | %s: %s",
        COLLECTION_SUMMARY,
        created_summary,
        COLLECTION_SAMPLE,
        created_sample,
    )
    return {
        COLLECTION_SUMMARY: created_summary,
        COLLECTION_SAMPLE: created_sample,
    }


def bulk_insert_collection(
    collection: Collection,
    documents: List[Dict[str, Any]],
    batch_size: int = 2000,
) -> Dict[str, Any]:
    """
    Inserta documentos por lotes usando `insert_many(..., ordered=False)` para
    maximizar el throughput de red hacia MongoDB Atlas y tolerar duplicados aislados.
    """
    if not documents:
        return {"inserted": 0, "duplicates_ignored": 0, "batches": 0, "elapsed_seconds": 0.0}

    t0 = time.perf_counter()
    inserted_total = 0
    duplicates_ignored = 0
    batches = 0

    for batch in _chunked(documents, batch_size):
        batches += 1
        try:
            res = collection.insert_many(batch, ordered=False)
            inserted_total += len(res.inserted_ids)
        except BulkWriteError as bwe:
            details = bwe.details or {}
            inserted_total += int(details.get("nInserted", 0))
            write_errors = details.get("writeErrors", [])
            dup_errors = [err for err in write_errors if err.get("code") == 11000]
            non_dup_errors = [err for err in write_errors if err.get("code") != 11000]
            duplicates_ignored += len(dup_errors)
            if non_dup_errors:
                logger.error(
                    "Errores críticos en lote %d de '%s': %s",
                    batches,
                    collection.name,
                    non_dup_errors[:3],
                )
                raise

    elapsed = round(time.perf_counter() - t0, 2)
    throughput = round(inserted_total / elapsed, 1) if elapsed > 0 else 0.0
    logger.info(
        "Carga masiva '%s': %s insertados (%d duplicados omitidos) en %d lotes | %.2f s (%.1f docs/s)",
        collection.name,
        f"{inserted_total:,}",
        duplicates_ignored,
        batches,
        elapsed,
        throughput,
    )
    return {
        "collection": collection.name,
        "inserted": inserted_total,
        "duplicates_ignored": duplicates_ignored,
        "batches": batches,
        "elapsed_seconds": elapsed,
        "docs_per_second": throughput,
    }


def load_to_mongodb_atlas(
    db: Database,
    summary_df: pd.DataFrame,
    sample_df: pd.DataFrame,
    etl_metrics: Optional[Dict[str, Any]] = None,
    config: Optional[AppConfig] = None,
    replace_existing: bool = True,
) -> Dict[str, Any]:
    """
    Orquesta la carga completa a MongoDB Atlas:
    1. Opcionalmente limpia colecciones previas para idempotencia de la práctica.
    2. Construye documentos desnormalizados (`sales_analytics_summary` y `transactions_sample`).
    3. Inserta por lotes con `insert_many(..., ordered=False)`.
    4. Garantiza la creación de índices compuestos óptimos antes de las consultas.
    5. Registra telemetría del pipeline en `etl_execution_logs`.
    """
    cfg = config or load_config()
    logger.info(
        "=== Iniciando carga masiva a MongoDB Atlas (batch_size=%d, replace=%s) ===",
        cfg.mongo_batch_size,
        replace_existing,
    )
    t0 = time.perf_counter()

    try:
        if replace_existing:
            logger.info("Limpiando documentos previos en colecciones analíticas para carga limpia...")
            db[COLLECTION_SUMMARY].delete_many({})
            db[COLLECTION_SAMPLE].delete_many({})

        # Crear índices compuestos antes o junto con la carga para garantizar soporte óptimo de consultas
        indexes = create_optimal_indexes(db)

        summary_docs = build_summary_documents(summary_df)
        sample_docs = build_detailed_sample_documents(sample_df)

        summary_stats = bulk_insert_collection(
            db[COLLECTION_SUMMARY],
            summary_docs,
            batch_size=cfg.mongo_batch_size,
        )
        sample_stats = bulk_insert_collection(
            db[COLLECTION_SAMPLE],
            sample_docs,
            batch_size=cfg.mongo_batch_size,
        )

        total_elapsed = round(time.perf_counter() - t0, 2)
        audit_doc = {
            "executed_at": datetime.now(timezone.utc),
            "cloud_provider": cfg.cloud_provider,
            "cloud_region": cfg.cloud_region,
            "environment": cfg.environment,
            "etl_metrics": etl_metrics or {},
            "load_stats": {
                "summary": summary_stats,
                "sample": sample_stats,
                "total_load_seconds": total_elapsed,
            },
            "indexes": indexes,
        }
        db[COLLECTION_ETL_LOGS].insert_one(audit_doc)

        logger.info("=== Carga a MongoDB Atlas completada en %.2f s ===", total_elapsed)
        return {
            "summary_load": summary_stats,
            "sample_load": sample_stats,
            "indexes": indexes,
            "total_elapsed_seconds": total_elapsed,
        }
    except PyMongoError as exc:
        logger.exception("Error durante la carga masiva a MongoDB Atlas: %s", exc)
        raise
