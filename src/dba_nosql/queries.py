"""
Consultas analíticas complejas en MongoDB Atlas y perfilado de rendimiento.

Rol responsable: DBA NoSQL.

Ejecuta 3 consultas complejas con filtros compuestos, `$match`, `$group`,
`$project`, `$sort` y `$limit` sobre colecciones indexadas en MongoDB Atlas,
midiendo latencia de servidor y uso de índices mediante `.explain("executionStats")`
y guardando el reporte formateado en `docs/query_results.txt`.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pymongo import DESCENDING
from pymongo.database import Database
from pymongo.errors import PyMongoError

from src.cloud_architect.config import AppConfig, load_config, setup_logging
from src.dba_nosql.loader import COLLECTION_SAMPLE, COLLECTION_SUMMARY
from src.dba_nosql.mongo_client import MongoAtlasClient

logger = logging.getLogger(__name__)


def _extract_plan_stages(plan: Dict[str, Any], stages: Optional[List[str]] = None) -> List[str]:
    """Recorre recursivamente un árbol de plan de ejecución de MongoDB para listar stages e índices."""
    if stages is None:
        stages = []
    if not isinstance(plan, dict):
        return stages

    stage_name = plan.get("stage")
    if stage_name:
        idx_name = plan.get("indexName")
        stages.append(f"{stage_name}({idx_name})" if idx_name else str(stage_name))

    for key in ("inputStage", "winningPlan", "queryPlan"):
        if key in plan and isinstance(plan[key], dict):
            _extract_plan_stages(plan[key], stages)

    for key in ("inputStages", "shards"):
        child = plan.get(key)
        if isinstance(child, list):
            for item in child:
                _extract_plan_stages(item, stages)
        elif isinstance(child, dict):
            for item in child.values():
                _extract_plan_stages(item, stages)
    return stages


def _parse_explain_stats(explain_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrae métricas clave de `.explain("executionStats")` tanto para consultas
    `find()` como para pipelines `aggregate()`.
    """
    exec_stats = explain_doc.get("executionStats")
    query_planner = explain_doc.get("queryPlanner", {})

    # En pipelines aggregate, executionStats suele residir en stages[0].$cursor
    if not exec_stats and "stages" in explain_doc:
        for stage in explain_doc["stages"]:
            cursor_block = stage.get("$cursor")
            if isinstance(cursor_block, dict):
                exec_stats = cursor_block.get("executionStats", {})
                query_planner = cursor_block.get("queryPlanner", {})
                break

    exec_stats = exec_stats or {}
    winning_plan = query_planner.get("winningPlan", {})
    stages_chain = _extract_plan_stages(winning_plan)
    uses_index = any("IXSCAN" in s or "DISTINCT_SCAN" in s for s in stages_chain)

    return {
        "server_execution_time_ms": exec_stats.get("executionTimeMillis", "N/A"),
        "n_returned": exec_stats.get("nReturned", "N/A"),
        "total_keys_examined": exec_stats.get("totalKeysExamined", "N/A"),
        "total_docs_examined": exec_stats.get("totalDocsExamined", "N/A"),
        "uses_index": uses_index,
        "winning_plan_stages": " <- ".join(stages_chain) if stages_chain else "N/A",
    }


def _run_aggregation_with_explain(
    db: Database,
    collection_name: str,
    title: str,
    description: str,
    pipeline: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Ejecuta un pipeline de agregación y captura su explain('executionStats')."""
    t0 = time.perf_counter()
    results = list(db[collection_name].aggregate(pipeline, allowDiskUse=True))
    client_latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)

    explain_raw = db.command(
        "explain",
        {
            "aggregate": collection_name,
            "pipeline": pipeline,
            "cursor": {},
        },
        verbosity="executionStats",
    )
    stats = _parse_explain_stats(explain_raw)

    logger.info(
        "%s | Resultados: %d | Latencia cliente: %.2f ms | Server exec: %s ms | IXSCAN: %s",
        title,
        len(results),
        client_latency_ms,
        stats["server_execution_time_ms"],
        stats["uses_index"],
    )

    return {
        "title": title,
        "collection": collection_name,
        "description": description,
        "pipeline": pipeline,
        "client_latency_ms": client_latency_ms,
        "execution_stats": stats,
        "results": results,
    }


def _run_indexed_find_verification(db: Database) -> Dict[str, Any]:
    """
    Ejecuta una consulta `find().sort().limit().explain("executionStats")`
    directa sobre el índice compuesto `idx_txn_region_cat_net` para evidenciar
    de forma explícita la lectura cubierta/indexada (`IXSCAN`).
    """
    filter_query = {
        "customer.region": "North_America",
        "product.category": "Electronics",
        "financials.net_amount": {"$gte": 1000.0},
    }
    projection = {
        "_id": 0,
        "transaction_id": 1,
        "customer.country": 1,
        "customer.segment": 1,
        "product.sku": 1,
        "financials.net_amount": 1,
    }
    sort_spec = [("financials.net_amount", DESCENDING)]

    t0 = time.perf_counter()
    cursor = (
        db[COLLECTION_SAMPLE]
        .find(filter_query, projection)
        .sort(sort_spec)
        .limit(5)
    )
    docs = list(cursor)
    client_latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)

    # Llamada explícita a .explain("executionStats") sobre el cursor PyMongo
    explain_doc = (
        db[COLLECTION_SAMPLE]
        .find(filter_query, projection)
        .sort(sort_spec)
        .limit(5)
        .explain()
    )
    stats = _parse_explain_stats(explain_doc)

    return {
        "title": "Verificación Directa de Índice Compuesto (.find().sort().limit().explain('executionStats'))",
        "collection": COLLECTION_SAMPLE,
        "description": "Top 5 transacciones de Electronics en North_America con monto neto >= $1,000 usando índice compuesto.",
        "filter": filter_query,
        "client_latency_ms": client_latency_ms,
        "execution_stats": stats,
        "results": docs,
    }


def execute_complex_queries(
    db: Database,
    output_path: Optional[Path] = None,
    config: Optional[AppConfig] = None,
) -> List[Dict[str, Any]]:
    """
    Ejecuta las 3 consultas analíticas complejas + verificación de índice compuesto
    y guarda el reporte legible en `docs/query_results.txt`.
    """
    cfg = config or load_config()
    out_file = output_path or (cfg.docs_dir / "query_results.txt")

    logger.info("=== Ejecutando 3 Consultas Analíticas Complejas en MongoDB Atlas ===")

    # -------------------------------------------------------------------------
    # CONSULTA 1: Resumen Ejecutivo por Región y Categoría (sales_analytics_summary)
    # Usa índice compuesto idx_summary_region_period_rev (region + period + revenue)
    # -------------------------------------------------------------------------
    pipeline_q1 = [
        {
            "$match": {
                "region": {"$in": ["North_America", "Europe", "Latin_America"]},
                "period": {"$gte": "2025-01", "$lte": "2025-12"},
                "metrics.total_revenue": {"$gte": 50000.0},
            }
        },
        {
            "$group": {
                "_id": {"region": "$region", "category": "$category"},
                "total_revenue_usd": {"$sum": "$metrics.total_revenue"},
                "total_units": {"$sum": "$metrics.total_units"},
                "total_transactions": {"$sum": "$metrics.transaction_count"},
                "peak_ticket_usd": {"$max": "$metrics.max_ticket"},
                "avg_discount_pct": {"$avg": "$metrics.avg_discount_pct"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "region": "$_id.region",
                "category": "$_id.category",
                "total_revenue_usd": {"$round": ["$total_revenue_usd", 2]},
                "total_units": 1,
                "total_transactions": 1,
                "weighted_avg_ticket_usd": {
                    "$round": [{"$divide": ["$total_revenue_usd", "$total_transactions"]}, 2]
                },
                "peak_ticket_usd": {"$round": ["$peak_ticket_usd", 2]},
                "avg_discount_pct": {"$round": ["$avg_discount_pct", 4]},
            }
        },
        {"$sort": {"total_revenue_usd": -1}},
        {"$limit": 10},
    ]

    # -------------------------------------------------------------------------
    # CONSULTA 2: Transacciones de Alto Valor por Segmento, Canal y Método de Pago
    # Usa índice compuesto idx_txn_status_segment_net (status + segment + net_amount)
    # -------------------------------------------------------------------------
    pipeline_q2 = [
        {
            "$match": {
                "status": "COMPLETED",
                "customer.segment": {"$in": ["Enterprise", "Premium"]},
                "financials.net_amount": {"$gte": 500.0},
            }
        },
        {
            "$group": {
                "_id": {
                    "segment": "$customer.segment",
                    "channel": "$channel",
                    "payment_method": "$payment_method",
                },
                "high_value_revenue_usd": {"$sum": "$financials.net_amount"},
                "avg_order_value_usd": {"$avg": "$financials.net_amount"},
                "max_order_value_usd": {"$max": "$financials.net_amount"},
                "order_count": {"$sum": 1},
                "avg_units_per_order": {"$avg": "$financials.quantity"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "segment": "$_id.segment",
                "channel": "$_id.channel",
                "payment_method": "$_id.payment_method",
                "order_count": 1,
                "high_value_revenue_usd": {"$round": ["$high_value_revenue_usd", 2]},
                "avg_order_value_usd": {"$round": ["$avg_order_value_usd", 2]},
                "max_order_value_usd": {"$round": ["$max_order_value_usd", 2]},
                "avg_units_per_order": {"$round": ["$avg_units_per_order", 2]},
            }
        },
        {"$sort": {"high_value_revenue_usd": -1}},
        {"$limit": 10},
    ]

    # -------------------------------------------------------------------------
    # CONSULTA 3: Top SKUs y Sensibilidad a Descuentos en Regiones Core
    # Usa índice compuesto idx_txn_region_cat_net (region + category + net_amount)
    # -------------------------------------------------------------------------
    pipeline_q3 = [
        {
            "$match": {
                "customer.region": {"$in": ["North_America", "Europe"]},
                "product.category": {"$in": ["Electronics", "Gaming", "Home_Appliances"]},
                "financials.net_amount": {"$gte": 200.0},
            }
        },
        {
            "$group": {
                "_id": {
                    "category": "$product.category",
                    "sku": "$product.sku",
                },
                "net_revenue_usd": {"$sum": "$financials.net_amount"},
                "gross_revenue_usd": {"$sum": "$financials.gross_amount"},
                "units_sold": {"$sum": "$financials.quantity"},
                "order_count": {"$sum": 1},
                "avg_discount_pct": {"$avg": "$financials.discount_pct"},
            }
        },
        {"$match": {"order_count": {"$gte": 3}}},
        {
            "$project": {
                "_id": 0,
                "category": "$_id.category",
                "sku": "$_id.sku",
                "order_count": 1,
                "units_sold": 1,
                "net_revenue_usd": {"$round": ["$net_revenue_usd", 2]},
                "gross_revenue_usd": {"$round": ["$gross_revenue_usd", 2]},
                "avg_discount_pct": {"$round": ["$avg_discount_pct", 4]},
            }
        },
        {"$sort": {"net_revenue_usd": -1}},
        {"$limit": 12},
    ]

    query_reports = [
        _run_aggregation_with_explain(
            db=db,
            collection_name=COLLECTION_SUMMARY,
            title="CONSULTA 1: Top 10 Combinaciones Región-Categoría por Ingresos Totales",
            description=(
                "Filtro compuesto en sales_analytics_summary por regiones principales, "
                "periodo 2025 y revenue >= $50,000; agrupación por región y categoría con ticket ponderado."
            ),
            pipeline=pipeline_q1,
        ),
        _run_aggregation_with_explain(
            db=db,
            collection_name=COLLECTION_SAMPLE,
            title="CONSULTA 2: Análisis de Transacciones de Alto Valor (Enterprise/Premium >= $500)",
            description=(
                "Filtro compuesto sobre transactions_sample (status COMPLETED, segmentos "
                "Enterprise/Premium y ticket >= $500); agrupación por segmento, canal y método de pago."
            ),
            pipeline=pipeline_q2,
        ),
        _run_aggregation_with_explain(
            db=db,
            collection_name=COLLECTION_SAMPLE,
            title="CONSULTA 3: Top 12 SKUs de Tecnología/Hogar en North_America y Europe",
            description=(
                "Filtro compuesto por región, categoría y monto >= $200; agrupación por SKU "
                "con filtro post-agregación (order_count >= 3), orden descendente y límite."
            ),
            pipeline=pipeline_q3,
        ),
        _run_indexed_find_verification(db=db),
    ]

    _write_formatted_report(query_reports, out_file, cfg.mongo_db_name)
    logger.info("Resultados y estadísticas explain('executionStats') guardados en: %s", out_file)
    return query_reports


def _write_formatted_report(
    reports: List[Dict[str, Any]],
    output_path: Path,
    db_name: str,
) -> None:
    """Genera un archivo de texto estructurado y legible listo para auditoría y anexos del informe."""
    lines: List[str] = [
        "=" * 96,
        "REPORTE DE CONSULTAS ANALÍTICAS COMPLEJAS Y EXPLAIN('executionStats') - MONGODB ATLAS",
        f"Base de Datos : {db_name}",
        f"Fecha (UTC)   : {datetime.now(timezone.utc).isoformat()}",
        "Rol Responsable: DBA NoSQL",
        "=" * 96,
        "",
    ]

    for idx, rep in enumerate(reports, start=1):
        stats = rep["execution_stats"]
        lines.extend(
            [
                "-" * 96,
                f"[{idx}] {rep['title']}",
                f"Colección          : {rep['collection']}",
                f"Descripción        : {rep['description']}",
                f"Latencia Cliente   : {rep['client_latency_ms']} ms",
                f"Server Exec Time   : {stats['server_execution_time_ms']} ms",
                f"Docs Retornados    : {stats['n_returned']}",
                f"Keys Examinadas    : {stats['total_keys_examined']}",
                f"Docs Examinados    : {stats['total_docs_examined']}",
                f"Usa Índice (IXSCAN): {'SÍ' if stats['uses_index'] else 'NO'}",
                f"Plan Ganador       : {stats['winning_plan_stages']}",
                "-" * 96,
                "Resultados (JSON formateado):",
                json.dumps(rep["results"], indent=2, ensure_ascii=False, default=str),
                "",
            ]
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    setup_logging()
    cfg = load_config()
    try:
        with MongoAtlasClient(cfg) as mongo:
            execute_complex_queries(mongo.db, config=cfg)
        return 0
    except (ValueError, ConnectionError, PyMongoError) as exc:
        logger.error("No se pudieron ejecutar las consultas en MongoDB Atlas: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
