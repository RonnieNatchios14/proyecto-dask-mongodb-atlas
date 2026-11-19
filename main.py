"""
Orquestador End-to-End de la Práctica Evaluada:
"Integración de Python, Procesamiento Paralelo con Dask y Persistencia NoSQL en MongoDB Atlas".

Roles integrados en el flujo:
1. Arquitecto Cloud (`src.cloud_architect`): Healthcheck de infraestructura, Dask y Atlas.
2. Ingeniero de Datos (`src.data_engineer`): Generación masiva, Benchmark Pandas vs. Dask y ETL distribuido.
3. DBA NoSQL (`src.dba_nosql`): Conexión segura a Atlas, carga masiva con índices y 3 consultas complejas.

Uso típico:
    python main.py
    python main.py --skip-benchmark
    python main.py --skip-mongo
    python main.py --regenerate --rows 6000000
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from src.cloud_architect.config import load_config, setup_logging
from src.cloud_architect.export_pdf import export_report_to_pdf
from src.cloud_architect.healthcheck import run_healthcheck
from src.data_engineer.benchmark import run_benchmark
from src.data_engineer.dask_pipeline import run_full_etl_pipeline
from src.data_engineer.generate_dataset import dataset_exists, generate_dataset
from src.dba_nosql.loader import load_to_mongodb_atlas
from src.dba_nosql.mongo_client import MongoAtlasClient
from src.dba_nosql.queries import execute_complex_queries

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Orquestador End-to-End: Healthcheck -> Generación Big Data -> "
            "Benchmark Pandas vs. Dask -> Carga MongoDB Atlas -> 3 Consultas Complejas."
        )
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Fuerza la regeneración del dataset sintético aunque ya exista en data/raw/.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Sobrescribe DATASET_ROWS al generar datos sintéticos.",
    )
    parser.add_argument(
        "--skip-benchmark",
        action="store_true",
        help="Omite el benchmark Pandas vs. Dask si ya cuentas con docs/benchmark_results.json.",
    )
    parser.add_argument(
        "--skip-mongo",
        action="store_true",
        help="Ejecuta únicamente las fases locales (healthcheck, datos, benchmark y ETL Dask) sin conectar a Atlas.",
    )
    parser.add_argument(
        "--skip-pdf",
        action="store_true",
        help="Omite la exportación automática de docs/REPORTE_TECNICO.pdf al finalizar.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging()
    config = load_config()

    t_global = time.perf_counter()
    logger.info("==========================================================================")
    logger.info("  ORQUESTADOR END-TO-END: PYTHON + DASK PARALELO + MONGODB ATLAS (NoSQL)")
    logger.info("==========================================================================")

    # -------------------------------------------------------------------------
    # FASE 1: Arquitecto Cloud -> Healthcheck de sistema, Dask y MongoDB Atlas
    # -------------------------------------------------------------------------
    logger.info("[FASE 1/5] Ejecutando Healthcheck de infraestructura (Arquitecto Cloud)...")
    health = run_healthcheck(
        config=config,
        check_mongo=not args.skip_mongo,
        require_mongo=not args.skip_mongo,
    )
    if health["overall_status"] == "FAILED":
        logger.error(
            "Healthcheck fallido. Corrige los errores reportados (revisa tu archivo .env y "
            "Network Access en MongoDB Atlas) o ejecuta con --skip-mongo para modo local."
        )
        return 1

    # -------------------------------------------------------------------------
    # FASE 2: Ingeniero de Datos -> Generación del dataset si no existe
    # -------------------------------------------------------------------------
    logger.info("[FASE 2/5] Verificando dataset particionado en data/raw/ (Ingeniero de Datos)...")
    if args.regenerate or not dataset_exists(config.raw_data_dir, "csv"):
        generate_dataset(
            config=config,
            total_rows=args.rows,
            file_format="csv",
            overwrite=args.regenerate,
        )
    else:
        logger.info("Dataset particionado encontrado en %s. Continuando sin regenerar.", config.raw_data_dir)

    # -------------------------------------------------------------------------
    # FASE 3: Ingeniero de Datos -> Benchmark comparativo Pandas vs. Dask
    # -------------------------------------------------------------------------
    if args.skip_benchmark:
        logger.info("[FASE 3/5] Benchmark omitido por parámetro --skip-benchmark.")
    else:
        logger.info("[FASE 3/5] Ejecutando Benchmark Pandas vs. Dask (Ingeniero de Datos)...")
        run_benchmark(config)

    # -------------------------------------------------------------------------
    # FASE 4: Ingeniero de Datos + DBA NoSQL -> ETL Dask y Carga Masiva a Atlas
    # -------------------------------------------------------------------------
    logger.info("[FASE 4/5] Ejecutando Pipeline ETL Dask para generar cargas analíticas...")
    summary_df, sample_df, etl_metrics = run_full_etl_pipeline(config)

    if args.skip_mongo:
        logger.info("[FASE 4-5/5] Carga y consultas en MongoDB Atlas omitidas (--skip-mongo).")
        if not args.skip_pdf:
            try:
                export_report_to_pdf(config)
            except Exception as pdf_exc:
                logger.warning("No se pudo regenerar docs/REPORTE_TECNICO.pdf: %s", pdf_exc)
        logger.info("Flujo local completado en %.2f s.", time.perf_counter() - t_global)
        return 0

    logger.info("[FASE 4/5] Cargando documentos e índices compuestos en MongoDB Atlas (DBA NoSQL)...")
    try:
        with MongoAtlasClient(config) as mongo:
            load_to_mongodb_atlas(
                db=mongo.db,
                summary_df=summary_df,
                sample_df=sample_df,
                etl_metrics=etl_metrics,
                config=config,
                replace_existing=True,
            )

            # -----------------------------------------------------------------
            # FASE 5: DBA NoSQL -> Ejecución de 3 consultas complejas + explain
            # -----------------------------------------------------------------
            logger.info("[FASE 5/5] Ejecutando 3 consultas complejas con explain('executionStats') (DBA NoSQL)...")
            execute_complex_queries(db=mongo.db, config=config)

    except Exception as exc:
        logger.exception("Error durante la persistencia o consulta en MongoDB Atlas: %s", exc)
        return 1

    if not args.skip_pdf:
        try:
            export_report_to_pdf(config)
        except Exception as pdf_exc:
            logger.warning("No se pudo regenerar docs/REPORTE_TECNICO.pdf: %s", pdf_exc)

    total_time = round(time.perf_counter() - t_global, 2)
    logger.info("==========================================================================")
    logger.info("  PIPELINE END-TO-END COMPLETADO EXITOSAMENTE EN %.2f SEGUNDOS", total_time)
    logger.info("  Artefactos generados:")
    logger.info("    - docs/benchmark_results.json")
    logger.info("    - docs/benchmark_chart.png")
    logger.info("    - docs/query_results.txt")
    logger.info("    - docs/REPORTE_TECNICO.pdf")
    logger.info("==========================================================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
