"""
Script de diagnóstico de infraestructura, clúster Dask y conectividad MongoDB Atlas.

Rol responsable: Arquitecto Cloud.

Verifica tres pilares críticos antes de lanzar pipelines de Big Data:
1. Recursos de cómputo del nodo (CPU física/lógica, RAM disponible, disco libre).
2. Estado y operatividad del clúster Dask (LocalCluster o scheduler distribuido AWS/GCP).
3. Conectividad de red, resolución DNS SRV y latencia de ping hacia MongoDB Atlas.
"""

from __future__ import annotations

import argparse
import json
import logging
import platform
import shutil
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

import psutil
from dask.distributed import Client, LocalCluster

from src.cloud_architect.config import AppConfig, load_config, mask_mongo_uri, setup_logging

logger = logging.getLogger(__name__)


def check_system_resources(config: AppConfig) -> Dict[str, Any]:
    """Inspecciona CPU, RAM y almacenamiento disponible en el nodo de ejecución."""
    vm = psutil.virtual_memory()
    disk = shutil.disk_usage(config.project_root)
    cpu_percent = psutil.cpu_percent(interval=0.25)

    total_ram_gb = round(vm.total / (1024 ** 3), 2)
    available_ram_gb = round(vm.available / (1024 ** 3), 2)
    free_disk_gb = round(disk.free / (1024 ** 3), 2)

    warnings = []
    if available_ram_gb < 1.5:
        warnings.append(
            f"RAM disponible baja ({available_ram_gb} GB). Considera reducir DASK_N_WORKERS o DATASET_ROWS."
        )
    if free_disk_gb < 2.5:
        warnings.append(
            f"Espacio libre en disco reducido ({free_disk_gb} GB). El dataset de ~5M filas requiere ~1 GB."
        )

    status = "WARN" if warnings else "OK"
    result = {
        "status": status,
        "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "python_version": platform.python_version(),
        "cloud_provider": config.cloud_provider,
        "cloud_region": config.cloud_region,
        "cpu_physical_cores": psutil.cpu_count(logical=False) or 1,
        "cpu_logical_cores": psutil.cpu_count(logical=True) or 1,
        "cpu_usage_pct": cpu_percent,
        "ram_total_gb": total_ram_gb,
        "ram_available_gb": available_ram_gb,
        "ram_used_pct": vm.percent,
        "disk_free_gb": free_disk_gb,
        "warnings": warnings,
    }
    logger.info(
        "[System] %s | CPUs: %s lógicos (%s%% uso) | RAM: %.2f/%.2f GB disp. | Disco libre: %.2f GB",
        status,
        result["cpu_logical_cores"],
        cpu_percent,
        available_ram_gb,
        total_ram_gb,
        free_disk_gb,
    )
    for warn in warnings:
        logger.warning("[System] %s", warn)
    return result


def check_dask_cluster(config: AppConfig) -> Dict[str, Any]:
    """
    Valida el arranque y ejecución paralela de un clúster Dask local o
    la conexión a un scheduler distribuido en la nube.
    """
    t0 = time.perf_counter()
    cluster = None
    client = None
    try:
        if config.dask_scheduler_address:
            mode = "distributed_remote"
            client = Client(config.dask_scheduler_address, timeout="10s")
        else:
            mode = "local_cluster"
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

        # Prueba funcional de ejecución paralela en workers
        futures = client.map(lambda x: x * x, range(config.dask_n_workers * 2))
        results = client.gather(futures)
        expected = [x * x for x in range(config.dask_n_workers * 2)]
        if results != expected:
            raise RuntimeError("La verificación de cálculo en workers Dask devolvió resultados inconsistentes.")

        info = client.scheduler_info()
        workers = info.get("workers", {})
        total_threads = sum(w.get("nthreads", 0) for w in workers.values())
        total_mem_gb = round(
            sum(w.get("memory_limit", 0) for w in workers.values()) / (1024 ** 3), 2
        )
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)

        report = {
            "status": "OK",
            "mode": mode,
            "scheduler_address": info.get("address", "N/A"),
            "active_workers": len(workers),
            "total_threads": total_threads,
            "cluster_memory_limit_gb": total_mem_gb,
            "startup_and_ping_ms": elapsed_ms,
        }
        logger.info(
            "[Dask] OK (%s) | Workers: %d | Hilos: %d | Límite RAM clúster: %.2f GB | Latencia check: %.1f ms",
            mode,
            report["active_workers"],
            total_threads,
            total_mem_gb,
            elapsed_ms,
        )
        return report
    except Exception as exc:
        logger.error("[Dask] ERROR al inicializar/validar el clúster: %s", exc)
        return {
            "status": "ERROR",
            "error": str(exc),
        }
    finally:
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()


def check_mongodb_atlas(config: AppConfig, require_mongo: bool = True) -> Dict[str, Any]:
    """
    Valida credenciales y conectividad real contra MongoDB Atlas (TLS + ping).

    Args:
        config: Configuración centralizada.
        require_mongo: Si es False, marca como SKIPPED en lugar de ERROR cuando falta MONGO_URI.
    """
    is_valid, validation_msg = config.validate_mongo_credentials()
    if not is_valid:
        status = "ERROR" if require_mongo else "SKIPPED"
        log_fn = logger.error if require_mongo else logger.warning
        log_fn("[MongoDB Atlas] %s | %s", status, validation_msg)
        return {
            "status": status,
            "uri_masked": mask_mongo_uri(config.mongo_uri),
            "database": config.mongo_db_name,
            "message": validation_msg,
        }

    # Importación local para evitar dependencia circular entre roles
    from src.dba_nosql.mongo_client import MongoAtlasClient

    t0 = time.perf_counter()
    try:
        with MongoAtlasClient(config) as mongo:
            ping_ms = mongo.ping()
            server_info = mongo.get_server_summary()

        total_ms = round((time.perf_counter() - t0) * 1000, 2)
        report = {
            "status": "OK",
            "uri_masked": mask_mongo_uri(config.mongo_uri),
            "database": config.mongo_db_name,
            "ping_latency_ms": ping_ms,
            "handshake_total_ms": total_ms,
            "mongodb_version": server_info.get("version", "unknown"),
        }
        logger.info(
            "[MongoDB Atlas] OK | DB: %s | Versión: %s | Ping: %.2f ms",
            config.mongo_db_name,
            report["mongodb_version"],
            ping_ms,
        )
        return report
    except Exception as exc:
        logger.error(
            "[MongoDB Atlas] ERROR de conexión (%s). Verifica Network Access (IP Allowlist / "
            "Security Groups en AWS/GCP), credenciales y salida por puerto TCP 27017.",
            exc,
        )
        return {
            "status": "ERROR",
            "uri_masked": mask_mongo_uri(config.mongo_uri),
            "database": config.mongo_db_name,
            "error": str(exc),
        }


def run_healthcheck(
    config: Optional[AppConfig] = None,
    check_mongo: bool = True,
    require_mongo: bool = True,
) -> Dict[str, Any]:
    """
    Ejecuta el diagnóstico completo de infraestructura y servicios.

    Returns:
        Diccionario consolidado con estado global (`overall_status`) y detalle por componente.
    """
    cfg = config or load_config()
    logger.info("=== Iniciando Healthcheck de Arquitectura Cloud (%s / %s) ===", cfg.cloud_provider, cfg.environment)

    system_status = check_system_resources(cfg)
    dask_status = check_dask_cluster(cfg)
    mongo_status = (
        check_mongodb_atlas(cfg, require_mongo=require_mongo)
        if check_mongo
        else {"status": "SKIPPED", "message": "Verificación de MongoDB omitida por parámetro."}
    )

    component_statuses = [system_status["status"], dask_status["status"], mongo_status["status"]]
    if "ERROR" in component_statuses:
        overall = "FAILED"
    elif "WARN" in component_statuses or "SKIPPED" in component_statuses:
        overall = "DEGRADED"
    else:
        overall = "HEALTHY"

    summary = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall,
        "system": system_status,
        "dask": dask_status,
        "mongodb_atlas": mongo_status,
    }
    logger.info("=== Resultado Global Healthcheck: %s ===", overall)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Diagnóstico de recursos locales, Dask y MongoDB Atlas (Rol: Arquitecto Cloud)."
    )
    parser.add_argument(
        "--skip-mongo",
        action="store_true",
        help="Omite la prueba de conectividad contra MongoDB Atlas (útil para pruebas locales).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Imprime el reporte completo en formato JSON.",
    )
    args = parser.parse_args()

    setup_logging()
    cfg = load_config()
    report = run_healthcheck(
        config=cfg,
        check_mongo=not args.skip_mongo,
        require_mongo=not args.skip_mongo,
    )

    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))

    return 0 if report["overall_status"] != "FAILED" else 1


if __name__ == "__main__":
    sys.exit(main())
