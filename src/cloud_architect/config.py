"""
Gestión centralizada de configuración, variables de entorno y parámetros de red Cloud.

Rol responsable: Arquitecto Cloud.

Este módulo carga y valida la configuración desde el archivo `.env` mediante
`python-dotenv`, aplica buenas prácticas de seguridad (enmascaramiento de
credenciales en logs, validación de URI SRV/TLS) y centraliza los timeouts
recomendados para conectividad desde AWS/GCP hacia MongoDB Atlas.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv

# Raíz del proyecto: src/cloud_architect/config.py -> parents[2]
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
ENV_FILE: Path = PROJECT_ROOT / ".env"

# Cargar variables de entorno desde .env si existe
load_dotenv(dotenv_path=ENV_FILE, override=False)

_URI_PASSWORD_PATTERN = re.compile(r"(mongodb(?:\+srv)?://[^:/?#]+:)([^@]+)(@)")


def mask_mongo_uri(uri: str) -> str:
    """Enmascara la contraseña de una URI de MongoDB para impresión segura en logs."""
    if not uri:
        return "<NO_CONFIGURADA>"
    return _URI_PASSWORD_PATTERN.sub(r"\1****\3", uri)


def setup_logging(level: Optional[str] = None) -> logging.Logger:
    """
    Configura logging estructurado uniforme para todos los módulos del proyecto.

    Args:
        level: Nivel de log opcional (DEBUG, INFO, WARNING, ERROR).

    Returns:
        Logger raíz configurado.
    """
    log_level_name = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    numeric_level = getattr(logging, log_level_name, logging.INFO)

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    root_logger.setLevel(numeric_level)

    # Reducir ruido excesivo de librerías externas en modo INFO
    logging.getLogger("distributed").setLevel(logging.WARNING)
    logging.getLogger("tornado").setLevel(logging.WARNING)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)

    return root_logger


def _parse_int(env_key: str, default: int, minimum: int = 1) -> int:
    """Convierte una variable de entorno a entero con validación de límite inferior."""
    raw_value = os.getenv(env_key)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        parsed = int(raw_value.strip())
        if parsed < minimum:
            raise ValueError(f"{env_key} debe ser >= {minimum}, recibido: {parsed}")
        return parsed
    except ValueError as exc:
        raise ValueError(f"Valor inválido para {env_key}='{raw_value}': {exc}") from exc


@dataclass(frozen=True)
class AppConfig:
    """Configuración inmutable y tipada del proyecto completo."""

    # Rutas locales
    project_root: Path
    data_dir: Path
    raw_data_dir: Path
    docs_dir: Path

    # MongoDB Atlas & Red Cloud
    mongo_uri: str
    mongo_db_name: str
    mongo_batch_size: int
    mongo_sample_size: int
    mongo_server_selection_timeout_ms: int
    mongo_connect_timeout_ms: int
    mongo_socket_timeout_ms: int

    # Clúster Dask
    dask_n_workers: int
    dask_threads_per_worker: int
    dask_memory_limit: str
    dask_blocksize: str
    dask_use_processes: bool
    dask_scheduler_address: Optional[str]

    # Dataset sintético
    dataset_rows: int
    dataset_partitions: int

    # Metadatos Cloud
    cloud_provider: str
    cloud_region: str
    environment: str
    log_level: str

    def validate_mongo_credentials(self) -> Tuple[bool, str]:
        """
        Valida que `MONGO_URI` y `MONGO_DB_NAME` estén configuradas correctamente
        y no contengan placeholders sin reemplazar.
        """
        if not self.mongo_uri:
            return (
                False,
                "La variable MONGO_URI no está definida. Copia .env.example a .env y pega tu URI de MongoDB Atlas.",
            )

        if not (
            self.mongo_uri.startswith("mongodb+srv://")
            or self.mongo_uri.startswith("mongodb://")
        ):
            return (
                False,
                "MONGO_URI debe comenzar con 'mongodb+srv://' (Atlas) o 'mongodb://'.",
            )

        placeholders = ("<usuario>", "<password>", "<cluster>", "<username>", "YOUR_PASSWORD")
        if any(token in self.mongo_uri for token in placeholders):
            return (
                False,
                "MONGO_URI todavía contiene marcadores de ejemplo (<usuario>/<password>/<cluster>). "
                "Reemplázalos en el archivo .env con tus credenciales reales de MongoDB Atlas.",
            )

        if not self.mongo_db_name:
            return False, "La variable MONGO_DB_NAME está vacía."

        return True, "Credenciales de MongoDB Atlas válidas en formato."

    def get_mongo_client_kwargs(self) -> Dict[str, object]:
        """
        Devuelve parámetros de red, TLS y timeouts recomendados para entornos
        AWS/GCP conectándose a MongoDB Atlas.
        """
        return {
            "serverSelectionTimeoutMS": self.mongo_server_selection_timeout_ms,
            "connectTimeoutMS": self.mongo_connect_timeout_ms,
            "socketTimeoutMS": self.mongo_socket_timeout_ms,
            "retryWrites": True,
            "w": "majority",
            "maxPoolSize": 50,
            "minPoolSize": 2,
            "appname": f"dask-mongodb-lab-{self.environment}",
        }

    def ensure_directories(self) -> None:
        """Garantiza la existencia de los directorios locales `data/raw` y `docs`."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.docs_dir.mkdir(parents=True, exist_ok=True)


def load_config(reload_env: bool = False) -> AppConfig:
    """
    Construye y retorna la configuración centralizada de la aplicación.

    Args:
        reload_env: Si es True, recarga el archivo `.env` desde disco.
    """
    if reload_env:
        load_dotenv(dotenv_path=ENV_FILE, override=True)

    cpu_count = os.cpu_count() or 4
    default_workers = min(4, max(2, cpu_count // 2))

    scheduler_addr = os.getenv("DASK_SCHEDULER_ADDRESS", "").strip() or None

    config = AppConfig(
        project_root=PROJECT_ROOT,
        data_dir=PROJECT_ROOT / "data",
        raw_data_dir=PROJECT_ROOT / "data" / "raw",
        docs_dir=PROJECT_ROOT / "docs",
        mongo_uri=os.getenv("MONGO_URI", "").strip(),
        mongo_db_name=os.getenv("MONGO_DB_NAME", "ecommerce_bigdata_analytics").strip(),
        mongo_batch_size=_parse_int("MONGO_BATCH_SIZE", default=2000, minimum=100),
        mongo_sample_size=_parse_int("MONGO_SAMPLE_SIZE", default=50000, minimum=1000),
        mongo_server_selection_timeout_ms=_parse_int(
            "MONGO_SERVER_SELECTION_TIMEOUT_MS", default=10000, minimum=1000
        ),
        mongo_connect_timeout_ms=_parse_int(
            "MONGO_CONNECT_TIMEOUT_MS", default=10000, minimum=1000
        ),
        mongo_socket_timeout_ms=_parse_int(
            "MONGO_SOCKET_TIMEOUT_MS", default=30000, minimum=5000
        ),
        dask_n_workers=_parse_int("DASK_N_WORKERS", default=default_workers, minimum=1),
        dask_threads_per_worker=_parse_int("DASK_THREADS_PER_WORKER", default=2, minimum=1),
        dask_memory_limit=os.getenv("DASK_MEMORY_LIMIT", "1.5GB").strip(),
        dask_blocksize=os.getenv("DASK_BLOCKSIZE", "64MB").strip(),
        dask_use_processes=os.getenv("DASK_USE_PROCESSES", "false").strip().lower() in {"1", "true", "yes"},
        dask_scheduler_address=scheduler_addr,
        dataset_rows=_parse_int("DATASET_ROWS", default=5_000_000, minimum=10_000),
        dataset_partitions=_parse_int("DATASET_PARTITIONS", default=20, minimum=2),
        cloud_provider=os.getenv("CLOUD_PROVIDER", "AWS").strip().upper(),
        cloud_region=os.getenv("CLOUD_REGION", "us-east-1").strip(),
        environment=os.getenv("ENVIRONMENT", "development").strip().lower(),
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
    )
    config.ensure_directories()
    return config
