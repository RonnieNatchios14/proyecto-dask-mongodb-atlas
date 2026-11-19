"""
Cliente seguro de conexión a MongoDB Atlas vía PyMongo.

Rol responsable: DBA NoSQL.

Implementa la conexión a MongoDB Atlas usando variables de entorno gestionadas
por el Arquitecto Cloud (`MONGO_URI`, `MONGO_DB_NAME`), `ServerApi('1')`,
timeouts cloud configurables y validación inmediata mediante el comando `ping`.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
)
from pymongo.server_api import ServerApi

from src.cloud_architect.config import AppConfig, load_config, mask_mongo_uri

logger = logging.getLogger(__name__)


class MongoAtlasClient:
    """
    Wrapper seguro y reutilizable para conexiones a MongoDB Atlas.
    Soporta uso como context manager (`with MongoAtlasClient() as client:`).
    """

    def __init__(self, config: Optional[AppConfig] = None) -> None:
        self.config: AppConfig = config or load_config()
        self._client: Optional[MongoClient] = None
        self._db: Optional[Database] = None

    def connect(self) -> Database:
        """
        Establece conexión segura a MongoDB Atlas y valida disponibilidad vía `ping`.

        Returns:
            Instancia de `pymongo.database.Database` lista para operar.
        """
        if self._client is not None and self._db is not None:
            return self._db

        is_valid, msg = self.config.validate_mongo_credentials()
        if not is_valid:
            raise ValueError(msg)

        masked = mask_mongo_uri(self.config.mongo_uri)
        logger.info("Conectando a MongoDB Atlas (%s) | DB: %s ...", masked, self.config.mongo_db_name)

        kwargs = self.config.get_mongo_client_kwargs()
        try:
            self._client = MongoClient(
                self.config.mongo_uri,
                server_api=ServerApi("1"),
                **kwargs,
            )
            ping_ms = self.ping()
            self._db = self._client[self.config.mongo_db_name]
            logger.info(
                "Conexión validada con MongoDB Atlas | Ping: %.2f ms | DB activa: %s",
                ping_ms,
                self.config.mongo_db_name,
            )
            return self._db
        except ConfigurationError as exc:
            self.close()
            raise ConnectionError(
                f"Error de configuración DNS/URI en MongoDB Atlas ({masked}): {exc}. "
                "Verifica dnspython y el formato mongodb+srv://."
            ) from exc
        except ServerSelectionTimeoutError as exc:
            self.close()
            raise ConnectionError(
                f"Timeout al conectar con MongoDB Atlas ({masked}): {exc}. "
                "Revisa Network Access (IP Allowlist / Security Group) en Atlas."
            ) from exc
        except OperationFailure as exc:
            self.close()
            raise ConnectionError(
                f"Fallo de autenticación/permisos en MongoDB Atlas: {exc}. "
                "Verifica usuario, contraseña y rol readWrite en Database Access."
            ) from exc
        except (ConnectionFailure, PyMongoError) as exc:
            self.close()
            raise ConnectionError(f"Error de conexión con MongoDB Atlas: {exc}") from exc

    @property
    def client(self) -> MongoClient:
        """Devuelve el cliente PyMongo activo, conectando si es necesario."""
        if self._client is None:
            self.connect()
        assert self._client is not None
        return self._client

    @property
    def db(self) -> Database:
        """Devuelve la base de datos activa, conectando si es necesario."""
        if self._db is None:
            self.connect()
        assert self._db is not None
        return self._db

    def ping(self) -> float:
        """Ejecuta `admin.command('ping')` y retorna la latencia en milisegundos."""
        if self._client is None:
            raise RuntimeError("Cliente MongoDB no inicializado. Llama primero a connect().")
        t0 = time.perf_counter()
        self._client.admin.command("ping")
        return round((time.perf_counter() - t0) * 1000.0, 2)

    def get_server_summary(self) -> Dict[str, Any]:
        """Obtiene metadatos básicos del clúster (versión del motor MongoDB)."""
        if self._client is None:
            self.connect()
        assert self._client is not None
        info = self._client.server_info()
        return {
            "version": info.get("version", "unknown"),
            "ok": info.get("ok", 0),
        }

    def close(self) -> None:
        """Cierra ordenadamente el pool de conexiones hacia MongoDB Atlas."""
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None
            logger.debug("Pool de conexiones MongoDB Atlas cerrado correctamente.")

    def __enter__(self) -> "MongoAtlasClient":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
