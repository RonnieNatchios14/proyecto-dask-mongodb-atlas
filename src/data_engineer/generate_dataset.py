"""
Generador de dataset sintético masivo particionado de transacciones e-commerce.

Rol responsable: Ingeniero de Datos.

Genera múltiples archivos CSV (o Parquet) particionados en `data/raw/` con millones
de transacciones realistas (~1 GB a 2 GB en disco para 5M-10M filas), inyectando
de forma controlada datos sucios (nulos, duplicados y cantidades/precios inválidos)
para validar las etapas de limpieza del pipeline distribuido con Dask.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.cloud_architect.config import AppConfig, load_config, setup_logging

logger = logging.getLogger(__name__)

# Catálogos sintéticos realistas de dominio e-commerce
REGIONS = np.array(
    ["North_America", "Latin_America", "Europe", "Asia_Pacific", "Middle_East"]
)
REGION_PROBS = np.array([0.34, 0.22, 0.24, 0.14, 0.06])

REGION_COUNTRIES: Dict[str, List[str]] = {
    "North_America": ["USA", "CAN", "MEX"],
    "Latin_America": ["SLV", "COL", "BRA", "ARG", "CHL", "PER"],
    "Europe": ["ESP", "DEU", "FRA", "GBR", "ITA", "NLD"],
    "Asia_Pacific": ["JPN", "KOR", "AUS", "SGP", "IND"],
    "Middle_East": ["ARE", "SAU", "QAT", "ISR"],
}

CATEGORIES = np.array(
    [
        "Electronics",
        "Home_Appliances",
        "Fashion",
        "Sports_Outdoors",
        "Books_Media",
        "Health_Beauty",
        "Automotive",
        "Gaming",
    ]
)
CATEGORY_PRICE_RANGES: Dict[str, tuple] = {
    "Electronics": (80.0, 1450.0),
    "Home_Appliances": (65.0, 980.0),
    "Fashion": (18.0, 260.0),
    "Sports_Outdoors": (25.0, 520.0),
    "Books_Media": (9.0, 85.0),
    "Health_Beauty": (12.0, 190.0),
    "Automotive": (30.0, 740.0),
    "Gaming": (45.0, 1200.0),
}
CATEGORY_SKU_PREFIX: Dict[str, str] = {
    "Electronics": "ELEC",
    "Home_Appliances": "HOME",
    "Fashion": "FASH",
    "Sports_Outdoors": "SPRT",
    "Books_Media": "BOOK",
    "Health_Beauty": "HLTH",
    "Automotive": "AUTO",
    "Gaming": "GAME",
}

PAYMENT_METHODS = np.array(
    ["Credit_Card", "Debit_Card", "PayPal", "Bank_Transfer", "Digital_Wallet"]
)
PAYMENT_PROBS = np.array([0.42, 0.24, 0.16, 0.08, 0.10])

CHANNELS = np.array(["Web_Desktop", "Mobile_App", "Marketplace", "Social_Shop"])
CHANNEL_PROBS = np.array([0.38, 0.40, 0.14, 0.08])

STATUSES = np.array(["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"])
STATUS_PROBS = np.array([0.88, 0.04, 0.05, 0.03])

SEGMENTS = np.array(["Enterprise", "Premium", "Regular", "New"])
SEGMENT_PROBS = np.array([0.10, 0.25, 0.45, 0.20])

WAREHOUSES = np.array(
    [
        "WH-US-EAST-01",
        "WH-US-WEST-02",
        "WH-EU-CENT-03",
        "WH-LATAM-04",
        "WH-APAC-05",
        "WH-MENA-06",
    ]
)

CAMPAIGNS = np.array(
    [
        "CAMPAIGN_ORGANIC_DIRECT_TRAFFIC",
        "CAMPAIGN_Q1_WINTER_CLEARANCE_VIP",
        "CAMPAIGN_Q2_SPRING_TECH_FESTIVAL",
        "CAMPAIGN_Q3_SUMMER_FLASH_SAVINGS",
        "CAMPAIGN_Q4_CYBER_WEEK_EXCLUSIVE",
        "CAMPAIGN_LOYALTY_MEMBER_REWARDS",
    ]
)


def _generate_partition_dataframe(
    part_idx: int,
    n_rows: int,
    start_id: int,
    seed: int,
) -> pd.DataFrame:
    """Genera una partición sintética vectorizada con anomalías controladas."""
    rng = np.random.default_rng(seed + part_idx)

    # Reservar ~1% para filas duplicadas realistas (reintentos de ingesta)
    n_duplicates = max(1, int(n_rows * 0.01))
    n_unique = n_rows - n_duplicates

    # IDs de transacción únicos por partición
    txn_nums = np.arange(start_id, start_id + n_unique, dtype=np.int64)
    transaction_ids = np.char.add("TXN-", np.char.zfill(txn_nums.astype(str), 10))

    # Timestamps distribuidos en todo el año 2025
    start_ts = np.datetime64("2025-01-01T00:00:00", "s")
    seconds_in_year = 365 * 24 * 3600
    offsets = rng.integers(0, seconds_in_year, size=n_unique, dtype=np.int64)
    timestamps = (start_ts + offsets.astype("timedelta64[s]")).astype(str)
    timestamps = np.char.replace(timestamps, "T", " ")

    # Clientes (250,000 clientes recurrentes)
    cust_nums = rng.integers(1, 250_001, size=n_unique, dtype=np.int32)
    customer_ids = np.char.add("CUST-", np.char.zfill(cust_nums.astype(str), 7))

    # Región y país consistente con la región
    region_idx = rng.choice(len(REGIONS), size=n_unique, p=REGION_PROBS)
    regions = REGIONS[region_idx].astype(object)
    countries = np.empty(n_unique, dtype=object)
    for idx, reg_name in enumerate(REGIONS):
        mask = region_idx == idx
        count = int(mask.sum())
        if count > 0:
            country_pool = REGION_COUNTRIES[reg_name]
            countries[mask] = rng.choice(country_pool, size=count)

    # Categoría, SKU y precio unitario acorde a la categoría
    cat_idx = rng.choice(len(CATEGORIES), size=n_unique)
    categories = CATEGORIES[cat_idx].astype(object)
    unit_prices = np.empty(n_unique, dtype=np.float64)
    skus = np.empty(n_unique, dtype=object)

    for idx, cat_name in enumerate(CATEGORIES):
        mask = cat_idx == idx
        count = int(mask.sum())
        if count > 0:
            low, high = CATEGORY_PRICE_RANGES[cat_name]
            unit_prices[mask] = np.round(rng.uniform(low, high, size=count), 2)
            sku_nums = rng.integers(1, 500, size=count)
            prefix = f"SKU-{CATEGORY_SKU_PREFIX[cat_name]}-"
            skus[mask] = np.char.add(prefix, np.char.zfill(sku_nums.astype(str), 4))

    payment_methods = rng.choice(PAYMENT_METHODS, size=n_unique, p=PAYMENT_PROBS)
    channels = rng.choice(CHANNELS, size=n_unique, p=CHANNEL_PROBS)
    statuses = rng.choice(STATUSES, size=n_unique, p=STATUS_PROBS)
    segments = rng.choice(SEGMENTS, size=n_unique, p=SEGMENT_PROBS)
    warehouses = rng.choice(WAREHOUSES, size=n_unique)
    campaigns = rng.choice(CAMPAIGNS, size=n_unique)

    quantities = rng.integers(1, 9, size=n_unique, dtype=np.int32).astype(np.float64)
    discount_pcts = np.round(
        rng.choice(
            [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
            size=n_unique,
            p=[0.40, 0.18, 0.16, 0.12, 0.08, 0.04, 0.02],
        ),
        2,
    )
    shipping_costs = np.round(rng.uniform(0.0, 35.0, size=n_unique), 2)

    # Inyección controlada de anomalías de calidad de datos:
    # 1) ~2.0% valores nulos en columnas críticas
    null_price_mask = rng.random(n_unique) < 0.010
    null_cat_mask = rng.random(n_unique) < 0.005
    null_reg_mask = rng.random(n_unique) < 0.005
    unit_prices[null_price_mask] = np.nan
    categories[null_cat_mask] = None
    regions[null_reg_mask] = None

    # 2) ~0.5% valores fuera de dominio (cantidades <= 0 o precios negativos)
    bad_qty_mask = rng.random(n_unique) < 0.003
    bad_price_mask = rng.random(n_unique) < 0.002
    quantities[bad_qty_mask] = rng.choice([-2.0, -1.0, 0.0], size=int(bad_qty_mask.sum()))
    unit_prices[bad_price_mask] = -np.abs(unit_prices[bad_price_mask])

    df = pd.DataFrame(
        {
            "transaction_id": transaction_ids,
            "timestamp": timestamps,
            "customer_id": customer_ids,
            "region": regions,
            "country": countries,
            "category": categories,
            "product_sku": skus,
            "payment_method": payment_methods,
            "channel": channels,
            "status": statuses,
            "quantity": quantities,
            "unit_price": unit_prices,
            "discount_pct": discount_pcts,
            "shipping_cost": shipping_costs,
            "customer_segment": segments,
            "warehouse_code": warehouses,
            "campaign_tag": campaigns,
        }
    )

    # 3) ~1.0% filas duplicadas exactas
    dup_indices = rng.integers(0, n_unique, size=n_duplicates)
    df_dups = df.iloc[dup_indices].copy()
    df_full = pd.concat([df, df_dups], ignore_index=True)

    return df_full


def dataset_exists(raw_dir: Path, file_format: str = "csv") -> bool:
    """Verifica si ya existen particiones generadas en el directorio de datos crudos."""
    pattern = f"transactions_part_*.{file_format}"
    return len(list(raw_dir.glob(pattern))) > 0


def generate_dataset(
    config: Optional[AppConfig] = None,
    total_rows: Optional[int] = None,
    n_partitions: Optional[int] = None,
    file_format: str = "csv",
    overwrite: bool = False,
    seed: int = 42,
) -> Dict[str, object]:
    """
    Genera el dataset sintético particionado en disco.

    Args:
        config: Configuración centralizada de la aplicación.
        total_rows: Total de filas a generar (default: `config.dataset_rows`).
        n_partitions: Número de archivos particionados (default: `config.dataset_partitions`).
        file_format: Formato de salida (`csv` o `parquet`). Default: `csv`.
        overwrite: Si es True, borra archivos previos antes de generar nuevos.
        seed: Semilla determinística para reproducibilidad.

    Returns:
        Metadatos del dataset generado (ruta, archivos, tamaño en MB, duración).
    """
    cfg = config or load_config()
    rows = total_rows or cfg.dataset_rows
    partitions = n_partitions or cfg.dataset_partitions
    file_format = file_format.lower()

    if file_format not in {"csv", "parquet"}:
        raise ValueError(f"Formato no soportado: '{file_format}'. Usa 'csv' o 'parquet'.")

    raw_dir = cfg.raw_data_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    existing_files = sorted(raw_dir.glob(f"transactions_part_*.{file_format}"))
    if existing_files and not overwrite:
        total_bytes = sum(f.stat().st_size for f in existing_files)
        logger.info(
            "Dataset ya existente detectado (%d archivos, %.2f MB). Usa --overwrite para regenerar.",
            len(existing_files),
            total_bytes / (1024 ** 2),
        )
        return {
            "status": "SKIPPED_EXISTING",
            "raw_dir": str(raw_dir),
            "file_format": file_format,
            "files_count": len(existing_files),
            "total_size_mb": round(total_bytes / (1024 ** 2), 2),
        }

    # Limpiar particiones anteriores del mismo formato si overwrite=True
    for old_file in existing_files:
        old_file.unlink()

    logger.info(
        "Iniciando generación de dataset sintético: %s filas en %d particiones (%s)...",
        f"{rows:,}",
        partitions,
        file_format.upper(),
    )
    t0 = time.perf_counter()

    base_rows = rows // partitions
    remainder = rows % partitions
    current_start_id = 1
    generated_files: List[Path] = []
    total_bytes = 0

    for part_idx in range(partitions):
        part_rows = base_rows + (1 if part_idx < remainder else 0)
        df_part = _generate_partition_dataframe(
            part_idx=part_idx,
            n_rows=part_rows,
            start_id=current_start_id,
            seed=seed,
        )
        current_start_id += part_rows

        out_path = raw_dir / f"transactions_part_{part_idx:03d}.{file_format}"
        if file_format == "csv":
            df_part.to_csv(out_path, index=False)
        else:
            df_part.to_parquet(out_path, index=False, engine="pyarrow")

        file_size = out_path.stat().st_size
        total_bytes += file_size
        generated_files.append(out_path)

        if (part_idx + 1) % max(1, partitions // 4) == 0 or (part_idx + 1) == partitions:
            logger.info(
                "Progreso: %d/%d particiones generadas (acumulado: %.2f MB)",
                part_idx + 1,
                partitions,
                total_bytes / (1024 ** 2),
            )

    elapsed = round(time.perf_counter() - t0, 2)
    total_mb = round(total_bytes / (1024 ** 2), 2)
    total_gb = round(total_bytes / (1024 ** 3), 3)

    logger.info(
        "Dataset generado exitosamente: %s filas | %d archivos | %.2f MB (%.3f GB) | %.2f s",
        f"{rows:,}",
        len(generated_files),
        total_mb,
        total_gb,
        elapsed,
    )
    return {
        "status": "GENERATED",
        "raw_dir": str(raw_dir),
        "file_format": file_format,
        "total_rows": rows,
        "files_count": len(generated_files),
        "total_size_mb": total_mb,
        "total_size_gb": total_gb,
        "elapsed_seconds": elapsed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Genera un dataset masivo particionado de transacciones e-commerce (Rol: Ingeniero de Datos)."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Número total de filas a generar (por defecto toma DATASET_ROWS de .env).",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=None,
        help="Cantidad de archivos particionados (por defecto toma DATASET_PARTITIONS de .env).",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Formato de los archivos generados (default: csv).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe particiones existentes si ya fueron generadas.",
    )
    args = parser.parse_args()

    setup_logging()
    cfg = load_config()
    try:
        generate_dataset(
            config=cfg,
            total_rows=args.rows,
            n_partitions=args.partitions,
            file_format=args.format,
            overwrite=args.overwrite,
        )
        return 0
    except Exception as exc:
        logger.exception("Error fatal generando dataset sintético: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
