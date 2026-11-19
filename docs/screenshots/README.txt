# Carpeta de Evidencias Visuales para el Reporte PDF (docs/REPORTE_TECNICO.pdf)

Coloca aquí tus capturas reales en formato .png, .jpg, .jpeg o .webp con estos
nombres exactos para que el script las incruste automáticamente en el PDF:

1. 01_cloud_compute_aws_gcp.png
   -> Consola AWS EC2 o GCP Compute Engine con la instancia en estado Running,
      vCPU/RAM, región e IP.

2. 02_cloud_network_security.png
   -> Security Groups (AWS) / VPC Firewall (GCP) mostrando salida TCP 27017 y/o
      MongoDB Atlas -> Network Access (IP Allowlist / VPC Peering).

3. 03_mongodb_atlas_cluster.png
   -> Panel Overview / Metrics de MongoDB Atlas con el clúster activo.

4. 04_mongodb_atlas_collections_indexes.png
   -> Vista Browse Collections e Indexes mostrando sales_analytics_summary,
      transactions_sample y los índices compuestos creados.

5. 05_terminal_healthcheck_pipeline.png
   -> Captura de terminal ejecutando healthcheck.py (HEALTHY) y python main.py.

(Cualquier otra imagen .png/.jpg que agregues aquí también se incluirá
automáticamente como "Evidencia Adicional" en el PDF.)

Para regenerar el PDF después de agregar tus capturas ejecuta:
    .venv/bin/python -m src.cloud_architect.export_pdf
