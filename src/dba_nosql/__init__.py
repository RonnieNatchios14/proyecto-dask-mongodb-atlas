"""
Módulo del DBA NoSQL (MongoDB Atlas).

Rol responsable: DBA NoSQL.
Responsabilidades:
- Gestión segura de conexiones TLS/SRV hacia MongoDB Atlas vía PyMongo.
- Diseño de esquema documental desnormalizado optimizado para analítica.
- Carga masiva eficiente por lotes (`insert_many` con `ordered=False`) e índices compuestos.
- Ejecución de consultas complejas de agregación y análisis de planes con `.explain("executionStats")`.
"""
