# DSY1107 · Streaming resiliente de ventas (EP5 / EP6)

| Carpeta | Contenido |
| --- | --- |
| [app/app](app/app) | Kafka con dos brokers y ZooKeeper, productor Java y Kafka Streams, Docker Compose, pruebas, scripts de escenarios y evidencias. **Empieza por su [README](app/app/README.md).** |
| [monitoring/monitoring](monitoring/monitoring) | Prometheus, Grafana, Loki y Alloy, con el dashboard *EA3 – Observabilidad integral*, para el despliegue en EC2 de las guías del curso. |

Inicio rápido (Docker Desktop):

```powershell
cd app/app
docker compose -p dsy1107-ep5 --env-file .env.example up -d --build
./scripts/probar-escenarios.ps1 -Proyecto dsy1107-ep5
```

El diagrama de la arquitectura está en [app/app/arquitectura_encargo.mmd](app/app/arquitectura_encargo.mmd).
