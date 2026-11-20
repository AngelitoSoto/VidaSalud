# Plataforma VidaSalud (Versión Cloud AWS - Evaluación Parcial N° 5)

**Asignatura:** Desarrollo Cloud Native I (DSY1107)  
**Institución:** Duoc UC  
**Caso:** Plataforma para la Gestión Unificada de Atenciones Médicas con Arquitectura Dual de Mensajería: **RabbitMQ (Transaccional AMQP)** y **Apache Kafka (Streaming & Analítica en Tiempo Real)**.

---

## 🏗️ Arquitectura de Mensajería y Streaming

La solución incorpora dos sistemas de mensajería desacoplados y complementarios:

### 1. RabbitMQ (Comandos Transaccionales y Notificaciones - Evaluación 4)
* **Broker:** `rabbitmq:3-management` (Puertos `5672` AMQP y `15672` Web UI)
* **Exchanges:** `cmd.topic`, `cmd.direct`, `cmd.dead.dlx` (Dead Letter Exchange)
* **Colas de Negocio:** `q.cmd.email`, `q.cmd.admission`, `q.cmd.record`
* **Colas Dead Letter (DLQ):** `q.cmd.email.dlq`, `q.cmd.admission.dlq`, `q.cmd.record.dlq`
* **Consumo con Confirmación:** `@RabbitListener` con confirmación manual (`basicAck`) y desvío de fallos a DLQ (`basicNack` con `requeue=false`).

### 2. Apache Kafka (Streaming, Telemetría y Analítica en Tiempo Real - Evaluación 5)
* **Zookeeper:** Puerto `2181`
* **Kafka Broker:** Puerto `9092` (Host) y `29092` (Red Docker interna)
* **Kafka-UI:** Puerto `8090` (Acceso web para gestión visual de brokers, tópicos, particiones y consumer groups)
* **Tópicos Configurados:**
  1. `vidasalud.telemetry.appointments`: Stream de eventos clínicos en tiempo real (Particiones: 3, Factor de Replicación: 1, Retención: 7 días, Cleanup Policy: delete).
  2. `vidasalud.audit.logs`: Stream de auditoría operativa y de seguridad (Particiones: 3, Factor de Replicación: 1, Retención: 30 días, Cleanup Policy: delete).
* **Productores Tipados (`ms-vidasalud-appointments`):**
  - Clase `KafkaEventPublisher`: Envío de DTOs tipados (`AppointmentTelemetryEventDTO`, `AuditLogEventDTO`) con serialización `StringSerializer` y `JsonSerializer`.
  - Manejo de callbacks asíncronos (`CompletableFuture`) para trazabilidad de topic, partición y offset.
* **Consumidores con Separación de Dominio (`ms-vidasalud-notify`):**
  - **Consumer Group 1 (`vidasalud-analytics-group`):** Procesa eventos de citas y calcula métricas analíticas en `AnalyticsProcessingService`.
  - **Consumer Group 2 (`vidasalud-audit-group`):** Procesa registros de seguridad y trazabilidad en `AuditProcessingService`.
  - **Manejo de Errores & Resiliencia:** `DefaultErrorHandler` con `FixedBackOff(1000L, 3)` (3 reintentos antes de logging contextual de descarte).
* **Administración Dinámica (`ms-vidasalud-admin`):**
  - Clase `KafkaAdminService` encapsulando `AdminClient`.
  - Endpoints REST para gestión en caliente:
    - `GET /api/admin/kafka/topics`: Lista todos los tópicos del cluster.
    - `POST /api/admin/kafka/topics`: Crea tópicos dinámicos con particiones, réplicas y retención.
    - `DELETE /api/admin/kafka/topics/{name}`: Elimina tópicos.
    - `PUT /api/admin/kafka/topics/{name}/config`: Modifica parámetros de retención o cleanup.
    - `GET /api/admin/kafka/consumer-groups`: Inspecciona el estado y miembros de los consumer groups.

---

## 🌐 Mapeo de Puertos y Servicios

| Componente | Servicio | Puerto Interno | Puerto Host / URL |
| :--- | :--- | :--- | :--- |
| **Zookeeper** | Coordinador Kafka | `2181` | `localhost:2181` |
| **Kafka Broker** | Motor Streaming | `29092` | `localhost:9092` |
| **Kafka-UI** | Consola Web Kafka | `8080` | `http://localhost:8090` |
| **RabbitMQ** | Broker AMQP | `5672` / `15672` | `localhost:5672` / `http://localhost:15672` |
| **BFF** | API Gateway Interno | `8080` | `http://localhost:8080` |
| **ms-appointments** | Citas & Productor Kafka/Rabbit | `8081` | `http://localhost:8081` |
| **ms-catalog** | Catálogo Médico | `8083` | `http://localhost:8083` |
| **ms-notify** | Notificaciones & Consumidor Kafka | `8084` | `http://localhost:8084` |
| **ms-admin** | Administración Dinámica Kafka/Rabbit | `8085` | `http://localhost:8085` |
| **Frontend** | Angular SPA | `80` (en Docker) / `4200` | `http://localhost:4200` |

---

## 🚀 Despliegue y Ejecución

### 1. Iniciar toda la infraestructura con Docker Compose:
```bash
docker compose up -d --build
```

### 2. Verificar los Contenedores en Ejecución:
```bash
docker compose ps
```

### 3. Ejecutar las Pruebas Automatizadas de Kafka:
```powershell
.\probar-kafka.ps1
```

### 4. Ejecutar las Pruebas Automatizadas de RabbitMQ:
```powershell
.\probar-rabbitmq.ps1
```

### 5. Iniciar el Frontend (en modo local):
```bash
cd frontend-vidasalud
npm start
```
Abrir en el navegador: **`http://localhost:4200`** e ingresar a la pestaña **Auditoría** para probar la consola en tiempo real de Kafka y RabbitMQ.