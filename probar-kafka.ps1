# Script de Verificación Rápida de Apache Kafka - Evaluación Parcial 5
param(
    [string]$TargetHost = "3.215.109.69"
)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "   VIDASALUD - VERIFICACION DE STREAMING APACHE KAFKA    " -ForegroundColor Yellow
Write-Host "   Host Objetivo: $TargetHost" -ForegroundColor Gray
Write-Host "==========================================================" -ForegroundColor Cyan

$baseUrlAdmin = "http://${TargetHost}:8085"
$baseUrlAppt = "http://${TargetHost}:8081"
$baseUrlBff = "http://${TargetHost}:8080"

# 1. Probar Creación Dinámica de Tópico en ms-vidasalud-admin (8085 o vía BFF 8080)
$topicSuffix = (Get-Date -Format "mmss")
$testTopicName = "vidasalud.telemetry.demo.$topicSuffix"
Write-Host "`n[1/6] Creando Tópico Dinámico '$testTopicName' en ms-vidasalud-admin..." -ForegroundColor White
$createTopicBody = @{
    name = $testTopicName
    partitions = 3
    replicationFactor = 1
    retentionMs = 604800000
    cleanupPolicy = "delete"
} | ConvertTo-Json

try {
    $resTopic = Invoke-RestMethod -Uri "$baseUrlBff/api/admin/kafka/topics" -Method Post -ContentType "application/json" -Body $createTopicBody
    Write-Host "  -> Exito (vía BFF 8080): $($resTopic.message)" -ForegroundColor Green
} catch {
    try {
        $resTopic = Invoke-RestMethod -Uri "$baseUrlAdmin/api/admin/kafka/topics" -Method Post -ContentType "application/json" -Body $createTopicBody
        Write-Host "  -> Exito (directo Admin 8085): $($resTopic.message)" -ForegroundColor Green
    } catch {
        Write-Host "  -> Error creando topico: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# 2. Consultar Listado de Tópicos en el Cluster
Write-Host "`n[2/6] Consultando Tópicos Existentes en el Cluster Kafka..." -ForegroundColor White
try {
    $topics = Invoke-RestMethod -Uri "$baseUrlBff/api/admin/kafka/topics" -Method Get
    foreach ($t in $topics) {
        Write-Host "  -> Topico: $($t.name) | Particiones: $($t.partitions) | Replicas: $($t.replicationFactor)" -ForegroundColor Gray
    }
} catch {
    try {
        $topics = Invoke-RestMethod -Uri "$baseUrlAdmin/api/admin/kafka/topics" -Method Get
        foreach ($t in $topics) {
            Write-Host "  -> Topico: $($t.name) | Particiones: $($t.partitions) | Replicas: $($t.replicationFactor)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "  -> Error consultando topicos: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# 3. Probar Publicación de Stream de Telemetría Clínica (vidasalud.telemetry.appointments)
Write-Host "`n[3/6] Publicando Stream de Telemetria Clinica..." -ForegroundColor White
try {
    $bodyTel = @{
        patientName = "Paciente Streaming Demo AWS"
        specialty = "Medicina General"
        status = "CONFIRMADA"
    } | ConvertTo-Json
    $resTel = Invoke-RestMethod -Uri "$baseUrlBff/api/appointments/kafka/publish-telemetry" -Method Post -ContentType "application/json" -Body $bodyTel
    Write-Host "  -> Exito: $($resTel.message)" -ForegroundColor Green
    Write-Host "  -> Topico: $($resTel.topic) | EventId: $($resTel.event.eventId)" -ForegroundColor Gray
} catch {
    try {
        $resTel = Invoke-RestMethod -Uri "$baseUrlAppt/api/appointments/kafka/publish-telemetry" -Method Post -ContentType "application/json" -Body $bodyTel
        Write-Host "  -> Exito (Directo 8081): $($resTel.message)" -ForegroundColor Green
        Write-Host "  -> Topico: $($resTel.topic) | EventId: $($resTel.event.eventId)" -ForegroundColor Gray
    } catch {
        Write-Host "  -> Error publicando telemetria: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# 4. Probar Publicación de Stream de Auditoría de Seguridad (vidasalud.audit.logs)
Write-Host "`n[4/6] Publicando Stream de Auditoria de Seguridad..." -ForegroundColor White
try {
    $bodyAudit = @{
        userEmail = "admin.seguridad@vidasalud.cl"
        action = "ACCESS_STREAMING_DASHBOARD"
        details = "Prueba automatizada de stream de auditoria"
    } | ConvertTo-Json
    $resAudit = Invoke-RestMethod -Uri "$baseUrlBff/api/appointments/kafka/publish-audit" -Method Post -ContentType "application/json" -Body $bodyAudit
    Write-Host "  -> Exito: $($resAudit.message)" -ForegroundColor Green
    Write-Host "  -> Topico: $($resAudit.topic) | EventId: $($resAudit.event.eventId)" -ForegroundColor Gray
} catch {
    try {
        $resAudit = Invoke-RestMethod -Uri "$baseUrlAppt/api/appointments/kafka/publish-audit" -Method Post -ContentType "application/json" -Body $bodyAudit
        Write-Host "  -> Exito (Directo 8081): $($resAudit.message)" -ForegroundColor Green
        Write-Host "  -> Topico: $($resAudit.topic) | EventId: $($resAudit.event.eventId)" -ForegroundColor Gray
    } catch {
        Write-Host "  -> Error publicando auditoria: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# 5. Probar Simulación de Error y Control de Reintentos (BackOff)
Write-Host "`n[5/6] Disparando Evento de Error Simulado para Probar Reintentos (DefaultErrorHandler)..." -ForegroundColor White
try {
    $resErr = Invoke-RestMethod -Uri "$baseUrlBff/api/appointments/kafka/publish-error" -Method Post
    Write-Host "  -> Exito al emitir evento de fallo: $($resErr.message)" -ForegroundColor Yellow
} catch {
    try {
        $resErr = Invoke-RestMethod -Uri "$baseUrlAppt/api/appointments/kafka/publish-error" -Method Post
        Write-Host "  -> Exito al emitir evento de fallo (Directo 8081): $($resErr.message)" -ForegroundColor Yellow
    } catch {
        Write-Host "  -> Error disparando prueba de fallo: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# 6. Consultar Consumer Groups Registrados
Write-Host "`n[6/6] Consultando Consumer Groups Registrados..." -ForegroundColor White
try {
    $groups = Invoke-RestMethod -Uri "$baseUrlBff/api/admin/kafka/consumer-groups" -Method Get
    foreach ($g in $groups) {
        Write-Host "  -> Grupo: $($g.groupId) | Estado: $($g.state)" -ForegroundColor Gray
    }
} catch {
    try {
        $groups = Invoke-RestMethod -Uri "$baseUrlAdmin/api/admin/kafka/consumer-groups" -Method Get
        foreach ($g in $groups) {
            Write-Host "  -> Grupo: $($g.groupId) | Estado: $($g.state)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "  -> Error consultando consumer groups: $($_.Exception.Message)" -ForegroundColor Red
    }
}

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "Verificacion completada exitosamente." -ForegroundColor Green
Write-Host "Consola Kafka-UI disponible en: http://${TargetHost}:8090" -ForegroundColor Yellow
Write-Host "Consola RabbitMQ disponible en: http://${TargetHost}:15672" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Cyan
