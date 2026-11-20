import { Component, ChangeDetectorRef, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../../environments/environment';

export interface AuditLog {
  id: string;
  timestamp: string;
  usuario: string;
  rol: string;
  evento: string;
  atencionId: string;
  origen: string;
}

export interface OperationResult {
  timestamp: string;
  action: string;
  status: 'SUCCESS' | 'ERROR' | 'PENDING';
  message: string;
  details?: any;
}

export interface KafkaTopicItem {
  name: string;
  partitions: number;
  replicationFactor: number;
  isInternal: boolean;
}

export interface KafkaGroupItem {
  groupId: string;
  state: string;
}

@Component({
  selector: 'app-audit',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './audit.html',
  styleUrl: './audit.css'
})
export class AuditComponent implements OnInit {
  searchTerm: string = '';

  // RabbitMQ State
  customQueueName: string = 'q.cmd.prueba.eval3';
  customEventMessage: string = '';
  isLoadingRabbit: boolean = false;
  lastRabbitResult: OperationResult | null = null;

  // Kafka State (Evaluación 5)
  isLoadingKafka: boolean = false;
  lastKafkaResult: OperationResult | null = null;
  kafkaTopics: KafkaTopicItem[] = [];
  kafkaConsumerGroups: KafkaGroupItem[] = [];
  newKafkaTopicName: string = 'vidasalud.telemetry.urgencias';
  newKafkaTopicPartitions: number = 3;
  newKafkaTopicReplication: number = 1;
  newKafkaTopicRetentionDays: number = 7;

  public get ec2Host(): string {
    const raw = (environment as any).ec2DirectUrl || environment.apiGatewayUrl || 'http://3.215.109.69:8080';
    const match = raw.match(/https?:\/\/([^:/]+)/);
    return match ? match[1] : '3.215.109.69';
  }

  private get gatewayUrl(): string {
    // Apunta al BFF en AWS EC2 (tanto si corre en local como en remoto)
    return (environment as any).ec2DirectUrl || environment.apiGatewayUrl || 'http://3.215.109.69:8080';
  }

  private get ec2Url(): string {
    return (environment as any).ec2DirectUrl || 'http://3.215.109.69:8080';
  }

  logs: AuditLog[] = [
    { id: 'LOG-8801', timestamp: '2026-09-13 18:22:10', usuario: 'recepcion.maipu@vidasalud.cl', rol: 'Recepcionista', evento: 'LLAMADO_A_BOX', atencionId: '#1', origen: 'BFF Gateway' },
    { id: 'LOG-8802', timestamp: '2026-09-13 18:15:04', usuario: 'carolina.mendoza@gmail.com', rol: 'Paciente', evento: 'CREACION_ATENCION', atencionId: '#1', origen: 'Web Portal' },
    { id: 'LOG-8803', timestamp: '2026-09-13 18:00:45', usuario: 'admin.general@vidasalud.cl', rol: 'Admin', evento: 'ACTUALIZACION_ARANCEL', atencionId: 'N/A', origen: 'BFF Gateway' },
    { id: 'LOG-8804', timestamp: '2026-09-13 17:45:12', usuario: 'dr.arancibia@vidasalud.cl', rol: 'Médico', evento: 'CIERRE_ATENCION', atencionId: '#4', origen: 'Box Terminal' }
  ];

  constructor(
    private http: HttpClient,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.loadKafkaTopics();
    this.loadKafkaConsumerGroups();
  }

  get filteredLogs(): AuditLog[] {
    if (!this.searchTerm) return this.logs;
    const term = this.searchTerm.toLowerCase();
    return this.logs.filter(l => 
      l.usuario.toLowerCase().includes(term) || 
      l.evento.toLowerCase().includes(term) || 
      l.atencionId.toLowerCase().includes(term)
    );
  }

  private getErrorMessage(err: any): string {
    if (err?.error?.message) return err.error.message;
    if (typeof err?.error === 'string' && err.error.length < 250) return err.error;
    if (err?.message) return err.message;
    return 'Error de comunicación con el servicio.';
  }

  // =========================================================================
  // SECCIÓN 1: PRUEBAS DE MENSAJERÍA RABBITMQ (EVALUACIÓN 4)
  // =========================================================================

  publishEvent(flow: string, isError: boolean = false): void {
    this.isLoadingRabbit = true;
    const msg = isError ? 'ERROR_SIMULADO' : (this.customEventMessage || 'Notificación automática de cita médica');
    const url = `${this.gatewayUrl}/api/appointments/test-publish?flow=${flow}&message=${encodeURIComponent(msg)}`;

    this.http.post<any>(url, {}).subscribe({
      next: (res) => {
        this.isLoadingRabbit = false;
        this.lastRabbitResult = {
          timestamp: new Date().toLocaleTimeString(),
          action: `Publicar a '${flow}' (${isError ? 'Simulación Fallo -> DLQ' : 'Flujo Normal -> ACK'})`,
          status: 'SUCCESS',
          message: isError 
            ? 'Mensaje con ERROR enviado. El consumidor rechazará con NACK (sin requeue) y se enrutará al DLQ q.cmd.' + flow + '.dlq.' 
            : 'Mensaje publicado exitosamente. El consumidor responderá con ACK manual.',
          details: res
        };

        this.logs.unshift({
          id: 'RBT-' + Math.floor(1000 + Math.random() * 9000),
          timestamp: new Date().toISOString().replace('T', ' ').substring(0, 19),
          usuario: 'sistema.rabbitmq@vidasalud.cl',
          rol: 'RabbitMQ Consumer',
          evento: isError ? `NACK_TO_DLQ (${flow.toUpperCase()})` : `ACK_OK (${flow.toUpperCase()})`,
          atencionId: res?.evento?.id ? `#${res.evento.id}` : '#TEST',
          origen: `Exchange: ${isError ? 'cmd.dead.dlx' : 'cmd.topic'}`
        });

        this.cdr.detectChanges();
      },
      error: (err) => {
        if (err.status && err.status !== 0) {
          this.isLoadingRabbit = false;
          this.lastRabbitResult = {
            timestamp: new Date().toLocaleTimeString(),
            action: `Publicar a '${flow}'`,
            status: 'ERROR',
            message: this.getErrorMessage(err),
            details: err
          };
          this.cdr.detectChanges();
          return;
        }

        const directUrl = `http://${this.ec2Host}:8081/api/appointments/test-publish?flow=${flow}&message=${encodeURIComponent(msg)}`;
        this.http.post<any>(directUrl, {}).subscribe({
          next: (res) => {
            this.isLoadingRabbit = false;
            this.lastRabbitResult = {
              timestamp: new Date().toLocaleTimeString(),
              action: `Publicar a '${flow}' (Directo Local)`,
              status: 'SUCCESS',
              message: 'Mensaje publicado en RabbitMQ local.',
              details: res
            };
            this.cdr.detectChanges();
          },
          error: (err2) => {
            this.isLoadingRabbit = false;
            this.lastRabbitResult = {
              timestamp: new Date().toLocaleTimeString(),
              action: `Publicar a '${flow}'`,
              status: 'ERROR',
              message: this.getErrorMessage(err2),
              details: err2
            };
            this.cdr.detectChanges();
          }
        });
      }
    });
  }

  createDynamicQueue(): void {
    if (!this.customQueueName) return;
    this.isLoadingRabbit = true;

    const body = {
      name: this.customQueueName.trim(),
      durable: true,
      exclusive: false,
      autoDelete: false
    };

    const url = `${this.gatewayUrl}/api/admin/queues`;
    this.http.post<any>(url, body).subscribe({
      next: (res) => {
        this.isLoadingRabbit = false;
        this.lastRabbitResult = {
          timestamp: new Date().toLocaleTimeString(),
          action: `Crear Cola '${body.name}'`,
          status: 'SUCCESS',
          message: res?.message || `Cola '${body.name}' creada exitosamente en RabbitMQ.`,
          details: res
        };
        this.cdr.detectChanges();
      },
      error: (err) => {
        if (err.status && err.status !== 0) {
          this.isLoadingRabbit = false;
          this.lastRabbitResult = {
            timestamp: new Date().toLocaleTimeString(),
            action: `Crear Cola '${body.name}'`,
            status: 'ERROR',
            message: this.getErrorMessage(err),
            details: err
          };
          this.cdr.detectChanges();
          return;
        }

        this.http.post<any>(`http://${this.ec2Host}:8085/api/admin/queues`, body).subscribe({
          next: (res) => {
            this.isLoadingRabbit = false;
            this.lastRabbitResult = {
              timestamp: new Date().toLocaleTimeString(),
              action: `Crear Cola (Local)`,
              status: 'SUCCESS',
              message: res?.message || `Cola '${body.name}' creada exitosamente.`,
              details: res
            };
            this.cdr.detectChanges();
          },
          error: (err2) => {
            this.isLoadingRabbit = false;
            this.lastRabbitResult = {
              timestamp: new Date().toLocaleTimeString(),
              action: `Crear Cola '${body.name}'`,
              status: 'ERROR',
              message: this.getErrorMessage(err2),
              details: err2
            };
            this.cdr.detectChanges();
          }
        });
      }
    });
  }

  deleteDynamicQueue(): void {
    if (!this.customQueueName) return;
    this.isLoadingRabbit = true;
    const qName = encodeURIComponent(this.customQueueName.trim());

    this.http.delete<any>(`${this.gatewayUrl}/api/admin/queues/${qName}`).subscribe({
      next: (res) => {
        this.isLoadingRabbit = false;
        this.lastRabbitResult = {
          timestamp: new Date().toLocaleTimeString(),
          action: `Eliminar Cola '${this.customQueueName}'`,
          status: 'SUCCESS',
          message: res?.message || `Cola eliminada exitosamente en RabbitMQ.`,
          details: res
        };
        this.cdr.detectChanges();
      },
      error: (err) => {
        if (err.status && err.status !== 0) {
          this.isLoadingRabbit = false;
          this.lastRabbitResult = {
            timestamp: new Date().toLocaleTimeString(),
            action: `Eliminar Cola '${this.customQueueName}'`,
            status: 'ERROR',
            message: this.getErrorMessage(err),
            details: err
          };
          this.cdr.detectChanges();
          return;
        }

        this.http.delete<any>(`http://${this.ec2Host}:8085/api/admin/queues/${qName}`).subscribe({
          next: (res) => {
            this.isLoadingRabbit = false;
            this.lastRabbitResult = {
              timestamp: new Date().toLocaleTimeString(),
              action: `Eliminar Cola (Local)`,
              status: 'SUCCESS',
              message: res?.message || `Cola eliminada exitosamente.`,
              details: res
            };
            this.cdr.detectChanges();
          },
          error: (err2) => {
            this.isLoadingRabbit = false;
            this.lastRabbitResult = {
              timestamp: new Date().toLocaleTimeString(),
              action: `Eliminar Cola '${this.customQueueName}'`,
              status: 'ERROR',
              message: this.getErrorMessage(err2),
              details: err2
            };
            this.cdr.detectChanges();
          }
        });
      }
    });
  }

  openRabbitMqManagement(): void {
    const host = this.ec2Host || '3.215.109.69';
    window.open(`http://${host}:15672`, '_blank');
  }

  // =========================================================================
  // SECCIÓN 2: STREAMING & ANALÍTICA APACHE KAFKA (EVALUACIÓN 5)
  // =========================================================================

  publishKafkaTelemetry(): void {
    this.isLoadingKafka = true;
    const body = {
      patientName: 'Paciente Streaming Telemetría',
      specialty: 'Medicina General',
      status: 'CONFIRMADA'
    };

    const url = `${this.gatewayUrl}/api/appointments/kafka/publish-telemetry`;
    this.http.post<any>(url, body).subscribe({
      next: (res) => this.handleKafkaPublishSuccess('Telemetría Citas (vidasalud.telemetry.appointments)', res, 'KAFKA_STREAM (TELEMETRY)', 'vidasalud-analytics-group'),
      error: (err) => {
        if (err.status && err.status !== 0) {
          this.handleKafkaError('Publicar Telemetría Kafka', err);
          return;
        }
        this.http.post<any>(`http://${this.ec2Host}:8081/api/appointments/kafka/publish-telemetry`, body).subscribe({
          next: (res) => this.handleKafkaPublishSuccess('Telemetría Citas (Local Appointments)', res, 'KAFKA_STREAM (TELEMETRY)', 'vidasalud-analytics-group'),
          error: (directErr) => this.handleKafkaError('Publicar Telemetría Kafka', directErr)
        });
      }
    });
  }

  publishKafkaAudit(): void {
    this.isLoadingKafka = true;
    const body = {
      userEmail: 'admin.seguridad@vidasalud.cl',
      action: 'AUDIT_SECURITY_STREAM',
      details: 'Transacción analítica capturada en tiempo real en Kafka'
    };

    const url = `${this.gatewayUrl}/api/appointments/kafka/publish-audit`;
    this.http.post<any>(url, body).subscribe({
      next: (res) => this.handleKafkaPublishSuccess('Auditoría Streaming (vidasalud.audit.logs)', res, 'KAFKA_AUDIT (SECURITY)', 'vidasalud-audit-group'),
      error: (err) => {
        if (err.status && err.status !== 0) {
          this.handleKafkaError('Publicar Auditoría Kafka', err);
          return;
        }
        this.http.post<any>(`http://${this.ec2Host}:8081/api/appointments/kafka/publish-audit`, body).subscribe({
          next: (res) => this.handleKafkaPublishSuccess('Auditoría Streaming (Local Appointments)', res, 'KAFKA_AUDIT (SECURITY)', 'vidasalud-audit-group'),
          error: (directErr) => this.handleKafkaError('Publicar Auditoría Kafka', directErr)
        });
      }
    });
  }

  publishKafkaError(): void {
    this.isLoadingKafka = true;
    const url = `${this.gatewayUrl}/api/appointments/kafka/publish-error`;
    this.http.post<any>(url, {}).subscribe({
      next: (res) => this.handleKafkaPublishSuccess('Simulación Fallo Kafka (Test Reintentos / BackOff)', res, 'KAFKA_RETRY_ERROR', 'vidasalud-analytics-group'),
      error: (err) => {
        if (err.status && err.status !== 0) {
          this.handleKafkaError('Simular Error Kafka', err);
          return;
        }
        this.http.post<any>(`http://${this.ec2Host}:8081/api/appointments/kafka/publish-error`, {}).subscribe({
          next: (res) => this.handleKafkaPublishSuccess('Simulación Fallo Kafka (Local Appointments)', res, 'KAFKA_RETRY_ERROR', 'vidasalud-analytics-group'),
          error: (directErr) => this.handleKafkaError('Simular Error Kafka', directErr)
        });
      }
    });
  }

  private handleKafkaPublishSuccess(action: string, res: any, eventType: string, group: string): void {
    this.isLoadingKafka = false;
    this.lastKafkaResult = {
      timestamp: new Date().toLocaleTimeString(),
      action: action,
      status: 'SUCCESS',
      message: res?.message || 'Evento publicado en broker Apache Kafka con serialización JSON tipada.',
      details: res
    };

    this.logs.unshift({
      id: 'KFK-' + Math.floor(1000 + Math.random() * 9000),
      timestamp: new Date().toISOString().replace('T', ' ').substring(0, 19),
      usuario: 'kafka.producer@vidasalud.cl',
      rol: 'Kafka Stream',
      evento: eventType,
      atencionId: res?.event?.appointmentId ? `#${res.event.appointmentId}` : '#STREAM',
      origen: `Topic: ${res?.topic || 'Kafka'} (Grupo: ${group})`
    });

    this.cdr.detectChanges();
  }

  private handleKafkaError(action: string, err: any): void {
    this.isLoadingKafka = false;
    this.lastKafkaResult = {
      timestamp: new Date().toLocaleTimeString(),
      action: action,
      status: 'ERROR',
      message: this.getErrorMessage(err),
      details: err
    };
    this.cdr.detectChanges();
  }

  loadKafkaTopics(): void {
    this.http.get<KafkaTopicItem[]>(`${this.gatewayUrl}/api/admin/kafka/topics`).subscribe({
      next: (topics) => {
        this.kafkaTopics = topics;
        this.cdr.detectChanges();
      },
      error: (err) => {
        if (err.status && err.status !== 0) {
          console.warn('Error HTTP al cargar tópicos Kafka:', err);
          return;
        }
        this.http.get<KafkaTopicItem[]>(`http://${this.ec2Host}:8085/api/admin/kafka/topics`).subscribe({
          next: (topics) => {
            this.kafkaTopics = topics;
            this.cdr.detectChanges();
          },
          error: (e) => console.warn('No se pudieron cargar tópicos Kafka:', e)
        });
      }
    });
  }

  loadKafkaConsumerGroups(): void {
    this.http.get<KafkaGroupItem[]>(`${this.gatewayUrl}/api/admin/kafka/consumer-groups`).subscribe({
      next: (groups) => {
        this.kafkaConsumerGroups = groups;
        this.cdr.detectChanges();
      },
      error: (err) => {
        if (err.status && err.status !== 0) {
          console.warn('Error HTTP al cargar consumer groups:', err);
          return;
        }
        this.http.get<KafkaGroupItem[]>(`http://${this.ec2Host}:8085/api/admin/kafka/consumer-groups`).subscribe({
          next: (groups) => {
            this.kafkaConsumerGroups = groups;
            this.cdr.detectChanges();
          },
          error: (e) => console.warn('No se pudieron cargar consumer groups:', e)
        });
      }
    });
  }

  createKafkaTopic(): void {
    if (!this.newKafkaTopicName) return;
    const topicName = this.newKafkaTopicName.trim();

    // Validación preventiva en frontend: no permitir crear tópicos duplicados
    if (this.kafkaTopics.some(t => t.name.toLowerCase() === topicName.toLowerCase())) {
      this.handleKafkaError('Crear Tópico Kafka', {
        message: `El tópico '${topicName}' ya existe en el cluster de Kafka. Por favor ingresa otro nombre o elimínalo primero.`
      });
      return;
    }

    this.isLoadingKafka = true;
    const payload = {
      name: topicName,
      partitions: this.newKafkaTopicPartitions,
      replicationFactor: this.newKafkaTopicReplication,
      retentionMs: this.newKafkaTopicRetentionDays * 86400000,
      cleanupPolicy: 'delete'
    };

    // Actualización optimista instantánea (0 ms de espera visual)
    const newTopicItem: KafkaTopicItem = {
      name: payload.name,
      partitions: payload.partitions,
      replicationFactor: payload.replicationFactor,
      isInternal: false
    };

    const url = `${this.gatewayUrl}/api/admin/kafka/topics`;
    this.http.post<any>(url, payload).subscribe({
      next: (res) => {
        this.kafkaTopics = [newTopicItem, ...this.kafkaTopics.filter(t => t.name !== newTopicItem.name)];
        this.handleKafkaSuccess(`Crear Tópico '${payload.name}'`, res?.message || 'Tópico creado exitosamente.');
        this.newKafkaTopicName = 'vidasalud.telemetry.' + Math.floor(100 + Math.random() * 900);
        this.cdr.detectChanges();

        // Sincronizar en segundo plano con el cluster tras propagación
        setTimeout(() => this.loadKafkaTopics(), 1200);
      },
      error: (err) => {
        if (err.status && err.status !== 0) {
          this.handleKafkaError('Crear Tópico Kafka', err);
          return;
        }

        // Fallback únicamente si el BFF no estuvo alcanzable (status: 0)
        this.http.post<any>(`http://${this.ec2Host}:8085/api/admin/kafka/topics`, payload).subscribe({
          next: (res) => {
            this.kafkaTopics = [newTopicItem, ...this.kafkaTopics.filter(t => t.name !== newTopicItem.name)];
            this.handleKafkaSuccess(`Crear Tópico '${payload.name}' (Local)`, res?.message || 'Tópico creado.');
            this.newKafkaTopicName = 'vidasalud.telemetry.' + Math.floor(100 + Math.random() * 900);
            this.cdr.detectChanges();
            setTimeout(() => this.loadKafkaTopics(), 1200);
          },
          error: (directErr) => this.handleKafkaError('Crear Tópico Kafka', directErr)
        });
      }
    });
  }

  deleteKafkaTopic(topicName: string): void {
    if (!topicName) return;
    this.isLoadingKafka = true;

    // Eliminación optimista instantánea en la interfaz
    const previousTopics = [...this.kafkaTopics];
    this.kafkaTopics = this.kafkaTopics.filter(t => t.name !== topicName);
    this.cdr.detectChanges();

    const url = `${this.gatewayUrl}/api/admin/kafka/topics/${encodeURIComponent(topicName)}`;
    this.http.delete<any>(url).subscribe({
      next: (res) => {
        this.handleKafkaSuccess(`Eliminar Tópico '${topicName}'`, res?.message || 'Tópico eliminado.');
        this.cdr.detectChanges();
        setTimeout(() => this.loadKafkaTopics(), 1200);
      },
      error: (err) => {
        if (err.status && err.status !== 0) {
          // Si falló por restricción (ej: 403 tópico base), restaurar en la lista
          this.kafkaTopics = previousTopics;
          this.handleKafkaError('Eliminar Tópico Kafka', err);
          return;
        }
        this.http.delete<any>(`http://${this.ec2Host}:8085/api/admin/kafka/topics/${encodeURIComponent(topicName)}`).subscribe({
          next: (res) => {
            this.handleKafkaSuccess(`Eliminar Tópico '${topicName}' (Local)`, res?.message || 'Tópico eliminado.');
            this.cdr.detectChanges();
            setTimeout(() => this.loadKafkaTopics(), 1200);
          },
          error: (directErr) => {
            this.kafkaTopics = previousTopics;
            this.handleKafkaError('Eliminar Tópico Kafka', directErr);
          }
        });
      }
    });
  }

  private handleKafkaSuccess(action: string, msg: string): void {
    this.isLoadingKafka = false;
    this.lastKafkaResult = {
      timestamp: new Date().toLocaleTimeString(),
      action: action,
      status: 'SUCCESS',
      message: msg
    };
    this.cdr.detectChanges();
  }

  openKafkaUi(): void {
    const host = this.ec2Host || '3.215.109.69';
    window.open(`http://${host}:8090`, '_blank');
  }
}
