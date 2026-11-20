package cl.duoc.vidasalud.appointments.config;

import cl.duoc.vidasalud.appointments.model.Appointment;
import cl.duoc.vidasalud.appointments.model.AppointmentStatus;
import cl.duoc.vidasalud.appointments.repository.AppointmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class DataInitializer implements CommandLineRunner {

    private final AppointmentRepository appointmentRepository;

    @Override
    public void run(String... args) {
        if (appointmentRepository.count() == 0) {
            log.info("Inicializando datos semilla de atenciones mÃ©dicas para VidaSalud...");

            List<Appointment> initialAppointments = List.of(
                Appointment.builder()
                    .patientId("PAC-001")
                    .patientName("DamiÃ¡n Soto")
                    .patientRut("19.876.543-2")
                    .serviceId("SRV-01")
                    .serviceName("Medicina General")
                    .centerId("CENTRO-01")
                    .boxId("Box 01 Med. General")
                    .appointmentDate(LocalDateTime.now().plusHours(1))
                    .status(AppointmentStatus.SOLICITADA)
                    .reason("Control de rutina y presiÃ³n arterial")
                    .notes("Paciente primera vez")
                    .createdBy("paciente@vidasalud.cl")
                    .build(),

                Appointment.builder()
                    .patientId("PAC-002")
                    .patientName("Ãngel Valenzuela")
                    .patientRut("18.765.432-1")
                    .serviceId("SRV-02")
                    .serviceName("OdontologÃ­a General")
                    .centerId("CENTRO-01")
                    .boxId("Box 03 Dental")
                    .appointmentDate(LocalDateTime.now().plusHours(2))
                    .status(AppointmentStatus.CONFIRMADA)
                    .reason("Limpieza dental y revisiÃ³n de caries")
                    .notes("Cupo confirmado en Box 03")
                    .createdBy("recepcionista@vidasalud.cl")
                    .build(),

                Appointment.builder()
                    .patientId("PAC-003")
                    .patientName("Carolina GÃ³mez")
                    .patientRut("20.123.456-7")
                    .serviceId("SRV-03")
                    .serviceName("PediatrÃ­a Preventiva")
                    .centerId("CENTRO-01")
                    .boxId("Box 05 PediatrÃ­a")
                    .appointmentDate(LocalDateTime.now().plusHours(3))
                    .status(AppointmentStatus.EN_ESPERA)
                    .reason("VacunaciÃ³n y control de peso")
                    .notes("Paciente en sala de espera")
                    .createdBy("recepcionista@vidasalud.cl")
                    .build(),

                Appointment.builder()
                    .patientId("PAC-004")
                    .patientName("Roberto Silva")
                    .patientRut("15.432.109-8")
                    .serviceId("SRV-04")
                    .serviceName("KinesiologÃ­a")
                    .centerId("CENTRO-01")
                    .boxId("Box 02 Med. General")
                    .appointmentDate(LocalDateTime.now().minusHours(1))
                    .status(AppointmentStatus.EN_ATENCION)
                    .reason("RehabilitaciÃ³n lumbar sesiÃ³n 3")
                    .notes("En tratamiento con especialista")
                    .createdBy("medico@vidasalud.cl")
                    .build()
            );

            appointmentRepository.saveAll(initialAppointments);
            log.info("Datos semilla cargados exitosamente. Total atenciones: {}", appointmentRepository.count());
        }
    }
}
