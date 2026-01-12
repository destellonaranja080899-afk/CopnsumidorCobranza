package com.gs.atc.bok.DAO;

import com.gs.atc.bok.schemaRegistry.PagoCredito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;


import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

// Clase DAO para Oracle
@Repository
public class PagoCreditoDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(PagoCreditoDAO.class);
    private final DataSource dataSource;

    public PagoCreditoDAO(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    public void insertarBatch(List<PagoCredito> pagos) throws SQLException {
        LocalDate today = LocalDate.now();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formatted = today.format(formatter);
        int fifecha = Integer.parseInt(formatted);

        String sql = "INSERT INTO SAGOCREDITO.TA_ABN_CRED " +
                "(FCIDTRX, FDFECHAHORA, FCNOMBRE, FCACCION, FDFECHAINIPRO, FDFECHAFINPRO, FIPAISCU, FICANALCU, FISUCURSALCU, FIFOLIOCU, FDFECHAHORAOPERACION, FIIMPORTETOTAL, FIIDOPERACION, FIIDPRODUCTO, FIIDESTATUS, FCUSUARIOEJECUCION, FIDAGENTE, FCFOLIOAUTORIZACION, FCREFERENCIA, FCCODIGOTERMINAL, FCNUMEROCONTRATO, FIIDCANALORIGEN, FIIDSUCURSALORIGEN, FCFOLIOEXPCORRESP, FCCODIGODIVISA, FIIDPAISPEDIDO, FIIDCANALPEDIDO, FIIDSUCURSALPEDIDO, FINUMEROPEDIDO, FCNUMEROTARJETA, FDFECHAHORAANBONO, FCIMPORTE, FIIDPROCESOPAGO, FIMONTOGEMA, FCIDTIPOPRODUCTO, FCIDTOKEN, FIFECHA, ULTIMA_MODIFICACION, USUARIO_MODIFICACION) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection con = dataSource.getConnection();
             PreparedStatement ps = con.prepareStatement(sql)) {

            con.setAutoCommit(false);

            int batchSize = 2000;
            int count = 0;

            for (PagoCredito msg : pagos) {
                // --- seteo de parámetros (igual que tu código original) ---
                ps.setString(1, String.valueOf(msg.getTransaccion().getId()));
                ps.setTimestamp(2, Timestamp.from(Instant.parse(msg.getTransaccion().getFechaHora())));
                ps.setString(3, String.valueOf(String.valueOf(msg.getTransaccion().getNombre())));
                ps.setString(4, String.valueOf(String.valueOf(msg.getTransaccion().getAccion())));
                ps.setTimestamp(5,  Timestamp.from(Instant.parse(msg.getTransaccion().getFechaInicioProceso())));
                ps.setTimestamp(6, Timestamp.from(Instant.parse(msg.getTransaccion().getFechaFinProceso())));

                ps.setInt(7, msg.getOperacion().getClienteUnico().getPais());
                ps.setInt(8, msg.getOperacion().getClienteUnico().getCanal());
                ps.setInt(9, msg.getOperacion().getClienteUnico().getSucursal());
                ps.setInt(10, msg.getOperacion().getClienteUnico().getFolio());

                ps.setTimestamp(11,  Timestamp.from(Instant.parse(msg.getOperacion().getFechaHoraOperacion())));
                ps.setDouble(12, msg.getOperacion().getImporteTotal());

                ps.setObject(13, msg.getOperacion().getIdOperacion(), Types.NUMERIC);
                ps.setObject(14, msg.getOperacion().getIdProducto(), Types.NUMERIC);
                ps.setObject(15, msg.getOperacion().getIdEstatus(), Types.NUMERIC);

                ps.setString(16, String.valueOf(msg.getOperacion().getUsuarioEjecucion()));
                ps.setObject(17, msg.getOperacion().getIdAgente(), Types.NUMERIC);
                ps.setString(18, String.valueOf(msg.getOperacion().getFolioAutorizacion()));
                ps.setString(19, String.valueOf(msg.getOperacion().getReferencia()));
                ps.setString(20, String.valueOf(msg.getOperacion().getCodigoTerminal()));

                ps.setString(21, String.valueOf(msg.getOperacion().getNumeroContrato()));
                ps.setInt(22, msg.getOperacion().getIdCanalOrigen());
                ps.setInt(23, msg.getOperacion().getIdSucursalOrigen());
                ps.setString(24, String.valueOf(msg.getOperacion().getFolioExpressCorresponsales()));
                ps.setString(25, String.valueOf(msg.getOperacion().getCodigoDivisa()));

                ps.setInt(26, msg.getOperacion().getIdPaisPedido());
                ps.setInt(27, msg.getOperacion().getIdCanalPedido());
                ps.setInt(28, msg.getOperacion().getIdSucursalPedido());
                ps.setInt(29, msg.getOperacion().getNumeroPedido());

                ps.setString(30, String.valueOf(msg.getOperacion().getNumeroTarjeta()));
                ps.setTimestamp(31, Timestamp.from(Instant.parse(msg.getOperacion().getFechaHoraAbono())));
                ps.setString(32, String.valueOf(msg.getOperacion().getImporte()));

                ps.setObject(33, msg.getOperacion().getIdProcesoPago(), Types.NUMERIC);
                ps.setObject(34, msg.getOperacion().getMontoGema(), Types.NUMERIC);
                ps.setString(35, String.valueOf(msg.getOperacion().getIdTipoProducto()));
                ps.setString(36, String.valueOf(msg.getOperacion().getIdToken()));
                ps.setInt(37, fifecha);
                ps.setTimestamp(38, Timestamp.valueOf(now));
                ps.setString(39,"USRINSUMOS");

                ps.addBatch();
                count++;

                if (count % batchSize == 0) {
                    ps.executeBatch();
                    con.commit();
                    ps.clearBatch();
                }
            }

            if (count % batchSize != 0) {
                ps.executeBatch();
                con.commit();
                LOGGER.info("Insertados {} registros en total.", count);
            }

        } catch (SQLException e) {
            LOGGER.error("Error al insertar batch en Oracle", e);
            throw e;
        }
    }
}

