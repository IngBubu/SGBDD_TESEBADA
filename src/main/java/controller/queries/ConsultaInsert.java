package controller.queries;

import controller.SQLaCypher;
import org.neo4j.driver.*;
import utils.FragmentoZonas;

import java.sql.*;
import java.util.*;

public class ConsultaInsert {
    private SQLaCypher sqlParser;

    public ConsultaInsert() {
        this.sqlParser = new SQLaCypher();
    }

    public boolean ejecutarInsertSQL(Connection conn, String sql) {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            stmt.executeUpdate();
            conn.commit();
            return true;
        } catch (SQLException e) {
            System.err.println("❌ Error en SQL Server: " + e.getMessage());
            return false;
        }
    }

    public boolean ejecutarInsertNeo4j(Session session, String sql) {
        try {
            String cypherQuery = sqlParser.convertirSQLaCypher(sql);
            session.run(cypherQuery);
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j: " + e.getMessage());
            return false;
        }
    }

    public void insertarEnZona(Map<String, Connection> conexionesSQL, Map<String, Session> conexionesNeo4j, String sql) {
        String estado = obtenerEstadoDesdeInsert(sql);
        String zonaDestino = FragmentoZonas.obtenerZonaPorEstado(estado);

        if (zonaDestino == null) {
            System.err.println("❌ No se encontró zona para el estado: " + estado);
            return;
        }

        System.out.println("📌 Se detectó que el estado '" + estado + "' pertenece a la zona: " + zonaDestino);

        for (Map.Entry<String, Connection> entry : conexionesSQL.entrySet()) {
            if (entry.getValue() != null) {
                if (ejecutarInsertSQL(entry.getValue(), sql)) {
                    System.out.println("✅ Insert realizado en SQL Server (Zona: " + zonaDestino + ")");
                    return;
                }
            }
        }

        for (Map.Entry<String, Session> entry : conexionesNeo4j.entrySet()) {
            if (entry.getValue() != null) {
                if (ejecutarInsertNeo4j(entry.getValue(), sql)) {
                    System.out.println("✅ Insert realizado en Neo4j (Zona: " + zonaDestino + ")");
                    return;
                }
            }
        }
    }

    private String obtenerEstadoDesdeInsert(String sql) {
        String estado = sql.split("'")[5]; // Extrae el estado del insert
        return estado;
    }
}
