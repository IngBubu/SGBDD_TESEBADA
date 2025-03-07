package controller.queries;

import controller.SQLaCypher;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.driver.*;

public class ConsultaUpdate {
    private final Map<String, Connection> conexionesSQL = new ConcurrentHashMap<>();
    private final Map<String, Session> conexionesNeo4j = new ConcurrentHashMap<>();
    private final Map<String, String> zonasSQL = new HashMap<>();
    private final Map<String, String> zonasNeo4j = new HashMap<>();
    private final SQLaCypher sqlParser = new SQLaCypher(); // Conversión de SQL a Cypher

    public void agregarConexionSQL(String nombre, Connection conexion, String zona) {
        conexionesSQL.put(nombre, conexion);
        zonasSQL.put(nombre, zona);
    }

    public void agregarConexionNeo4j(String nombre, Session session, String zona) {
        conexionesNeo4j.put(nombre, session);
        zonasNeo4j.put(nombre, zona);
    }


    public boolean ejecutarUpdateDistribuido(String sql, String tablaLogica, Map<String, Map<String, String>> mapeoTablas) {
        System.out.println("🔹 UPDATE lógico en tabla: " + tablaLogica);

        String sqlZonaNorte = sql.replace(tablaLogica, obtenerNombreFisico(tablaLogica, "ZonaNorte", mapeoTablas));
        String sqlZonaCentro = sql.replace(tablaLogica, obtenerNombreFisico(tablaLogica, "ZonaCentro", mapeoTablas));
        String sqlZonaSur = sql.replace(tablaLogica, obtenerNombreFisico(tablaLogica, "ZonaSur", mapeoTablas));

        boolean actualizado = false;

        // **🔹 Ejecutar en SQL Server**
        for (Map.Entry<String, Connection> entry : conexionesSQL.entrySet()) {
            String zona = zonasSQL.get(entry.getKey());
            String sqlModificado = switch (zona) {
                case "ZonaNorte" -> sqlZonaNorte;
                case "ZonaCentro" -> sqlZonaCentro;
                case "ZonaSur" -> sqlZonaSur;
                default -> null;
            };

            if (sqlModificado != null && ejecutarActualizacionSQL(entry.getValue(), sqlModificado)) {
                actualizado = true;
            }
        }

        // **🔹 Convertir y ejecutar en Neo4j**
        String cypherZonaSur = sqlParser.convertirSQLaCypher(sqlZonaSur);
        if (cypherZonaSur == null || cypherZonaSur.trim().isEmpty()) {
            System.err.println("❌ ERROR: La conversión a Cypher falló.");
            return actualizado;
        }

        for (Map.Entry<String, Session> entry : conexionesNeo4j.entrySet()) {
            String zona = zonasNeo4j.get(entry.getKey());
            if ("ZonaSur".equals(zona) && ejecutarActualizacionNeo4j(entry.getValue(), cypherZonaSur)) {
                actualizado = true;
            }
        }

        return actualizado;
    }

    /**
     * 🔹 Obtiene el nombre físico de la tabla en una zona específica.
     */
    private String obtenerNombreFisico(String tablaLogica, String zona, Map<String, Map<String, String>> mapeoTablas) {
        return mapeoTablas.getOrDefault(tablaLogica.toLowerCase(), new HashMap<>()).getOrDefault(zona, tablaLogica);
    }

    /**
     * 🔹 Ejecuta un UPDATE en SQL Server con control de transacción.
     */
    private boolean ejecutarActualizacionSQL(Connection conn, String sql) {
        try {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                int filasAfectadas = stmt.executeUpdate(sql);
                if (filasAfectadas > 0) {
                    conn.commit();
                    System.out.println("✅ UPDATE ejecutado en SQL Server. Filas afectadas: " + filasAfectadas);
                    return true;
                } else {
                    System.out.println("⚠️ UPDATE en SQL Server no afectó ninguna fila.");
                    conn.rollback();
                    return false;
                }
            }
        } catch (SQLException e) {
            System.err.println("❌ Error en SQL Server UPDATE: " + e.getMessage());
            try {
                conn.rollback();
            } catch (SQLException rollbackEx) {
                System.err.println("❌ Error en rollback SQL Server: " + rollbackEx.getMessage());
            }
            return false;
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                System.err.println("⚠️ No se pudo restaurar AutoCommit en SQL Server.");
            }
        }
    }

    /**
     * 🔹 Ejecuta un UPDATE en Neo4j.
     */
    private boolean ejecutarActualizacionNeo4j(Session session, String cypherQuery) {
        if (session == null) return false;
        try (Transaction tx = session.beginTransaction()) { // **🔹 Cada consulta usa su propia transacción**
            tx.run(cypherQuery);
            tx.commit();
            System.out.println("✅ UPDATE en Neo4j ejecutado correctamente.");
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j UPDATE: " + e.getMessage());
            return false;
        }
    }
}
