package controller.queries;

import controller.SQLaCypher;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.driver.*;

public class ConsultaDelete {
    private final Map<String, Connection> conexionesSQL = new ConcurrentHashMap<>();
    private final Map<String, Session> conexionesNeo4j = new ConcurrentHashMap<>();
    private final SQLaCypher sqlParser = new SQLaCypher(); // Conversión de SQL a Cypher
    private final Map<String, String> zonasSQL = new HashMap<>();
    private final Map<String, String> zonasNeo4j = new HashMap<>();

    public void agregarConexionSQL(String nombre, Connection conexion, String zona) {
        conexionesSQL.put(nombre, conexion);
        zonasSQL.put(nombre, zona);
    }

    public void agregarConexionNeo4j(String nombre, Session session, String zona) {
        conexionesNeo4j.put(nombre, session);
        zonasNeo4j.put(nombre, zona);
    }

    /**
     * 🔹 Ejecuta un DELETE distribuido en todos los fragmentos (ZonaNorte, ZonaCentro, ZonaSur).
     */
    public boolean ejecutarDeleteDistribuido(String sql, String tablaLogica, Map<String, Map<String, String>> mapeoTablas) {
        System.out.println("🔹 DELETE lógico en tabla: " + tablaLogica);

        String sqlZonaNorte = sql.replace(tablaLogica, obtenerNombreFisico(tablaLogica, "ZonaNorte", mapeoTablas));
        String sqlZonaCentro = sql.replace(tablaLogica, obtenerNombreFisico(tablaLogica, "ZonaCentro", mapeoTablas));
        String sqlZonaSur = sql.replace(tablaLogica, obtenerNombreFisico(tablaLogica, "ZonaSur", mapeoTablas));

        boolean eliminado = false;

        // **🔹 Ejecutar en SQL Server**
        for (Map.Entry<String, Connection> entry : conexionesSQL.entrySet()) {
            String zona = zonasSQL.get(entry.getKey());
            String sqlModificado = switch (zona) {
                case "ZonaNorte" -> sqlZonaNorte;
                case "ZonaCentro" -> sqlZonaCentro;
                case "ZonaSur" -> sqlZonaSur;
                default -> null;
            };

            if (sqlModificado != null && ejecutarDeleteSQL(entry.getValue(), sqlModificado)) {
                eliminado = true;
            }
        }

        // **🔹 Convertir y ejecutar en Neo4j**
        String cypherZonaSur = sqlParser.convertirSQLaCypher(sqlZonaSur);
        if (cypherZonaSur == null || cypherZonaSur.trim().isEmpty()) {
            System.err.println("❌ ERROR: La conversión a Cypher falló.");
            return eliminado;
        }

        for (Map.Entry<String, Session> entry : conexionesNeo4j.entrySet()) {
            String zona = zonasNeo4j.get(entry.getKey());
            if ("ZonaSur".equals(zona) && ejecutarDeleteNeo4j(entry.getValue(), cypherZonaSur)) {
                eliminado = true;
            }
        }

        return eliminado;
    }

    /**
     * 🔹 Obtiene el nombre físico de la tabla en una zona específica.
     */
    private String obtenerNombreFisico(String tablaLogica, String zona, Map<String, Map<String, String>> mapeoTablas) {
        return mapeoTablas.getOrDefault(tablaLogica.toLowerCase(), new HashMap<>()).getOrDefault(zona, tablaLogica);
    }

    /**
     * 🔹 Ejecuta un DELETE en SQL Server con control de transacción.
     */
    private boolean ejecutarDeleteSQL(Connection conn, String sql) {
        try {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                int filasAfectadas = stmt.executeUpdate(sql);
                if (filasAfectadas > 0) {
                    conn.commit();
                    System.out.println("✅ DELETE ejecutado en SQL Server. Filas eliminadas: " + filasAfectadas);
                    return true;
                } else {
                    System.out.println("⚠️ DELETE en SQL Server no afectó ninguna fila.");
                    conn.rollback();
                    return false;
                }
            }
        } catch (SQLException e) {
            System.err.println("❌ Error en SQL Server DELETE: " + e.getMessage());
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
     * 🔹 Ejecuta un DELETE en Neo4j.
     */
    private boolean ejecutarDeleteNeo4j(Session session, String cypherQuery) {
        if (session == null) return false;
        try (Transaction tx = session.beginTransaction()) {
            Result result = tx.run(cypherQuery);
            tx.commit();

            int nodosEliminados = result.consume().counters().nodesDeleted();
            if (nodosEliminados > 0) {
                System.out.println("✅ DELETE ejecutado en Neo4j. Nodos eliminados: " + nodosEliminados);
                return true;
            } else {
                System.out.println("⚠️ DELETE en Neo4j no eliminó ningún nodo.");
                return false;
            }
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j DELETE: " + e.getMessage());
            return false;
        }
    }
}
