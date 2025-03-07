package controller.queries;

import controller.SQLaCypher;
import org.neo4j.driver.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    public void ejecutarUpdate(String sql) {
        boolean actualizado = false;
        List<Connection> participantesSQL = new ArrayList<>(conexionesSQL.values());
        List<Session> participantesNeo4j = new ArrayList<>(conexionesNeo4j.values());

        if (!fasePreparacion(participantesSQL, participantesNeo4j)) {
            System.err.println("❌ ABORTANDO: No todos los participantes están listos.");
            return;
        }

        // * Ejecutar UPDATE en SQL Server*
        for (Connection conn : participantesSQL) {
            if (ejecutarActualizacionSQL(conn, sql)) {
                actualizado = true;
            }
        }

        // *Ejecutar UPDATE en Neo4j*
        String cypherQuery = sqlParser.convertirSQLaCypher(sql);
        for (Session session : participantesNeo4j) {
            if (ejecutarActualizacionNeo4j(session, cypherQuery)) {
                actualizado = true;
            }
        }

        // *Commit o Rollback según el resultado*
        if (actualizado) {
            commitTransaccion(participantesSQL, participantesNeo4j);
        } else {
            rollbackTransaccion(participantesSQL, participantesNeo4j);
        }
    }

    private boolean fasePreparacion(List<Connection> sqlConns, List<Session> neo4jSessions) {
        try {
            for (Connection conn : sqlConns) {
                conn.setAutoCommit(false);
            }
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en fase de preparación: " + e.getMessage());
            return false;
        }
    }

    private void commitTransaccion(List<Connection> sqlConns, List<Session> neo4jSessions) {
        try {
            for (Connection conn : sqlConns) {
                conn.commit();
                conn.setAutoCommit(true);
            }
            System.out.println("✅ COMMIT en todas las bases de datos completado.");
        } catch (SQLException e) {
            System.err.println("❌ Error en commit SQL Server: " + e.getMessage());
        }
    }

    private void rollbackTransaccion(List<Connection> sqlConns, List<Session> neo4jSessions) {
        try {
            for (Connection conn : sqlConns) {
                conn.rollback();
                conn.setAutoCommit(true);
            }
            System.out.println("🔄 ROLLBACK realizado en todas las bases de datos.");
        } catch (SQLException e) {
            System.err.println("❌ Error en rollback SQL Server: " + e.getMessage());
        }
    }

    private boolean ejecutarActualizacionSQL(Connection conn, String sql) {
        try (Statement stmt = conn.createStatement()) {
            int filasAfectadas = stmt.executeUpdate(sql);
            return filasAfectadas > 0;
        } catch (SQLException e) {
            System.err.println("❌ Error en SQL Server: " + e.getMessage());
            return false;
        }
    }

    private boolean ejecutarActualizacionNeo4j(Session session, String cypherQuery) {
        try (Transaction tx = session.beginTransaction()) { // * Cada consulta usa su propia transacción*
            tx.run(cypherQuery);
            tx.commit();
            System.out.println("✅ UPDATE en Neo4j ejecutado correctamente.");
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j: " + e.getMessage());
            return false;
        }
    }
}
