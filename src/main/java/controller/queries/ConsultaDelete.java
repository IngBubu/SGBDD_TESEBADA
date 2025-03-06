package controller.queries;

import controller.SQLaCypher;
import org.neo4j.driver.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    public boolean ejecutarDelete(String sql) {
        if (!sql.trim().toUpperCase().startsWith("DELETE")) {
            System.err.println("⚠️ La consulta no es un DELETE válido.");
            return false;
        }

        System.out.println("🔹 Consulta original en SQL: " + sql); // 🔹 Imprimir SQL original

        boolean eliminado = false;

        // **🔹 Ejecutar en SQL Server**
        for (Connection conn : conexionesSQL.values()) {
            if (ejecutarDeleteSQL(conn, sql)) {
                eliminado = true;
            }
        }

        // **🔹 Convertir y ejecutar en Neo4j**
        String cypherQuery = sqlParser.convertirSQLaCypher(sql);
        System.out.println("🔹 Consulta transformada a Cypher: " + cypherQuery); // 🔹 Imprimir Cypher generado

        if (cypherQuery == null || cypherQuery.trim().isEmpty()) {
            System.err.println("❌ ERROR: La conversión a Cypher falló.");
            return false;
        }

        for (Session session : conexionesNeo4j.values()) {
            if (ejecutarDeleteNeo4j(session, cypherQuery)) {
                eliminado = true;
            }
        }

        return eliminado;
    }

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
