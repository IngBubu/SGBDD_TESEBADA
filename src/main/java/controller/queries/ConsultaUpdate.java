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
        boolean ejecutadoEnSQL = false;
        boolean ejecutadoEnNeo4j = false;

        // 🔹 Intentar en SQL Server
        for (Connection conn : conexionesSQL.values()) {
            if (ejecutarActualizacionSQL(conn, sql)) {
                ejecutadoEnSQL = true;
                System.out.println("✅ Update realizado en SQL Server.");
                break; // Si se ejecutó en SQL Server, detener la búsqueda.
            }
        }

        // 🔹 Convertir SQL a Cypher si la consulta no se ejecutó en SQL Server
        String cypherQuery = sqlParser.convertirSQLaCypher(sql);

        // 🔹 Intentar en Neo4j
        for (Session session : conexionesNeo4j.values()) {
            if (ejecutarActualizacionNeo4j(session, cypherQuery)) {
                ejecutadoEnNeo4j = true;
                System.out.println("✅ Update realizado en Neo4j.");
                break; // Si se ejecutó en Neo4j, detener la búsqueda.
            }
        }

        // 🔹 Si no se pudo actualizar en ninguna base de datos, mostrar advertencia
        if (!ejecutadoEnSQL && !ejecutadoEnNeo4j) {
            System.err.println("⚠️ No se pudo actualizar en ninguna base de datos.");
        }
    }

    private boolean ejecutarActualizacionSQL(Connection conn, String sql) {
        try (Statement stmt = conn.createStatement()) {
            int filasAfectadas = stmt.executeUpdate(sql);
            return filasAfectadas > 0; // Solo devolver true si se afectaron filas
        } catch (SQLException e) {
            System.err.println("❌ Error en SQL Server: " + e.getMessage());
            return false;
        }
    }

    private boolean ejecutarActualizacionNeo4j(Session session, String cypherQuery) {
        try {
            System.out.println("🔍 Consulta Cypher generada para UPDATE: " + cypherQuery);
            session.run(cypherQuery);
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j: " + e.getMessage());
            return false;
        }
    }
}
