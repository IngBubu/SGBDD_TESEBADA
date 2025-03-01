package controller.queries;

import org.neo4j.driver.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConsultaUpdate {
    private Map<String, Connection> conexionesSQL = new ConcurrentHashMap<>();
    private Map<String, Session> conexionesNeo4j = new ConcurrentHashMap<>();
    private Map<String, String> zonasSQL = new HashMap<>();
    private Map<String, String> zonasNeo4j = new HashMap<>();

    public void agregarConexionSQL(String nombre, Connection conexion, String zona) {
        conexionesSQL.put(nombre, conexion);
        zonasSQL.put(nombre, zona);
    }

    public void agregarConexionNeo4j(String nombre, Session session, String zona) {
        conexionesNeo4j.put(nombre, session);
        zonasNeo4j.put(nombre, zona);
    }

    public void ejecutarUpdate(String sql) {
        for (Connection conn : conexionesSQL.values()) {
            if (ejecutarActualizacionSQL(conn, sql)) {
                System.out.println("✅ Update realizado en SQL Server.");
                return;
            }
        }

        for (Session session : conexionesNeo4j.values()) {
            if (ejecutarActualizacionNeo4j(session, sql)) {
                System.out.println("✅ Update realizado en Neo4j.");
                return;
            }
        }

        System.err.println("⚠️ No se pudo actualizar en ninguna base de datos.");
    }

    private boolean ejecutarActualizacionSQL(Connection conn, String sql) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            return true;
        } catch (SQLException e) {
            System.err.println("❌ Error en SQL Server: " + e.getMessage());
            return false;
        }
    }

    private boolean ejecutarActualizacionNeo4j(Session session, String cypherQuery) {
        try {
            session.run(cypherQuery);
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j: " + e.getMessage());
            return false;
        }
    }
}
