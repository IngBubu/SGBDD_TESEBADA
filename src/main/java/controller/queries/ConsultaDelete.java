package controller.queries;

import controller.SQLaCypher;
import org.neo4j.driver.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConsultaDelete {
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

    public boolean ejecutarDelete(String sql) {
        if (!sql.trim().toUpperCase().startsWith("DELETE")) {
            System.err.println("⚠️ La consulta no es un DELETE válido.");
            return false;
        }

        boolean eliminado = false;
        String cypherQuery = sqlParser.convertirSQLaCypher(sql); // Convertir SQL a Cypher

        // 🔹 Ejecutar DELETE en SQL Server
        for (Map.Entry<String, Connection> entry : conexionesSQL.entrySet()) {
            if (ejecutarActualizacionSQL(entry.getValue(), sql)) {
                System.out.println("✅ DELETE ejecutado en SQL Server");
                eliminado = true;
            }
        }

        // 🔹 Ejecutar DELETE en Neo4j
        for (Map.Entry<String, Session> entry : conexionesNeo4j.entrySet()) {
            if (ejecutarActualizacionNeo4j(entry.getValue(), cypherQuery)) {
                System.out.println("✅ DELETE ejecutado en Neo4j");
                eliminado = true;
            }
        }

        if (!eliminado) {
            System.err.println("⚠️ No se pudo eliminar en ninguna base de datos.");
        }

        return eliminado;
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
        try {
            session.run(cypherQuery);
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j: " + e.getMessage());
            return false;
        }
    }
}
