package controller.queries;

import controller.SQLaCypher;
import org.neo4j.driver.*;
import utils.FragmentoZonas;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConsultaInsert {
    private final Map<String, Connection> conexionesSQL = new ConcurrentHashMap<>();
    private final Map<String, Session> conexionesNeo4j = new ConcurrentHashMap<>();
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

    public void ejecutarInsert(String sql) {
        String estado = extraerEstadoDesdeInsert(sql);
        String zonaDestino = FragmentoZonas.obtenerZonaPorEstado(estado);

        System.out.println("📌 Estado detectado: " + estado);
        System.out.println("📌 Zona asignada: " + zonaDestino);

        if (zonaDestino.equals("ZonaDesconocida")) {
            System.err.println("⚠️ No se pudo insertar en ninguna base de datos para el estado: " + estado);
            return;
        }

        boolean insertado = false;

        // Insertar en la base de datos SQL correspondiente a la zona
        for (Map.Entry<String, String> entry : zonasSQL.entrySet()) {
            if (entry.getValue().equals(zonaDestino)) {
                if (ejecutarActualizacionSQL(conexionesSQL.get(entry.getKey()), sql)) {
                    System.out.println("✅ Insert realizado en SQL Server (Zona: " + zonaDestino + ")");
                    insertado = true;
                    break;
                }
            }
        }

        // Insertar en Neo4j si el estado pertenece a ZonaSur
        for (Map.Entry<String, String> entry : zonasNeo4j.entrySet()) {
            if (entry.getValue().equals(zonaDestino)) {
                String cypherQuery = new SQLaCypher().convertirSQLaCypher(sql);
                if (ejecutarActualizacionNeo4j(conexionesNeo4j.get(entry.getKey()), cypherQuery)) {
                    System.out.println("✅ Insert realizado en Neo4j (Zona: " + zonaDestino + ")");
                    insertado = true;
                    break;
                }
            }
        }

        if (!insertado) {
            System.err.println("⚠️ No se pudo insertar en ninguna base de datos para el estado: " + estado);
        }
    }


    private boolean ejecutarActualizacionSQL(Connection conn, String sql) {
        if (conn == null) return false;
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            return true;
        } catch (SQLException e) {
            System.err.println("❌ Error en SQL Server: " + e.getMessage());
            return false;
        }
    }

    private boolean ejecutarActualizacionNeo4j(Session session, String cypherQuery) {
        if (session == null) return false;
        try {
            session.run(cypherQuery);
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j: " + e.getMessage());
            return false;
        }
    }

    private String extraerEstadoDesdeInsert(String sql) {
        sql = sql.toLowerCase(); // Convertimos a minúsculas para evitar problemas
        int indexValues = sql.indexOf("values");
        if (indexValues == -1) return null;

        String[] partes = sql.substring(indexValues).split(",");
        if (partes.length < 3) return null;

        // Extraemos el estado del INSERT
        String estado = partes[2].replace("'", "").trim().toLowerCase();
        System.out.println("📌 Estado extraído desde INSERT: " + estado);

        return estado;
    }


    private String convertirInsertACypher(String sql) {
        // Implementar conversión de INSERT SQL a Cypher si es necesario
        return sql.replace("INSERT INTO", "CREATE (n:")
                .replace("VALUES", ") SET n =")
                .replace(";", "");
    }
}
