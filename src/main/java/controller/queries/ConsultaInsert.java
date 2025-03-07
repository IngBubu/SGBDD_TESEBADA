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

    public Map<String, Connection> getConexionesSQL() {
        return conexionesSQL;
    }

    public Map<String, Session> getConexionesNeo4j() {
        return conexionesNeo4j;
    }

    public void ejecutarInsert(String sql) {
        String estado = extraerEstadoDesdeInsert(sql);
        if (estado == null) {
            System.err.println("❌ No se pudo extraer el estado desde la consulta INSERT.");
            return;
        }

        String zonaDestino = FragmentoZonas.obtenerZonaPorEstado(estado);
        System.out.println("📌 Estado detectado: " + estado);
        System.out.println("📌 Zona asignada: " + zonaDestino);

        if (zonaDestino.equals("ZonaDesconocida")) {
            System.err.println("⚠️ No se pudo insertar en ninguna base de datos para el estado: " + estado);
            return;
        }

        boolean insertado = false;
        List<Connection> participantesSQL = new ArrayList<>();
        List<Session> participantesNeo4j = new ArrayList<>();

        for (Map.Entry<String, String> entry : zonasSQL.entrySet()) {
            if (entry.getValue().equals(zonaDestino)) {
                participantesSQL.add(conexionesSQL.get(entry.getKey()));
            }
        }

        for (Map.Entry<String, String> entry : zonasNeo4j.entrySet()) {
            if (entry.getValue().equals(zonaDestino)) {
                participantesNeo4j.add(conexionesNeo4j.get(entry.getKey()));
            }
        }

        if (!fasePreparacion(participantesSQL)) {
            System.err.println("❌ ABORTANDO: No todos los participantes están listos.");
            return;
        }

        for (Connection conn : participantesSQL) {
            if (ejecutarActualizacionSQL(conn, sql)) {
                insertado = true;
            }
        }

        for (Session session : participantesNeo4j) {
            String cypherQuery = new SQLaCypher().convertirSQLaCypher(sql);
            if (ejecutarActualizacionNeo4j(session, cypherQuery)) {
                insertado = true;
            }
        }

        if (insertado) {
            commitTransaccion(participantesSQL);
        } else {
            rollbackTransaccion(participantesSQL);
        }
    }

    private boolean fasePreparacion(List<Connection> sqlConns) {
        try {
            for (Connection conn : sqlConns) {
                conn.setAutoCommit(false);
            }
            System.out.println("✅ Fase de preparación completada.");
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en fase de preparación: " + e.getMessage());
            return false;
        }
    }

    private void commitTransaccion(List<Connection> sqlConns) {
        try {
            for (Connection conn : sqlConns) {
                conn.commit();
                conn.setAutoCommit(true);
            }
            System.out.println("✅ COMMIT completado en SQL Server.");
        } catch (SQLException e) {
            System.err.println("❌ Error en commit SQL Server: " + e.getMessage());
        }
    }

    private void rollbackTransaccion(List<Connection> sqlConns) {
        try {
            for (Connection conn : sqlConns) {
                conn.rollback();
                conn.setAutoCommit(true);
            }
            System.out.println("🔄 ROLLBACK realizado en SQL Server.");
        } catch (SQLException e) {
            System.err.println("❌ Error en rollback SQL Server: " + e.getMessage());
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
        try (Transaction tx = session.beginTransaction()) { // 🔹 Nueva transacción para cada consulta
            tx.run(cypherQuery);
            tx.commit();
            System.out.println("✅ INSERT ejecutado en Neo4j con nueva transacción.");
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j: " + e.getMessage());
            return false;
        }
    }

    private String extraerEstadoDesdeInsert(String sql) {
        sql = sql.toLowerCase();
        int indexValues = sql.indexOf("values");
        if (indexValues == -1) return null;

        String[] partes = sql.substring(indexValues).split(",");
        if (partes.length < 3) return null;

        String estado = partes[2].replace("'", "").trim().toLowerCase();
        System.out.println("📌 Estado extraído desde INSERT: " + estado);

        return estado;
    }

    private String convertirInsertACypher(String sql) {
        return sql.replace("INSERT INTO", "CREATE (n:")
                .replace("VALUES", ") SET n =")
                .replace(";", "");
    }
    public boolean ejecutarTransaccionDistribuida(String sqlZonaNorte, String sqlZonaCentro, String sqlZonaSur, String tipo) {
        try {
            if (sqlZonaNorte != null) {
                ejecutarInsert(sqlZonaNorte);
            }
            if (sqlZonaCentro != null) {
                ejecutarInsert(sqlZonaCentro);
            }
            if (sqlZonaSur != null) {
                ejecutarInsert(sqlZonaSur);
            }
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en la transacción distribuida: " + e.getMessage());
            return false;
        }
    }

}
