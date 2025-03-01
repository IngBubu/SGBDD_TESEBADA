package controller;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.neo4j.driver.Session;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;

public class GestorDeDatos {
    private ExecutorService executor;
    private SQLaCypher sqlParser;
    private Map<String, Connection> conexionesSQL;
    private Map<String, Session> conexionesNeo4j;

    public GestorDeDatos() {
        this.executor = Executors.newFixedThreadPool(5);
        this.sqlParser = new SQLaCypher();
        this.conexionesSQL = new ConcurrentHashMap<>();
        this.conexionesNeo4j = new ConcurrentHashMap<>();
    }

    public void agregarConexionSQL(String nombre, Connection conexion, String zona) {
        conexionesSQL.put(nombre, conexion);
        System.out.println("✅ Conexión SQL agregada: " + nombre + " en " + zona);
    }

    public void agregarConexionNeo4j(String nombre, Session session, String zona) {
        conexionesNeo4j.put(nombre, session);
        System.out.println("✅ Conexión Neo4j agregada: " + nombre + " en " + zona);
    }


    public void ejecutarConsulta(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            System.err.println("⚠️ Consulta vacía, no se puede ejecutar.");
            return;
        }

        sql = sql.trim();
        String sqlUpper = sql.toUpperCase();

        if (sqlUpper.startsWith("SELECT")) {
            ejecutarConsultaSelect(sql);
        } else if (sqlUpper.startsWith("INSERT") || sqlUpper.startsWith("UPDATE") || sqlUpper.startsWith("DELETE")) {
            ejecutarModificacion(sql);
        } else {
            System.err.println("⚠️ Consulta no reconocida.");
        }
    }

    // 🔹 Cambio de private a public para permitir acceso desde Ui.java
    public Future<List<String[]>> ejecutarConsultaSelect(String sql) {
        return executor.submit(() -> {
            List<String[]> resultados = new ArrayList<>();
            String[] nombresColumnas = obtenerNombresColumnas(sql);

            for (Connection conn : conexionesSQL.values()) {
                resultados.addAll(ejecutarConsultaSQL(conn, sql));
            }

            for (Session session : conexionesNeo4j.values()) {
                String cypherQuery = sqlParser.convertirSQLaCypher(sql);
                resultados.addAll(ejecutarConsultaNeo4j(session, cypherQuery, nombresColumnas));
            }

            System.out.println("📊 Registros obtenidos: " + resultados.size());
            return resultados;
        });
    }

    private void ejecutarModificacion(String sql) {
        boolean ejecutado = false;

        for (Connection conn : conexionesSQL.values()) {
            if (ejecutarActualizacionSQL(conn, sql)) {
                ejecutado = true;
                break;
            }
        }

        if (!ejecutado) {
            for (Session session : conexionesNeo4j.values()) {
                if (ejecutarActualizacionNeo4j(session, sqlParser.convertirSQLaCypher(sql))) {
                    break;
                }
            }
        }
    }

    private boolean ejecutarActualizacionSQL(Connection conn, String sql) {
        try (Statement stmt = conn.createStatement()) {
            int filasAfectadas = stmt.executeUpdate(sql);
            System.out.println("✅ " + filasAfectadas + " filas afectadas en SQL Server.");
            return true;
        } catch (SQLException e) {
            System.err.println("❌ Error en SQL Server: " + e.getMessage());
            return false;
        }
    }

    private boolean ejecutarActualizacionNeo4j(Session session, String cypherQuery) {
        try {
            session.run(cypherQuery);
            System.out.println("✅ Consulta ejecutada en Neo4j.");
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j: " + e.getMessage());
            return false;
        }
    }

    private List<String[]> ejecutarConsultaSQL(Connection conn, String sql) {
        List<String[]> resultados = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            int columnas = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                String[] fila = new String[columnas];
                for (int i = 0; i < columnas; i++) {
                    fila[i] = rs.getString(i + 1);
                }
                resultados.add(fila);
            }
        } catch (SQLException e) {
            System.err.println("❌ Error en consulta SQL Server: " + e.getMessage());
        }
        return resultados;
    }

    private List<String[]> ejecutarConsultaNeo4j(Session session, String cypherQuery, String[] nombresColumnas) {
        List<String[]> resultados = new ArrayList<>();
        System.out.println("🔎 Consulta ejecutada en Neo4j: " + cypherQuery);

        try {
            Result result = session.run(cypherQuery);
            while (result.hasNext()) {
                Record record = result.next();
                Map<String, Object> propiedades = record.get("n").asNode().asMap();
                List<String> fila = new ArrayList<>();

                for (String columna : nombresColumnas) {
                    String columnaNeo4j = columna.toLowerCase();
                    fila.add(propiedades.getOrDefault(columnaNeo4j, "null").toString());
                }

                resultados.add(fila.toArray(new String[0]));
            }
        } catch (Exception e) {
            System.err.println("❌ Error ejecutando consulta en Neo4j: " + e.getMessage());
        }

        return resultados;
    }

    // 🔹 Cambio de private a public para permitir acceso desde Ui.java
    public String[] obtenerNombresColumnas(String sql) {
        if (!sql.trim().toUpperCase().startsWith("SELECT")) {
            return new String[]{"Error: No es una consulta SELECT"};
        }

        for (Connection conexion : conexionesSQL.values()) {
            try (Statement stmt = conexion.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                ResultSetMetaData metaData = rs.getMetaData();
                int columnas = metaData.getColumnCount();
                String[] nombresColumnas = new String[columnas];

                for (int i = 0; i < columnas; i++) {
                    nombresColumnas[i] = metaData.getColumnName(i + 1);
                }
                return nombresColumnas;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return new String[]{"Error"};
    }
}
