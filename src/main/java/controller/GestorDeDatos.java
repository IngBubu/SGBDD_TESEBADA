package controller;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import org.neo4j.driver.Session;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;

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

    public void agregarConexionSQL(String nombre, Connection conexion) {
        conexionesSQL.put(nombre, conexion);
        System.out.println("✅ Conexión SQL agregada: " + nombre);
    }

    public void agregarConexionNeo4j(String nombre, Session session) {
        conexionesNeo4j.put(nombre, session);
        System.out.println("✅ Conexión Neo4j agregada: " + nombre);
    }

    public String[] obtenerNombresColumnas(String consulta) {
        for (Connection conexion : conexionesSQL.values()) {
            try (Statement stmt = conexion.createStatement();
                 ResultSet rs = stmt.executeQuery(consulta)) {

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
    public void ejecutarConsulta(String sql) {
        executor.execute(() -> {
            try {
                String cypherQuery = sqlParser.convertirSQLaCypher(sql);

                // Ejecutar en todas las conexiones SQL
                for (Connection conn : conexionesSQL.values()) {
                    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                        conn.setAutoCommit(false);
                        stmt.executeUpdate();
                        conn.commit();
                    } catch (SQLException e) {
                        conn.rollback();
                        System.err.println("❌ Error en SQL Server: " + e.getMessage());
                    }
                }

                // Ejecutar en todas las conexiones Neo4j
                for (Session session : conexionesNeo4j.values()) {
                    try {
                        session.run(cypherQuery);
                    } catch (Exception e) {
                        System.err.println("❌ Error en Neo4j: " + e.getMessage());
                    }
                }

                System.out.println("✅ Transacción confirmada en todas las bases de datos.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public Future<List<String[]>> ejecutarConsultaSelect(String sql) {
        return executor.submit(() -> {
            List<String[]> resultados = new ArrayList<>();
            String cypherQuery = sqlParser.convertirSQLaCypher(sql);

            for (Connection conn : conexionesSQL.values()) {
                resultados.addAll(ejecutarConsultaSQL(conn, sql));
            }
            for (Session session : conexionesNeo4j.values()) {
                resultados.addAll(ejecutarConsultaNeo4j(session, cypherQuery));
            }

            System.out.println("📊 Registros obtenidos: " + resultados.size());
            return resultados;
        });
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

    private List<String[]> ejecutarConsultaNeo4j(Session session, String cypherQuery) {
        List<String[]> resultados = new ArrayList<>();
        System.out.println("🔎 Consulta ejecutada en Neo4j: " + cypherQuery);

        try {
            Result result = session.run(cypherQuery);

            while (result.hasNext()) {
                Record record = result.next();
                if (record.size() > 0) {
                    Value node = record.get(0); // Obtenemos el nodo

                    // Extraer propiedades del nodo
                    List<String> fila = new ArrayList<>();
                    node.asNode().asMap().forEach((key, value) -> fila.add(value.toString()));

                    resultados.add(fila.toArray(new String[0])); // Convertir la lista en array
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Error ejecutando consulta en Neo4j: " + e.getMessage());
            e.printStackTrace();
        }

        return resultados;
    }


    public void listarConexiones() {
        System.out.println("🔍 Conexiones SQL Activas: " + conexionesSQL.keySet());
        System.out.println("🔍 Conexiones Neo4j Activas: " + conexionesNeo4j.keySet());
    }
}
