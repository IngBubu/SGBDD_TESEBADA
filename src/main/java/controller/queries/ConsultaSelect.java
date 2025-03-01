package controller.queries;

import controller.SQLaCypher;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class ConsultaSelect {
    private final Map<String, Connection> conexionesSQL = new ConcurrentHashMap<>();
    private final Map<String, Session> conexionesNeo4j = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);
    private final Map<String, String> zonasSQL = new HashMap<>();
    private final Map<String, String> zonasNeo4j = new HashMap<>();
    private final SQLaCypher sqlParser = new SQLaCypher(); // Conversión de SQL a Cypher

    // Agregar conexión a SQL Server
    public void agregarConexionSQL(String nombre, Connection conexion, String zona) {
        conexionesSQL.put(nombre, conexion);
        zonasSQL.put(nombre, zona);
    }

    // Agregar conexión a Neo4j
    public void agregarConexionNeo4j(String nombre, Session session, String zona) {
        conexionesNeo4j.put(nombre, session);
        zonasNeo4j.put(nombre, zona);
    }

    // Ejecutar consulta SELECT en todas las conexiones
    public List<String[]> ejecutarConsultaSelect(String sql) {
        if (!sql.trim().toUpperCase().startsWith("SELECT")) {
            System.err.println("⚠️ La consulta no es un SELECT válido.");
            return new ArrayList<>();
        }

        List<String[]> resultados = new ArrayList<>();
        String[] nombresColumnas = obtenerNombresColumnas(sql);

        // Ejecutar en SQL Server
        for (Connection conn : conexionesSQL.values()) {
            resultados.addAll(ejecutarConsultaSQL(conn, sql));
        }

        // Convertir SQL a Cypher y ejecutar en Neo4j
        String cypherQuery = sqlParser.convertirSQLaCypher(sql);
        for (Session session : conexionesNeo4j.values()) {
            resultados.addAll(ejecutarConsultaNeo4j(session, cypherQuery, nombresColumnas));
        }

        System.out.println("📊 Total registros obtenidos: " + resultados.size());
        return resultados;
    }

    // Obtener nombres de columnas de SQL Server
    public String[] obtenerNombresColumnas(String consulta) {
        if (consulta.trim().isEmpty()) {
            return new String[]{"Error: Consulta vacía"};
        }

        String sqlUpper = consulta.trim().toUpperCase();

        // Si es una consulta SELECT, obtener las columnas
        if (sqlUpper.startsWith("SELECT")) {
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
                    System.err.println("❌ Error obteniendo nombres de columnas en SQL Server: " + e.getMessage());
                }
            }
        }

        // Si no es SELECT, simplemente devuelve un mensaje neutral sin bloquear
        return new String[]{"No aplica"};
    }


    // Ejecutar consulta en SQL Server
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

    // Ejecutar consulta en Neo4j
    private List<String[]> ejecutarConsultaNeo4j(Session session, String cypherQuery, String[] nombresColumnas) {
        List<String[]> resultados = new ArrayList<>();
        System.out.println("🔎 Consulta ejecutada en Neo4j: " + cypherQuery);

        try {
            Result result = session.run(cypherQuery);

            while (result.hasNext()) {
                Record record = result.next();
                Map<String, Object> propiedades = record.get("n").asNode().asMap();

                System.out.println("📌 Datos obtenidos de Neo4j: " + propiedades); // Ver qué datos devuelve

                List<String> fila = new ArrayList<>();

                // Reorganizar los datos de Neo4j en el mismo orden que SQL Server
                for (String columna : nombresColumnas) {
                    String columnaNeo4j = columna.toLowerCase(); // Asegurar coincidencia de nombres
                    if (propiedades.containsKey(columnaNeo4j)) {
                        Object valor = propiedades.get(columnaNeo4j);
                        fila.add(valor != null ? valor.toString() : "null");
                    } else {
                        System.err.println("⚠️ Propiedad no encontrada en Neo4j: " + columnaNeo4j);
                        fila.add("null"); // Si no se encuentra la propiedad, asignamos "null"
                    }
                }

                resultados.add(fila.toArray(new String[0]));
            }
        } catch (Exception e) {
            System.err.println("❌ Error ejecutando consulta en Neo4j: " + e.getMessage());
            e.printStackTrace();
        }

        return resultados;
    }

}
