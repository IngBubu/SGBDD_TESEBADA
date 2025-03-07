package controller.queries;

import controller.SQLaCypher;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.types.Node;

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


    public void agregarConexionSQL(String nombre, Connection conexion, String zona) {
        conexionesSQL.put(nombre, conexion);
        zonasSQL.put(nombre, zona);
    }


    public void agregarConexionNeo4j(String nombre, Session session, String zona) {
        conexionesNeo4j.put(nombre, session);
        zonasNeo4j.put(nombre, zona);
    }

    // SELECT en todas las conexiones
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


    public String[] obtenerNombresColumnas(String consulta) {
        if (consulta.trim().isEmpty()) {
            return new String[]{"Error: Consulta vacía"};
        }

        String sqlUpper = consulta.trim().toUpperCase();


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


        return new String[]{"No aplica"};
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
                Node nodo = record.get("n").asNode();  // Obtener el nodo


                String idCliente = nodo.containsKey("idcliente") ? String.valueOf(nodo.get("idcliente").asInt()) : "null";
                String nombre = nodo.containsKey("nombre") ? nodo.get("nombre").asString() : "null";
                String estado = nodo.containsKey("estado") ? nodo.get("estado").asString() : "null";
                String credito = nodo.containsKey("credito") ? String.format("%.2f", nodo.get("credito").asDouble()) : "null";
                String deuda = nodo.containsKey("deuda") ? String.format("%.2f", nodo.get("deuda").asDouble()) : "null";
                String zona = nodo.containsKey("zona") ? nodo.get("zona").asString() : "null";

                System.out.println("📌 Registro obtenido de Neo4j: " + idCliente + ", " + nombre + ", " + estado + ", " + credito + ", " + deuda + ", " + zona);

                resultados.add(new String[]{idCliente, nombre, estado, credito, deuda, zona});
            }



        } catch (Exception e) {
            System.err.println("❌ Error ejecutando consulta en Neo4j: " + e.getMessage());
            e.printStackTrace();
        }

        return resultados;
    }

}
