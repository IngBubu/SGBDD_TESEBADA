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
    public Map<String, Connection> getConexionesSQL() {
        return conexionesSQL;
    }

    public Map<String, Session> getConexionesNeo4j() {
        return conexionesNeo4j;
    }
    public String[] obtenerNombresColumnas(String consulta) {
        if (consulta.trim().isEmpty()) {
            return new String[]{"Error: Consulta vacía"};
        }

        String sqlUpper = consulta.trim().toUpperCase();
        String[] nombresColumnas = null;

        if (sqlUpper.startsWith("SELECT")) {
            for (Connection conexion : conexionesSQL.values()) {
                try (Statement stmt = conexion.createStatement();
                     ResultSet rs = stmt.executeQuery(consulta)) {

                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnas = metaData.getColumnCount();
                    nombresColumnas = new String[columnas];

                    for (int i = 0; i < columnas; i++) {
                        nombresColumnas[i] = metaData.getColumnName(i + 1);
                    }
                    return nombresColumnas;
                } catch (SQLException e) {
                    return new String[]{"Error obteniendo nombres de columnas"};
                }
            }
        }

        return nombresColumnas != null ? nombresColumnas : new String[]{"No aplica"};
    }


    public List<String[]> ejecutarConsultaSelect(String sql, String zona) {
        if (!sql.trim().toUpperCase().startsWith("SELECT")) {
            return new ArrayList<>();
        }

        List<String[]> resultados = new ArrayList<>();
        String[] nombresColumnas = obtenerNombresColumnas(sql);

        if (zonasSQL.containsValue(zona)) {
            for (Map.Entry<String, Connection> entry : conexionesSQL.entrySet()) {
                if (zonasSQL.get(entry.getKey()).equalsIgnoreCase(zona)) {
                    resultados.addAll(ejecutarConsultaSQL(entry.getValue(), sql));
                }
            }
        }

        // ✅ Asegurar que la consulta a Neo4j se ejecuta
        if (zonasNeo4j.containsValue(zona)) {
            System.out.println("🟢 Preparando ejecución en Neo4j para zona: " + zona);
            String cypherQuery = sqlParser.convertirSQLaCypher(sql);

            for (Map.Entry<String, Session> entry : conexionesNeo4j.entrySet()) {
                if (zonasNeo4j.get(entry.getKey()).equalsIgnoreCase(zona)) {
                    System.out.println("🟢 Ejecutando en Neo4j para " + zona + ": " + cypherQuery);
                    resultados.addAll(ejecutarConsultaNeo4j(entry.getValue(), cypherQuery, nombresColumnas));
                }
            }
        } else {
            System.err.println("❌ ERROR: No se encontró conexión para la zona: " + zona);
        }

        return resultados;
    }


    public List<String[]> ejecutarConsultaDistribuida(String sqlZonaNorte, String sqlZonaCentro, String sqlZonaSur) {
        List<String[]> resultados = new ArrayList<>();

        if (sqlZonaNorte != null) {
            System.out.println("🔍 Ejecutando consulta en Zona Norte: " + sqlZonaNorte);
            resultados.addAll(ejecutarConsultaSelect(sqlZonaNorte, "ZonaNorte"));
        }
        if (sqlZonaCentro != null) {
            System.out.println("🔍 Ejecutando consulta en Zona Centro: " + sqlZonaCentro);
            resultados.addAll(ejecutarConsultaSelect(sqlZonaCentro, "ZonaCentro"));
        }
        if (sqlZonaSur != null) {
            System.out.println("🔍 Ejecutando consulta en Zona Sur: " + sqlZonaSur);
            resultados.addAll(ejecutarConsultaSelect(sqlZonaSur, "ZonaSur"));
        }else {
            System.err.println("❌ ERROR: No se encontró conexión para la zona: ZonaSur");
        }

        return resultados;
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
            return new ArrayList<>();
        }
        return resultados;
    }

    private List<String[]> ejecutarConsultaNeo4j(Session session, String cypherQuery, String[] nombresColumnas) {
        List<String[]> resultados = new ArrayList<>();

        System.out.println("🔎 Enviando consulta a Neo4j: " + cypherQuery);

        try {
            Result result = session.run(cypherQuery);

            if (!result.hasNext()) {
                System.out.println("⚠️ La consulta no devolvió registros en Neo4j.");
                return resultados;
            }

            System.out.println("✅ Registros encontrados en Neo4j:");

            while (result.hasNext()) {
                Record record = result.next();
                System.out.println("🔍 Registro recibido: " + record);

                if (!record.containsKey("n")) {
                    System.out.println("⚠️ Registro sin clave 'n': " + record);
                    continue;
                }

                Node nodo = record.get("n").asNode();
                System.out.println("🔍 Atributos en el nodo: " + nodo.keys());

                List<String> valores = new ArrayList<>();
                for (String key : nodo.keys()) {
                    valores.add(nodo.get(key).toString());
                }

                resultados.add(valores.toArray(new String[0]));
            }

        } catch (Exception e) {
            System.err.println("❌ Error ejecutando consulta en Neo4j: " + e.getMessage());
            e.printStackTrace();
        }

        return resultados;
    }


}
