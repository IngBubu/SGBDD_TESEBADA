package controller.queries;

import controller.SQLaCypher;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

import java.sql.*;
import java.util.*;

public class ConsultaSelect {
    private SQLaCypher sqlParser;

    public ConsultaSelect() {
        this.sqlParser = new SQLaCypher();
    }

    // 🔹 SELECT para SQL Server
    public List<String[]> ejecutarConsultaSQL(Connection conn, String sql) {
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

    // 🔹 SELECT para Neo4j
    public List<String[]> ejecutarConsultaNeo4j(Session session, String sql, String[] nombresColumnas) {
        List<String[]> resultados = new ArrayList<>();
        String cypherQuery = sqlParser.convertirSQLaCypher(sql);

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
}
