package controller;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.neo4j.driver.Session;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import repository.ConexionNeo4jServerSur;
import repository.ConexionSQLServerCentro;
import repository.ConexionSQLServerNorte;

public class GestorDeDatos {
    private ConexionSQLServerCentro conexionSQLCentro;
    private ConexionSQLServerNorte conexionSQLNorte;
    private ConexionNeo4jServerSur conexionNeo4j;
    private ExecutorService executor;
    private SQLaCypher sqlParser;

    public GestorDeDatos() {
        this.conexionSQLCentro = new ConexionSQLServerCentro();
        this.conexionSQLNorte = new ConexionSQLServerNorte();
        this.conexionNeo4j = new ConexionNeo4jServerSur();
        this.executor = Executors.newFixedThreadPool(3);
        this.sqlParser = new SQLaCypher();
    }
    public String[] obtenerNombresColumnas(String consulta) {
        try (Connection conexion = ConexionSQLServerNorte.obtenerConexion();
             Statement stmt = conexion.createStatement();
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
            return new String[]{"Error"};
        }
    }

    public void ejecutarConsulta(String sql) {
        executor.execute(() -> {
            try (
                    Connection connCentro = conexionSQLCentro.obtenerConexion();
                 Connection connNorte = conexionSQLNorte.obtenerConexion();
                 Session sessionNeo4j = conexionNeo4j.obtenerSesion()) {

                connCentro.setAutoCommit(false);
                connNorte.setAutoCommit(false);

                String cypherQuery = sqlParser.convertirSQLaCypher(sql);

                try (PreparedStatement stmtCentro = connCentro.prepareStatement(sql);
                     PreparedStatement stmtNorte = connNorte.prepareStatement(sql)) {

                    stmtCentro.executeUpdate();
                    stmtNorte.executeUpdate();
                    sessionNeo4j.run(cypherQuery);

                    connCentro.commit();
                    connNorte.commit();
                    System.out.println("✅ Transacción confirmada en todas las bases de datos.");
                } catch (SQLException e) {
                    connCentro.rollback();
                    connNorte.rollback();
                    System.err.println("❌ Transacción revertida debido a un error: " + e.getMessage());
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public Future<List<String[]>> ejecutarConsultaSelect(String sql) {
        return executor.submit(new Callable<List<String[]>>() {
            @Override
            public List<String[]> call() throws Exception {
                List<String[]> resultados = new ArrayList<>();
                try (Connection connCentro = conexionSQLCentro.obtenerConexion();
                     Connection connNorte = conexionSQLNorte.obtenerConexion()) {

                    List<String[]> resultadosCentro = ejecutarConsultaSQL(connCentro, sql);
                    List<String[]> resultadosNorte = ejecutarConsultaSQL(connNorte, sql);
                    List<String[]> resultadosNeo4j = ejecutarConsultaNeo4j(sql);

                    System.out.println("📊 Registros obtenidos:");
                    System.out.println("- ZonaCentro: " + resultadosCentro.size());
                    System.out.println("- ZonaNorte: " + resultadosNorte.size());
                    System.out.println("- ZonaSur (Neo4j): " + resultadosNeo4j.size()); // Corrección del conteo real

                    resultados.addAll(resultadosCentro);
                    resultados.addAll(resultadosNorte);
                    resultados.addAll(resultadosNeo4j);

                    System.out.println("Consulta ejecutada exitosamente.");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return resultados;
            }
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
            e.printStackTrace();
        }
        return resultados;
    }

    private List<String[]> ejecutarConsultaNeo4j(String sql) {
        List<String[]> resultados = new ArrayList<>();
        String cypherQuery = sqlParser.convertirSQLaCypher(sql);

        System.out.println("🔎 Consulta ejecutada en Neo4j: " + cypherQuery);

        try (Session session = conexionNeo4j.obtenerSesion()) {
            if (session == null) {
                System.err.println("❌ Error: No se pudo establecer la conexión con Neo4j.");
                return resultados;
            }

            Result result = session.run(cypherQuery);
            while (result.hasNext()) {
                Record record = result.next();

                // Extraer propiedades de los nodos
                if (record.size() > 0) {
                    Value node = record.get(0);
                    List<String> fila = new ArrayList<>();

                    node.asMap().forEach((key, value) -> fila.add(value.toString()));
                    resultados.add(fila.toArray(new String[0]));
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Error ejecutando consulta en Neo4j: " + e.getMessage());
            e.printStackTrace();
        }

        return resultados;
    }
}
