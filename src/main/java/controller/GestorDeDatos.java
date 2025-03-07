package controller;

import controller.queries.ConsultaSelect;
import controller.queries.ConsultaInsert;
import controller.queries.ConsultaUpdate;
import controller.queries.ConsultaDelete;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Session;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class GestorDeDatos {
    private final ExecutorService executor;
    private final ConsultaSelect consultaSelect;
    private final ConsultaInsert consultaInsert;
    private final ConsultaUpdate consultaUpdate;
    private final ConsultaDelete consultaDelete;
    private Map<String, Map<String, String>> mapeoTablas;

    private static final String CONFIG_FILE = "config.json";

    public GestorDeDatos() {
        this.executor = Executors.newFixedThreadPool(5);
        this.consultaSelect = new ConsultaSelect();
        this.consultaInsert = new ConsultaInsert();
        this.consultaUpdate = new ConsultaUpdate();
        this.consultaDelete = new ConsultaDelete();
        cargarMapeoTablas();
        cargarConexionesDesdeJson();
        System.out.println("🔍 Verificando conexiones en GestorDeDatos...");
        System.out.println("   🔹 Conexiones SQL Server registradas: " + consultaSelect.getConexionesSQL().size());
        System.out.println("   🔹 Conexiones Neo4j registradas: " + consultaSelect.getConexionesNeo4j().size());

    }

    public String[] obtenerNombresColumnas(String consulta) {
        return consultaSelect.obtenerNombresColumnas(consulta);
    }
    private void cargarConexionesDesdeJson() {
        try (Reader reader = new FileReader(CONFIG_FILE)) {
            Map<String, Object> configData = new Gson().fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
            Map<String, Map<String, String>> conexiones = (Map<String, Map<String, String>>) configData.get("conexiones");

            for (Map.Entry<String, Map<String, String>> entry : conexiones.entrySet()) {
                String nombreConexion = entry.getKey();
                Map<String, String> datosConexion = entry.getValue();

                String tipo = datosConexion.get("Zona");
                String ip = datosConexion.get("IP");
                String usuario = datosConexion.get("Usuario");
                String password = datosConexion.get("Password");
                String baseDatos = datosConexion.get("BaseDatos");

                System.out.println("🔹 Intentando conectar a " + nombreConexion + " (" + tipo + ")");
                System.out.println("   📌 IP: " + ip + " | Base de Datos: " + baseDatos + " | Usuario: " + usuario);

                if (tipo.equals("ZonaNorte") || tipo.equals("ZonaCentro")) {
                    try {
                        String url = "jdbc:sqlserver://" + ip + ":1433;databaseName=" + baseDatos + ";encrypt=true;trustServerCertificate=true";
                        Connection conexion = DriverManager.getConnection(url, usuario, password);
                        agregarConexionSQL(nombreConexion, conexion, tipo);
                        System.out.println("✅ Conectado exitosamente a " + nombreConexion);
                    } catch (SQLException e) {
                        System.err.println("❌ Error conectando a " + nombreConexion + ": " + e.getMessage());
                    }
                } else if (tipo.equals("ZonaSur")) {
                    try {
                        String uri = "bolt://" + ip + ":7687";
                        Session session = org.neo4j.driver.GraphDatabase.driver(uri, AuthTokens.basic(usuario, password)).session();
                        agregarConexionNeo4j(nombreConexion, session, tipo);
                        System.out.println("✅ Conectado exitosamente a " + nombreConexion);
                    } catch (Exception e) {
                        System.err.println("❌ Error conectando a Neo4j en " + nombreConexion + ": " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("❌ Error al cargar conexiones desde JSON: " + e.getMessage());
        }
    }


    public void agregarConexionSQL(String nombre, Connection conexion, String zona) {
        consultaSelect.agregarConexionSQL(nombre, conexion, zona);
        consultaInsert.agregarConexionSQL(nombre, conexion, zona);
        consultaUpdate.agregarConexionSQL(nombre, conexion, zona);
        consultaDelete.agregarConexionSQL(nombre, conexion, zona);
        System.out.println("✅ Conexión establecida con " + nombre + " en la zona " + zona);
        System.out.println("🔹 Total de conexiones activas: SQL Server: " + consultaSelect.getConexionesSQL().size() + ", Neo4j: " + consultaSelect.getConexionesNeo4j().size());
    }

    public void agregarConexionNeo4j(String nombre, Session session, String zona) {
        consultaSelect.agregarConexionNeo4j(nombre, session, zona);
        consultaInsert.agregarConexionNeo4j(nombre, session, zona);
        consultaUpdate.agregarConexionNeo4j(nombre, session, zona);
        consultaDelete.agregarConexionNeo4j(nombre, session, zona);
        System.out.println("✅ Conexión establecida con " + nombre + " en la zona " + zona);
        System.out.println("🔹 Total de conexiones activas: SQL Server: " + consultaSelect.getConexionesSQL().size() + ", Neo4j: " + consultaSelect.getConexionesNeo4j().size());
    }

    private void cargarMapeoTablas() {
        try (Reader reader = new FileReader(CONFIG_FILE)) {
            Map<String, Object> configData = new Gson().fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
            this.mapeoTablas = (Map<String, Map<String, String>>) configData.get("mapeoTablas");
            System.out.println("✅ Mapeo de tablas cargado correctamente. Datos: " + this.mapeoTablas);
        } catch (Exception e) {
            System.err.println("⚠️ Error al cargar el mapeo de tablas: " + e.getMessage());
            this.mapeoTablas = new HashMap<>();
        }
    }

    public String obtenerNombreFisico(String tablaLogica, String zona) {
        if (tablaLogica == null || zona == null) {
            System.err.println("⚠️ Error: Tabla lógica o zona es null.");
            return tablaLogica; // Devuelve el mismo nombre si hay un problema
        }

        String tablaLogicaLower = tablaLogica.toLowerCase();
        if (!mapeoTablas.containsKey(tablaLogicaLower)) {
            System.err.println("⚠️ No se encontró mapeo para la tabla lógica: " + tablaLogica);
            return tablaLogica; // Devuelve el nombre original si no está en el mapeo
        }

        // Obtener el nombre físico de la tabla en la zona específica
        String nombreFisico = mapeoTablas.get(tablaLogicaLower).get(zona);
        if (nombreFisico == null) {
            System.err.println("⚠️ No hay un nombre físico definido para " + tablaLogica + " en " + zona);
            return tablaLogica; // Si no hay nombre físico, devolver la tabla original
        }

        System.out.println("✅ Mapeo aplicado: " + tablaLogica + " → " + nombreFisico + " en " + zona);
        return nombreFisico;
    }


    public void ejecutarConsulta(String sql) {
        System.out.println("🔹 Consulta recibida en ejecutarConsulta: " + sql);

        if (sql == null || sql.trim().isEmpty()) {
            System.err.println("⚠️ Consulta vacía, no se puede ejecutar.");
            return;
        }

        sql = sql.trim();
        String sqlUpper = sql.toUpperCase();

        if (sqlUpper.startsWith("SELECT")) {
            System.out.println("🔹 Se detectó SELECT, ejecutando consulta distribuida.");
            ejecutarConsultaDistribuida(sql);
        } else {
            System.err.println("⚠️ Consulta no reconocida.");
        }
    }

    public Future<List<String[]>> ejecutarConsultaDistribuida(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            System.err.println("⚠️ La consulta está vacía.");
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        String tablaLogica = extraerNombreTabla(sql);
        if (tablaLogica == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        // Reemplazar la tabla lógica con los nombres físicos en cada zona
        String sqlZonaNorte = sql.replaceAll("(?i)\\b" + tablaLogica + "\\b", obtenerNombreFisico(tablaLogica, "ZonaNorte")) + ";";
        String sqlZonaCentro = sql.replaceAll("(?i)\\b" + tablaLogica + "\\b", obtenerNombreFisico(tablaLogica, "ZonaCentro")) + ";";
        String sqlZonaSur = sql.replaceAll("(?i)\\b" + tablaLogica + "\\b", obtenerNombreFisico(tablaLogica, "ZonaSur")) + ";";



        // Transformar SQL a Cypher solo para Neo4j
        String cypherZonaSur = new SQLaCypher().convertirSQLaCypher(sqlZonaSur);

        return executor.submit(() -> {
            List<String[]> resultados = consultaSelect.ejecutarConsultaDistribuida(sqlZonaNorte, sqlZonaCentro, cypherZonaSur);
            System.out.println("📊 Registros totales obtenidos de todas las zonas: " + resultados.size());
            for (String[] fila : resultados) {
            }
            return resultados;
        });
    }





    public boolean ejecutarUpdateDistribuida(String sql) {
        String tablaLogica = extraerNombreTabla(sql);
        if (tablaLogica == null) {
            System.err.println("⚠️ No se pudo extraer el nombre de la tabla.");
            return false;
        }

        return consultaUpdate.ejecutarUpdateDistribuido(sql, tablaLogica, mapeoTablas);
    }

    public boolean ejecutarDeleteDistribuida(String sql) {
        String tablaLogica = extraerNombreTabla(sql);
        if (tablaLogica == null) {
            System.err.println("⚠️ No se pudo extraer el nombre de la tabla.");
            return false;
        }

        return consultaDelete.ejecutarDeleteDistribuido(sql, tablaLogica, mapeoTablas);
    }

    private String extraerNombreTabla(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return null;
        }

        // Eliminar el punto y coma final si existe
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        String[] palabras = sql.split("\\s+"); // Dividir por espacios
        for (int i = 0; i < palabras.length - 1; i++) {
            if (palabras[i].equalsIgnoreCase("FROM") || palabras[i].equalsIgnoreCase("INTO") ||
                    palabras[i].equalsIgnoreCase("UPDATE") || palabras[i].equalsIgnoreCase("DELETE")) {
                return palabras[i + 1].replace(";", "").trim(); // Eliminar cualquier ; errante
            }
        }
        return null;
    }


    private boolean fasePreparacion(List<Connection> sqlConns) {
        try {
            for (Connection conn : sqlConns) {
                if (conn == null) {
                    System.err.println("❌ Error: Una conexión a SQL Server es NULL.");
                    return false;
                }
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
            System.out.println("✅ COMMIT en todas las bases de datos completado.");
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
}
