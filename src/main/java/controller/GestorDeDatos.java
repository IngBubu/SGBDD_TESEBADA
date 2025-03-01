package controller;

import controller.queries.ConsultaSelect;
import controller.queries.ConsultaInsert;
import controller.queries.ConsultaUpdate;
import controller.queries.ConsultaDelete;
import org.neo4j.driver.Session;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class GestorDeDatos {
    private ExecutorService executor;
    private ConsultaSelect consultaSelect;
    private ConsultaInsert consultaInsert;
    private ConsultaUpdate consultaUpdate;
    private ConsultaDelete consultaDelete;



    public GestorDeDatos() {
        this.executor = Executors.newFixedThreadPool(5);
        consultaSelect = new ConsultaSelect();
        consultaInsert = new ConsultaInsert();
        consultaUpdate = new ConsultaUpdate();
        consultaDelete = new ConsultaDelete();
    }
    public String[] obtenerNombresColumnas(String consulta) {
        return consultaSelect.obtenerNombresColumnas(consulta);
    }

    public void agregarConexionSQL(String nombre, Connection conexion, String zona) {
        consultaSelect.agregarConexionSQL(nombre, conexion, zona);
        consultaInsert.agregarConexionSQL(nombre, conexion, zona);
        consultaUpdate.agregarConexionSQL(nombre, conexion, zona);
        consultaDelete.agregarConexionSQL(nombre, conexion, zona);
    }

    public void agregarConexionNeo4j(String nombre, Session session, String zona) {
        consultaSelect.agregarConexionNeo4j(nombre, session, zona);
        consultaInsert.agregarConexionNeo4j(nombre, session, zona);
        consultaUpdate.agregarConexionNeo4j(nombre, session, zona);
        consultaDelete.agregarConexionNeo4j(nombre, session, zona);
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
        } else if (sqlUpper.startsWith("INSERT")) {
            consultaInsert.ejecutarInsert(sql);
        } else if (sqlUpper.startsWith("UPDATE")) {
            consultaUpdate.ejecutarUpdate(sql);
        } else if (sqlUpper.startsWith("DELETE")) {
            consultaDelete.ejecutarDelete(sql);
        } else {
            System.err.println("⚠️ Consulta no reconocida.");
        }
    }

    public Future<List<String[]>> ejecutarConsultaSelect(String sql) {
        return executor.submit(() -> consultaSelect.ejecutarConsultaSelect(sql));
    }
}
