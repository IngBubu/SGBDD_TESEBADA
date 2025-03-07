package controller;

import controller.queries.ConsultaSelect;
import controller.queries.ConsultaInsert;
import controller.queries.ConsultaUpdate;
import controller.queries.ConsultaDelete;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class GestorDeDatos {
    private final ExecutorService executor;
    private final ConsultaSelect consultaSelect;
    private final ConsultaInsert consultaInsert;
    private final ConsultaUpdate consultaUpdate;
    private final ConsultaDelete consultaDelete;

    public GestorDeDatos() {
        this.executor = Executors.newFixedThreadPool(5);
        this.consultaSelect = new ConsultaSelect();
        this.consultaInsert = new ConsultaInsert();
        this.consultaUpdate = new ConsultaUpdate();
        this.consultaDelete = new ConsultaDelete();
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
            ejecutarTransaccion2PC(sql, "INSERT");
        } else if (sqlUpper.startsWith("UPDATE")) {
            ejecutarTransaccion2PC(sql, "UPDATE");
        } else if (sqlUpper.startsWith("DELETE")) {
            ejecutarTransaccion2PC(sql, "DELETE");
        } else {
            System.err.println("⚠️ Consulta no reconocida.");
        }
    }

    public Future<List<String[]>> ejecutarConsultaSelect(String sql) {
        return executor.submit(() -> consultaSelect.ejecutarConsultaSelect(sql));
    }

    public boolean ejecutarTransaccion2PC(String sql, String tipo) {
        List<Connection> participantesSQL = new ArrayList<>(consultaInsert.getConexionesSQL().values());
        List<Session> participantesNeo4j = new ArrayList<>(consultaInsert.getConexionesNeo4j().values());

        try {
            // * Fase 1: Preparación*
            if (!fasePreparacion(participantesSQL)) {
                System.err.println("❌ ABORTANDO: No todos los participantes están listos.");
                return false;
            }

            // *Fase 2: Ejecución*
            switch (tipo.toUpperCase()) {
                case "INSERT":
                    consultaInsert.ejecutarInsert(sql);
                    break;
                case "UPDATE":
                    consultaUpdate.ejecutarUpdate(sql);
                    break;
                case "DELETE":
                    consultaDelete.ejecutarDelete(sql);
                    break;
                default:
                    System.err.println("⚠️ Tipo de operación no soportado.");
                    return false;
            }

            // *Fase 3: Commit*
            commitTransaccion(participantesSQL);
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en la transacción 2PC: " + e.getMessage());
            rollbackTransaccion(participantesSQL);
            return false;
        }
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
