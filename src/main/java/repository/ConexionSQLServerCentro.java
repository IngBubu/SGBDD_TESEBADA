package repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConexionSQLServerCentro {
    private static final String URL = "jdbc:sqlserver://25.5.185.106:1433;databaseName=Empresa_ZonaCentro;encrypt=true;trustServerCertificate=true";
    private static final String USUARIO = "sa";
    private static final String CONTRASENA = "123456789";

    public static Connection obtenerConexion() {
        try {
            return DriverManager.getConnection(URL, USUARIO, CONTRASENA);
        } catch (SQLException e) {
            System.out.println("⚠ No se pudo conectar a Empresa_ZonaCentro.");
            return null;
        }
    }
}
