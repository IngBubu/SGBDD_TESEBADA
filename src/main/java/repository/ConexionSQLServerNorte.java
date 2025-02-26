package repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConexionSQLServerNorte {
    private static final String URL = "jdbc:sqlserver://25.65.4.242:1433;databaseName=Empresa_ZonaNorte;encrypt=true;trustServerCertificate=true";
    private static final String USUARIO = "sa";
    private static final String CONTRASENA = "123456789";

    public static Connection obtenerConexion() {
        try {
            return DriverManager.getConnection(URL, USUARIO, CONTRASENA);
        } catch (SQLException e) {
            System.out.println("⚠ No se pudo conectar a Empresa_ZonaNorte.");
            return null;
        }
    }
}
