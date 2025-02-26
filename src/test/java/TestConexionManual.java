import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestConexionManual {
    public static void main(String[] args) {
        String url = "jdbc:sqlserver://25.5.185.106:1433;databaseName=Empresa_ZonaCentro;encrypt=true;trustServerCertificate=true";
        String user = "sa";
        String password = "123456789";

        System.out.println("Intentando conectar...");

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            if (conn != null) {
                System.out.println("✅ Conexión exitosa a Empresa_ZonaCentro");
            } else {
                System.out.println("❌ Conexión fallida");
            }
        } catch (SQLException e) {
            System.out.println("❌ Error: " + e.getMessage());
            e.printStackTrace(); // Solo para diagnóstico
        }
    }
}
