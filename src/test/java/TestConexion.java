
import java.sql.Connection;
import org.neo4j.driver.Session;

public class TestConexion {
    public static void main(String[] args) {
        // Prueba de conexión a SQL Server (ZonaCentro)
        System.out.println("Probando conexión a SQL Server (ZonaCentro)...");
        try (Connection conexionCentro = ConexionSQLServerCentro.obtenerConexion()) {
            if (conexionCentro != null) {
                System.out.println("✅ Conexión exitosa a Empresa_ZonaCentro");
            } else {
                System.out.println("❌ Falló la conexión a Empresa_ZonaCentro");
            }
        } catch (Exception e) {
            System.out.println("⚠ Error en la conexión a Empresa_ZonaCentro: " + e.getMessage());
        }

        // Prueba de conexión a SQL Server (ZonaNorte)
        System.out.println("\nProbando conexión a SQL Server (ZonaNorte)...");
        try (Connection conexionNorte = ConexionSQLServerNorte.obtenerConexion()) {
            if (conexionNorte != null) {
                System.out.println("✅ Conexión exitosa a Empresa_ZonaNorte");
            } else {
                System.out.println("❌ Falló la conexión a Empresa_ZonaNorte");
            }
        } catch (Exception e) {
            System.out.println("⚠ Error en la conexión a Empresa_ZonaNorte: " + e.getMessage());
        }

        // Prueba de conexión a Neo4j (ZonaSur)
        System.out.println("\nProbando conexión a Neo4j (ZonaSur)...");
        try (Session sesionNeo4j = ConexionNeo4jServerSur.obtenerSesion()) {
            if (sesionNeo4j != null) {
                System.out.println("✅ Conexión exitosa a Empresa_ZonaSur");
            } else {
                System.out.println("❌ Falló la conexión a Empresa_ZonaSur");
            }
        } catch (Exception e) {
            System.out.println("⚠ Error en la conexión a Empresa_ZonaSur: " + e.getMessage());
        }
    }
}
