
import repository.ConexionNeo4jServerSur;
import org.neo4j.driver.Session;

public class TestConexionNeo4j {
    public static void main(String[] args) {
        System.out.println("🔍 Probando conexión a Neo4j...");

        try (Session session = ConexionNeo4jServerSur.obtenerSesion()) {
            if (session != null) {
                session.run("RETURN 1"); // Prueba de consulta en Neo4j
                System.out.println("✅ Conexión exitosa a Neo4j (Empresa_ZonaSur)");
            } else {
                System.err.println("❌ No se pudo establecer la conexión a Neo4j.");
            }
        } catch (Exception e) {
            System.err.println("❌ Error al conectar a Neo4j: " + e.getMessage());
        }
    }
}
