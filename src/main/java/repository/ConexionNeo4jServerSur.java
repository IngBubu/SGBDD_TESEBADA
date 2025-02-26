package repository;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

public class ConexionNeo4jServerSur {
    private static final String URI = "bolt://25.3.62.48:7687";
    private static final String USUARIO = "neo4j";
    private static final String CONTRASENA = "chilaquilesconpollo123";
    private static final String BASE_DATOS = "neo4j";
    private static Driver driver;

    static {
        driver = GraphDatabase.driver(URI, AuthTokens.basic(USUARIO, CONTRASENA));
    }

    public static Session obtenerSesion() {
        System.out.println("🔗 Intentando conectar a Neo4j en " + URI + " con la base de datos: " + BASE_DATOS);
        return driver.session(SessionConfig.forDatabase(BASE_DATOS));
    }


    public static void cerrarConexion() {
        driver.close();
    }
}
