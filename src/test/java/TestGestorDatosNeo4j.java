import controller.GestorDeDatos;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestGestorDatosNeo4j {
    private static GestorDeDatos gestorDatos;

    @BeforeAll
    public static void setUp() {
        gestorDatos = new GestorDeDatos();
    }

    @Test
    public void testConsultaSelectSoloNeo4j() {
        String consulta = "SELECT * FROM clientes;";
        Future<List<String[]>> futureResultados = gestorDatos.ejecutarConsultaSelect(consulta);

        try {
            List<String[]> resultados = futureResultados.get(); // Esperar el resultado de la consulta

            System.out.println("🔎 Resultados obtenidos solo de Neo4j:");
            for (String[] fila : resultados) {
                System.out.println(String.join(", ", fila));
            }

            assertNotNull(resultados, "La lista de resultados no debe ser nula.");
            assertFalse(resultados.isEmpty(), "No se encontraron registros en Neo4j.");
        } catch (InterruptedException | ExecutionException e) {
            fail("Error ejecutando la consulta en Neo4j: " + e.getMessage());
        }
    }
}
