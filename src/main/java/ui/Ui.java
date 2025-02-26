package ui;

import controller.GestorDeDatos;
import org.neo4j.driver.Session;
import repository.ConexionNeo4jServerSur;
import repository.ConexionSQLServerCentro;
import repository.ConexionSQLServerNorte;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Future;

public class Ui extends JFrame {
    private JTextArea areaConsulta;
    private JButton botonEjecutar;
    private JTable tablaResultados;
    private JScrollPane panelTabla;
    private JLabel etiquetaEstado;
    private Connection conexionCentro;
    private Connection conexionNorte;
    private Session conexionNeo4j;
    private GestorDeDatos gestorDatos;

    public Ui() {
        setTitle("Ejecutor de Consultas SGBDD");
        setSize(800, 500);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        // Conectar a las bases de datos al iniciar la aplicación
        conectarBasesDeDatos();

        // Inicializar GestorDeDatos
        gestorDatos = new GestorDeDatos();

        areaConsulta = new JTextArea(5, 50);
        JScrollPane panelConsulta = new JScrollPane(areaConsulta);
        add(panelConsulta, BorderLayout.NORTH);

        JPanel panelBoton = new JPanel();
        botonEjecutar = new JButton("Ejecutar Consulta");
        panelBoton.add(botonEjecutar);
        add(panelBoton, BorderLayout.SOUTH);

        tablaResultados = new JTable();
        panelTabla = new JScrollPane(tablaResultados);
        add(panelTabla, BorderLayout.CENTER);

        etiquetaEstado = new JLabel("Estado: Esperando consulta...");
        add(etiquetaEstado, BorderLayout.WEST);

        // Evento para ejecutar la consulta
        botonEjecutar.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ejecutarConsulta();
            }
        });

        setVisible(true);
    }

    private void conectarBasesDeDatos() {
        conexionCentro = ConexionSQLServerCentro.obtenerConexion();
        conexionNorte = ConexionSQLServerNorte.obtenerConexion();
        conexionNeo4j = ConexionNeo4jServerSur.obtenerSesion();

        boolean conexionNeo4jValida = false;
        if (conexionNeo4j != null) {
            try {
                conexionNeo4j.run("RETURN 1"); // Prueba de consulta en Neo4j
                conexionNeo4jValida = true;
            } catch (Exception e) {
                System.err.println("❌ Error al conectar a Neo4j: " + e.getMessage());
                conexionNeo4j = null;
            }
        }

        if (conexionCentro != null && conexionNorte != null && conexionNeo4jValida) {
            System.out.println("✅ Conectado a todas las bases de datos correctamente.");
        } else {
            System.err.println("❌ Error al conectar a una o más bases de datos.");
        }
    }

    private void ejecutarConsulta() {
        String consulta = areaConsulta.getText().trim();
        if (consulta.isEmpty()) {
            etiquetaEstado.setText("Estado: La consulta está vacía");
            return;
        }

        etiquetaEstado.setText("Estado: Ejecutando consulta...");

        if (consulta.toUpperCase().startsWith("SELECT")) {
            Future<List<String[]>> futureResultados = gestorDatos.ejecutarConsultaSelect(consulta);

            new Thread(() -> {
                try {
                    List<String[]> resultados = futureResultados.get(); // Espera la respuesta
                    String[] nombresColumnas = gestorDatos.obtenerNombresColumnas(consulta);

                    SwingUtilities.invokeLater(() -> actualizarTabla(resultados, nombresColumnas));
                } catch (Exception ex) {
                    SwingUtilities.invokeLater(() -> etiquetaEstado.setText("Estado: Error en la consulta"));
                    ex.printStackTrace();
                }
            }).start();

        } else {
            gestorDatos.ejecutarConsulta(consulta);
            etiquetaEstado.setText("Estado: Consulta ejecutada");
        }
    }

    private void actualizarTabla(List<String[]> datos, String[] nombresColumnas) {
        if (datos.isEmpty()) {
            etiquetaEstado.setText("Estado: No se encontraron resultados");
            return;
        }

        DefaultTableModel modelo = new DefaultTableModel(nombresColumnas, 0);
        for (String[] fila : datos) {
            modelo.addRow(fila);
        }
        tablaResultados.setModel(modelo);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new Ui());
    }
}
