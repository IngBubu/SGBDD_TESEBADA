package ui;

import controller.GestorDeDatos;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.util.List;
import java.util.concurrent.Future;

public class Ui extends JFrame {
    private JTextArea areaConsulta;
    private JButton botonEjecutar;
    private JButton botonConfiguraciones;
    private JTable tablaResultados;
    private JScrollPane panelTabla;
    private JLabel etiquetaEstado;
    private GestorDeDatos gestorDatos;

    public Ui() {
        setTitle("Ejecutor de Consultas SGBDD");
        setSize(800, 500);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        // Inicializar GestorDeDatos
        gestorDatos = new GestorDeDatos();

        JPanel panelSuperior = new JPanel(new BorderLayout());

        areaConsulta = new JTextArea(5, 50);
        JScrollPane panelConsulta = new JScrollPane(areaConsulta);
        panelSuperior.add(panelConsulta, BorderLayout.CENTER);

        botonConfiguraciones = new JButton("Configuraciones");
        botonConfiguraciones.addActionListener(e -> new ConfiguracionUI(this, gestorDatos));
        panelSuperior.add(botonConfiguraciones, BorderLayout.EAST);

        add(panelSuperior, BorderLayout.NORTH);

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
        botonEjecutar.addActionListener(e -> ejecutarConsulta());

        setVisible(true);
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
                    List<String[]> resultados = futureResultados.get();
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
}
