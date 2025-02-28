package repository;

import controller.GestorDeDatos;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;

public class AgregarConexionSQL extends JFrame {
    private JTextField campoNombre, campoIP, campoBaseDatos, campoUsuario;
    private JPasswordField campoContrasena;
    private GestorDeDatos gestorDatos;
    private static final int PUERTO_POR_DEFECTO = 1433; // Puerto de SQL Server
    private static final String PREFIJO_URL = "jdbc:sqlserver://"; // Prefijo fijo

    public AgregarConexionSQL(GestorDeDatos gestorDatos) {
        this.gestorDatos = gestorDatos;

        setTitle("Agregar Conexión SQL Server");
        setSize(400, 300);
        setLayout(new GridLayout(6, 2));

        add(new JLabel("Nombre de la conexión:"));
        campoNombre = new JTextField();
        add(campoNombre);

        add(new JLabel("IP del Servidor:"));
        campoIP = new JTextField();
        add(campoIP);

        add(new JLabel("Nombre de la Base de Datos:"));
        campoBaseDatos = new JTextField();
        add(campoBaseDatos);

        add(new JLabel("Usuario:"));
        campoUsuario = new JTextField();
        add(campoUsuario);

        add(new JLabel("Contraseña:"));
        campoContrasena = new JPasswordField();
        add(campoContrasena);

        JButton botonAgregar = new JButton("Agregar Conexión");
        botonAgregar.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                agregarConexion();
            }
        });
        add(botonAgregar);

        setVisible(true);
    }

    private void agregarConexion() {
        String nombre = campoNombre.getText();
        String ipServidor = campoIP.getText();
        String baseDatos = campoBaseDatos.getText();
        String usuario = campoUsuario.getText();
        String contrasena = new String(campoContrasena.getPassword());

        // Construye la URL con el prefijo y el puerto por defecto
        String urlCompleta = PREFIJO_URL + ipServidor + ":" + PUERTO_POR_DEFECTO + ";databaseName=" + baseDatos;

        try {
            Connection conexion = DriverManager.getConnection(urlCompleta, usuario, contrasena);
            gestorDatos.agregarConexionSQL(nombre, conexion);
            JOptionPane.showMessageDialog(this, "✅ Conexión SQL agregada correctamente.");
            dispose();
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "❌ Error al conectar a SQL Server: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }
}
