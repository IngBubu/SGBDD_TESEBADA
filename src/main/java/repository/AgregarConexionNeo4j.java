package repository;

import controller.GestorDeDatos;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class AgregarConexionNeo4j extends JFrame {
    private JTextField campoNombre, campoIP, campoBaseDatos, campoUsuario;
    private JPasswordField campoContrasena;
    private GestorDeDatos gestorDatos;
    private static final int PUERTO_POR_DEFECTO = 7687; // Puerto estándar de Neo4j
    private static final String PREFIJO_HOST = "bolt://"; // Prefijo fijo para Neo4j

    public AgregarConexionNeo4j(GestorDeDatos gestorDatos) {
        this.gestorDatos = gestorDatos;

        setTitle("Agregar Conexión Neo4j");
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

        // Construye el host con el prefijo y el puerto por defecto
        String hostCompleto = PREFIJO_HOST + ipServidor + ":" + PUERTO_POR_DEFECTO;

        try {
            Driver driver = GraphDatabase.driver(hostCompleto, AuthTokens.basic(usuario, contrasena));
            Session session = driver.session(org.neo4j.driver.SessionConfig.forDatabase(baseDatos));
            gestorDatos.agregarConexionNeo4j(nombre, session);
            JOptionPane.showMessageDialog(this, "✅ Conexión Neo4j agregada correctamente.");
            dispose();
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "❌ Error al conectar a Neo4j: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }
}
