package ui;

import controller.GestorDeDatos;
import org.neo4j.driver.*;

import javax.swing.*;
import java.awt.*;
import java.sql.Connection;
import java.sql.DriverManager;

public class ConfiguracionUI extends JFrame {
    private JTextField campoNombreSQL, campoIPSQL, campoBaseDatosSQL, campoUsuarioSQL;
    private JPasswordField campoPasswordSQL;
    private JTextField campoNombreNeo4j, campoIPNeo4j, campoBaseDatosNeo4j, campoUsuarioNeo4j;
    private JPasswordField campoPasswordNeo4j;
    private JButton botonAgregarSQL, botonAgregarNeo4j;
    private GestorDeDatos gestorDatos;
    private JComboBox<String> campoZonaSQL, campoZonaNeo4j;

    private static final int PUERTO_SQL = 1433;
    private static final int PUERTO_NEO4J = 7687;
    private static final String PREFIJO_SQL = "jdbc:sqlserver://";
    private static final String PREFIJO_NEO4J = "bolt://";

    public ConfiguracionUI(JFrame parent, GestorDeDatos gestorDatos) {
        super("Configuración de Conexiones");
        this.gestorDatos = gestorDatos;

        String[] zonas = {"ZonaNorte", "ZonaCentro", "ZonaSur"};
        campoZonaSQL = new JComboBox<>(zonas);
        campoZonaNeo4j = new JComboBox<>(zonas);

        setSize(500, 500);
        setLayout(new GridLayout(3, 1));

        // Panel para SQL Server
        JPanel panelSQL = new JPanel(new GridLayout(7, 2));
        panelSQL.setBorder(BorderFactory.createTitledBorder("SQL Server"));
        campoNombreSQL = new JTextField();
        campoIPSQL = new JTextField();
        campoBaseDatosSQL = new JTextField();
        campoUsuarioSQL = new JTextField();
        campoPasswordSQL = new JPasswordField();
        botonAgregarSQL = new JButton("Agregar SQL Server");

        panelSQL.add(new JLabel("Nombre de Conexión:"));
        panelSQL.add(campoNombreSQL);
        panelSQL.add(new JLabel("IP del Servidor:"));
        panelSQL.add(campoIPSQL);
        panelSQL.add(new JLabel("Base de Datos:"));
        panelSQL.add(campoBaseDatosSQL);
        panelSQL.add(new JLabel("Usuario:"));
        panelSQL.add(campoUsuarioSQL);
        panelSQL.add(new JLabel("Contraseña:"));
        panelSQL.add(campoPasswordSQL);
        panelSQL.add(new JLabel("Zona:"));
        panelSQL.add(campoZonaSQL);
        panelSQL.add(botonAgregarSQL);

        botonAgregarSQL.addActionListener(e -> agregarConexionSQL());

        // Panel para Neo4j
        JPanel panelNeo4j = new JPanel(new GridLayout(7, 2));
        panelNeo4j.setBorder(BorderFactory.createTitledBorder("Neo4j"));
        campoNombreNeo4j = new JTextField();
        campoIPNeo4j = new JTextField();
        campoBaseDatosNeo4j = new JTextField();
        campoUsuarioNeo4j = new JTextField();
        campoPasswordNeo4j = new JPasswordField();
        botonAgregarNeo4j = new JButton("Agregar Neo4j");

        panelNeo4j.add(new JLabel("Nombre de Conexión:"));
        panelNeo4j.add(campoNombreNeo4j);
        panelNeo4j.add(new JLabel("IP del Servidor:"));
        panelNeo4j.add(campoIPNeo4j);
        panelNeo4j.add(new JLabel("Base de Datos:"));
        panelNeo4j.add(campoBaseDatosNeo4j);
        panelNeo4j.add(new JLabel("Usuario:"));
        panelNeo4j.add(campoUsuarioNeo4j);
        panelNeo4j.add(new JLabel("Contraseña:"));
        panelNeo4j.add(campoPasswordNeo4j);
        panelNeo4j.add(new JLabel("Zona:"));
        panelNeo4j.add(campoZonaNeo4j);
        panelNeo4j.add(botonAgregarNeo4j);

        botonAgregarNeo4j.addActionListener(e -> agregarConexionNeo4j());

        add(panelSQL);
        add(panelNeo4j);

        setLocationRelativeTo(parent);
        setVisible(true);
    }

    private void agregarConexionSQL() {
        String nombre = campoNombreSQL.getText().trim();
        String ipServidor = campoIPSQL.getText().trim();
        String baseDatos = campoBaseDatosSQL.getText().trim();
        String usuario = campoUsuarioSQL.getText().trim();
        String password = new String(campoPasswordSQL.getPassword()).trim();
        String zona = (String) campoZonaSQL.getSelectedItem();

        if (nombre.isEmpty() || ipServidor.isEmpty() || baseDatos.isEmpty() || usuario.isEmpty()) {
            JOptionPane.showMessageDialog(this, "❌ Todos los campos deben estar llenos.", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        String urlCompleta = PREFIJO_SQL + ipServidor + ":" + PUERTO_SQL + ";databaseName=" + baseDatos
                + ";encrypt=true;trustServerCertificate=true";

        try {
            Connection conn = DriverManager.getConnection(urlCompleta, usuario, password);
            gestorDatos.agregarConexionSQL(nombre, conn, zona);
            JOptionPane.showMessageDialog(this, "✅ Conexión SQL Server agregada correctamente en " + zona);
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "❌ Error al conectar a SQL Server: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void agregarConexionNeo4j() {
        String nombre = campoNombreNeo4j.getText().trim();
        String ipServidor = campoIPNeo4j.getText().trim();
        String baseDatos = campoBaseDatosNeo4j.getText().trim();
        String usuario = campoUsuarioNeo4j.getText().trim();
        String password = new String(campoPasswordNeo4j.getPassword()).trim();
        String zona = (String) campoZonaNeo4j.getSelectedItem();

        if (nombre.isEmpty() || ipServidor.isEmpty() || baseDatos.isEmpty() || usuario.isEmpty()) {
            JOptionPane.showMessageDialog(this, "❌ Todos los campos deben estar llenos.", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        String urlCompleta = PREFIJO_NEO4J + ipServidor + ":" + PUERTO_NEO4J;

        try {
            Driver driver = GraphDatabase.driver(urlCompleta, AuthTokens.basic(usuario, password));
            Session session = driver.session(SessionConfig.forDatabase(baseDatos));
            session.run("RETURN 1").consume();  // Verifica autenticación

            gestorDatos.agregarConexionNeo4j(nombre, session, zona);
            JOptionPane.showMessageDialog(this, "✅ Conexión Neo4j agregada correctamente en " + zona);
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "❌ Error al conectar a Neo4j: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }
}
