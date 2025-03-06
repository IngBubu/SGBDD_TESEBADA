package ui;

import com.google.gson.GsonBuilder;
import controller.GestorDeDatos;
import org.neo4j.driver.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

public class ConfiguracionUI extends JFrame {
    private JTextField campoTablaLogica;
    private JTextField campoNombreSQL, campoIPSQL, campoBaseDatosSQL, campoUsuarioSQL, campoTablaSQL;
    private JPasswordField campoPasswordSQL;
    private JTextField campoNombreNeo4j, campoIPNeo4j, campoBaseDatosNeo4j, campoUsuarioNeo4j, campoTablaNeo4j;
    private JPasswordField campoPasswordNeo4j;
    private JButton botonAgregarSQL, botonAgregarNeo4j, botonCargarConfiguraciones;
    private GestorDeDatos gestorDatos;
    private JComboBox<String> campoZonaSQL, campoZonaNeo4j;

    private static final String CONFIG_FILE = "config.json";

    public ConfiguracionUI(JFrame parent, GestorDeDatos gestorDatos) {
        super("Configuración de Conexiones");
        this.gestorDatos = gestorDatos;

        setSize(1000, 320);  // ⬅ Ajuste de tamaño para mejor visibilidad
        setLayout(new BorderLayout());

        // **🔹 Panel Principal con Espaciado**
        JPanel panelPrincipal = new JPanel(new GridLayout(1, 3, 15, 10));  // ⬅ Espaciado mejorado

        // **🔹 Panel Tabla Lógica - Mejor estética**
        JPanel panelTabla = new JPanel(new BorderLayout());
        panelTabla.setBorder(BorderFactory.createTitledBorder("📂 Tabla Lógica"));

        JPanel panelTablaInterno = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.gridx = 0;
        gbc.gridy = 0;
        panelTablaInterno.add(new JLabel("🔹 Nombre Lógico:"), gbc);

        gbc.gridy = 1;
        campoTablaLogica = new JTextField(15);  // ⬅ Reducimos tamaño
        panelTablaInterno.add(campoTablaLogica, gbc);

        panelTabla.add(panelTablaInterno, BorderLayout.CENTER);

        // **🔹 Panel SQL Server**
        JPanel panelSQL = new JPanel(new GridLayout(8, 2, 5, 5));
        panelSQL.setBorder(BorderFactory.createTitledBorder("🖥️ SQL Server"));
        campoNombreSQL = new JTextField();
        campoIPSQL = new JTextField();
        campoBaseDatosSQL = new JTextField();
        campoUsuarioSQL = new JTextField();
        campoPasswordSQL = new JPasswordField();
        campoTablaSQL = new JTextField();
        campoZonaSQL = new JComboBox<>(new String[]{"ZonaNorte", "ZonaCentro", "ZonaSur"});
        botonAgregarSQL = new JButton("Agregar SQL Server");

        panelSQL.add(new JLabel("🔹 Nombre de Conexión:"));
        panelSQL.add(campoNombreSQL);
        panelSQL.add(new JLabel("🌍 IP del Servidor:"));
        panelSQL.add(campoIPSQL);
        panelSQL.add(new JLabel("📂 Base de Datos:"));
        panelSQL.add(campoBaseDatosSQL);
        panelSQL.add(new JLabel("👤 Usuario:"));
        panelSQL.add(campoUsuarioSQL);
        panelSQL.add(new JLabel("🔑 Contraseña:"));
        panelSQL.add(campoPasswordSQL);
        panelSQL.add(new JLabel("📍 Zona:"));
        panelSQL.add(campoZonaSQL);
        panelSQL.add(new JLabel("📌 Nombre Físico:"));
        panelSQL.add(campoTablaSQL);
        panelSQL.add(botonAgregarSQL);
        botonAgregarSQL.addActionListener(e -> agregarConexionSQL());

        // **🔹 Panel Neo4j**
        JPanel panelNeo4j = new JPanel(new GridLayout(8, 2, 5, 5));
        panelNeo4j.setBorder(BorderFactory.createTitledBorder("🌐 Neo4j"));
        campoNombreNeo4j = new JTextField();
        campoIPNeo4j = new JTextField();
        campoBaseDatosNeo4j = new JTextField();
        campoUsuarioNeo4j = new JTextField();
        campoPasswordNeo4j = new JPasswordField();
        campoTablaNeo4j = new JTextField();
        campoZonaNeo4j = new JComboBox<>(new String[]{"ZonaNorte", "ZonaCentro", "ZonaSur"});
        botonAgregarNeo4j = new JButton("Agregar Neo4j");

        panelNeo4j.add(new JLabel("🔹 Nombre de Conexión:"));
        panelNeo4j.add(campoNombreNeo4j);
        panelNeo4j.add(new JLabel("🌍 IP del Servidor:"));
        panelNeo4j.add(campoIPNeo4j);
        panelNeo4j.add(new JLabel("📂 Base de Datos:"));
        panelNeo4j.add(campoBaseDatosNeo4j);
        panelNeo4j.add(new JLabel("👤 Usuario:"));
        panelNeo4j.add(campoUsuarioNeo4j);
        panelNeo4j.add(new JLabel("🔑 Contraseña:"));
        panelNeo4j.add(campoPasswordNeo4j);
        panelNeo4j.add(new JLabel("📍 Zona:"));
        panelNeo4j.add(campoZonaNeo4j);
        panelNeo4j.add(new JLabel("📌 Nombre Físico:"));
        panelNeo4j.add(campoTablaNeo4j);
        panelNeo4j.add(botonAgregarNeo4j);
        botonAgregarNeo4j.addActionListener(e -> agregarConexionNeo4j());

        // **🔹 Botón de Cargar Configuración**
        botonCargarConfiguraciones = new JButton("🔄 Cargar Configuración");
        botonCargarConfiguraciones.addActionListener(e -> cargarConfiguracionManual());

        // **🔹 Agregar los paneles al principal**
        panelPrincipal.add(panelTabla);
        panelPrincipal.add(panelSQL);
        panelPrincipal.add(panelNeo4j);

        // **🔹 Agregar todo al Frame**
        add(panelPrincipal, BorderLayout.CENTER);
        add(botonCargarConfiguraciones, BorderLayout.SOUTH);

        setLocationRelativeTo(parent);
        setVisible(true);
    }

    private void cargarConfiguracionManual() {
        cargarConfiguracion();
        JOptionPane.showMessageDialog(this, "✅ Configuraciones y mapeos cargados correctamente.", "Carga Exitosa", JOptionPane.INFORMATION_MESSAGE);
    }

    private void guardarConfiguracion(String nombre, String ip, String baseDatos, String usuario, String password, String zona, String tablaLogica, String tablaFisica) {
        Map<String, Object> configData = cargarConfiguracion();

        Map<String, Map<String, String>> conexiones = (Map<String, Map<String, String>>) configData.getOrDefault("conexiones", new HashMap<>());
        Map<String, Map<String, String>> mapeoTablas = (Map<String, Map<String, String>>) configData.getOrDefault("mapeoTablas", new HashMap<>());

        conexiones.put(nombre, Map.of("IP", ip, "BaseDatos", baseDatos, "Usuario", usuario, "Password", password, "Zona", zona));
        mapeoTablas.putIfAbsent(tablaLogica.toLowerCase(), new HashMap<>());
        mapeoTablas.get(tablaLogica.toLowerCase()).put(zona, tablaFisica);

        configData.put("conexiones", conexiones);
        configData.put("mapeoTablas", mapeoTablas);

        try (FileWriter file = new FileWriter(CONFIG_FILE)) {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            file.write(gson.toJson(configData));

        } catch (IOException e) {
            System.err.println("❌ Error al guardar config.json: " + e.getMessage());
        }
    }

    private Map<String, Object> cargarConfiguracion() {
        try (Reader reader = new FileReader(CONFIG_FILE)) {
            return new Gson().fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
        } catch (IOException e) {
            System.out.println("⚠️ No se encontró archivo de configuración. Se creará uno nuevo.");
            return new HashMap<>(); // Retorna un mapa vacío si no hay configuración previa
        }
    }


    private void agregarConexionSQL() {
        String nombre = campoNombreSQL.getText().trim();
        String ipServidor = campoIPSQL.getText().trim();
        String baseDatos = campoBaseDatosSQL.getText().trim();
        String usuario = campoUsuarioSQL.getText().trim();
        String password = new String(campoPasswordSQL.getPassword()).trim();
        String zona = (String) campoZonaSQL.getSelectedItem();
        String tablaFisica = campoTablaSQL.getText().trim();
        String tablaLogica = campoTablaLogica.getText().trim().toLowerCase();

        if (nombre.isEmpty() || ipServidor.isEmpty() || baseDatos.isEmpty() || usuario.isEmpty() || tablaFisica.isEmpty() || tablaLogica.isEmpty()) {
            JOptionPane.showMessageDialog(this, "❌ Todos los campos deben estar llenos.", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        String urlCompleta = "jdbc:sqlserver://" + ipServidor + ":1433;databaseName=" + baseDatos + ";encrypt=true;trustServerCertificate=true";

        try {
            Connection conn = DriverManager.getConnection(urlCompleta, usuario, password);
            gestorDatos.agregarConexionSQL(nombre, conn, zona);
            guardarConfiguracion(nombre, ipServidor, baseDatos, usuario, password, zona, tablaLogica, tablaFisica);
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
        String tablaFisica = campoTablaNeo4j.getText().trim();
        String tablaLogica = campoTablaLogica.getText().trim().toLowerCase();

        if (nombre.isEmpty() || ipServidor.isEmpty() || baseDatos.isEmpty() || usuario.isEmpty() || tablaFisica.isEmpty() || tablaLogica.isEmpty()) {
            JOptionPane.showMessageDialog(this, "❌ Todos los campos deben estar llenos.", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        String urlCompleta = "bolt://" + ipServidor + ":7687";

        try {
            Driver driver = GraphDatabase.driver(urlCompleta, AuthTokens.basic(usuario, password));
            Session session = driver.session(SessionConfig.forDatabase(baseDatos));
            session.run("RETURN 1").consume();  // Verifica autenticación

            gestorDatos.agregarConexionNeo4j(nombre, session, zona);
            guardarConfiguracion(nombre, ipServidor, baseDatos, usuario, password, zona, tablaLogica, tablaFisica);
            JOptionPane.showMessageDialog(this, "✅ Conexión Neo4j agregada correctamente en " + zona);
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "❌ Error al conectar a Neo4j: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }


}
