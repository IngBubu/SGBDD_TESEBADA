package controller;

public class SQLaCypher {

    public String convertirSQLaCypher(String sql) {
        sql = sql.trim().toUpperCase().replace(";", ""); // Elimina el punto y coma

        if (sql.startsWith("SELECT")) {
            return convertirSelect(sql);
        } else if (sql.startsWith("INSERT")) {
            return convertirInsert(sql);
        } else if (sql.startsWith("UPDATE")) {
            return convertirUpdate(sql);
        } else if (sql.startsWith("DELETE")) {
            return convertirDelete(sql);
        } else {
            throw new IllegalArgumentException("Consulta no soportada: " + sql);
        }
    }

    private String convertirSelect(String sql) {
        // Manejar diferentes formatos de SELECT
        if (!sql.contains("FROM")) {
            throw new IllegalArgumentException("Consulta SELECT mal formada: " + sql);
        }

        String[] partes = sql.split("FROM");
        if (partes.length < 2) {
            throw new IllegalArgumentException("No se encontró la tabla en la consulta SELECT: " + sql);
        }

        String tabla = partes[1].trim().split(" ")[0]; // Extrae solo el nombre de la tabla sin espacios adicionales

        if (tabla.equalsIgnoreCase("CLIENTES")) {
            tabla = "clientes"; // Asegura que coincida con la etiqueta en Neo4j
        }

        return "MATCH (n:" + tabla + ") RETURN n";
    }

    private String convertirInsert(String sql) {
        // INSERT INTO clientes (nombre, edad) VALUES ('Juan', 30)
        String[] partes = sql.split("VALUES");
        String tabla = partes[0].split("INTO")[1].split("\\(")[0].trim();
        String columnas = partes[0].split("\\(")[1].split("\\)")[0].trim();
        String valores = partes[1].split("\\(")[1].split("\\)")[0].trim();

        String[] columnasArray = columnas.split(",");
        String[] valoresArray = valores.split(",");

        StringBuilder query = new StringBuilder("CREATE (n:" + tabla + " {");
        for (int i = 0; i < columnasArray.length; i++) {
            query.append(columnasArray[i].trim()).append(": ").append(valoresArray[i].trim());
            if (i < columnasArray.length - 1) query.append(", ");
        }
        query.append("})");

        return query.toString();
    }

    private String convertirUpdate(String sql) {
        // UPDATE clientes SET edad = 35 WHERE nombre = 'Juan'
        String[] partes = sql.split("SET");
        String tabla = partes[0].split("UPDATE")[1].trim();
        String cambios = partes[1].split("WHERE")[0].trim();
        String condicion = partes.length > 1 ? partes[1].split("WHERE")[1].trim() : "";

        if (tabla.equalsIgnoreCase("CLIENTES")) {
            tabla = "clientes";
        }

        return "MATCH (n:" + tabla + ") WHERE " + condicion + " SET " + cambios;
    }

    private String convertirDelete(String sql) {
        // DELETE FROM clientes WHERE nombre = 'Juan'
        String[] partes = sql.split("FROM");
        String tabla = partes[1].split("WHERE")[0].trim();
        String condicion = partes.length > 1 ? partes[1].split("WHERE")[1].trim() : "";

        if (tabla.equalsIgnoreCase("CLIENTES")) {
            tabla = "clientes";
        }

        return "MATCH (n:" + tabla + ") WHERE " + condicion + " DELETE n";
    }
}
