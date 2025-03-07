package controller;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.schema.Table;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SQLaCypher {
    public String convertirSQLaCypher(String sql) {
        try {
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            Statement stmt = CCJSqlParserUtil.parse(sql);
            if (stmt instanceof Select) {
                System.out.println("✅ Consulta SQL válida para conversión a Cypher.");
                return convertirSelect((Select) stmt);
            } else if (stmt instanceof Insert) {
                System.out.println("✅ Consulta SQL válida para conversión a Cypher.");
                return convertirInsert((Insert) stmt);
            } else if (stmt instanceof Update) {
                System.out.println("✅ Consulta SQL válida para conversión a Cypher.");
                return convertirUpdate((Update) stmt);
            } else if (stmt instanceof Delete) {
                System.out.println("✅ Consulta SQL válida para conversión a Cypher.");
                return convertirDelete((Delete) stmt);
            }
        } catch (Exception e) {
            System.err.println("❌ Error al convertir SQL a Cypher: " + e.getMessage());
        }
        return "";
    }

    private String convertirSelect(Select selectStmt) {
        PlainSelect select = (PlainSelect) selectStmt.getSelectBody();
        String tabla = ((Table) select.getFromItem()).getName().toLowerCase();
        String columnas = select.getSelectItems().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));

        // Asegurar que la condición WHERE referencia el alias 'n'
        String where = "";
        if (select.getWhere() != null) {
            where = " WHERE " + select.getWhere().toString().replaceAll("(\\b[a-zA-Z_]+\\b)", "n.$1");
        }

        return "MATCH (n:" + tabla + ")" + where + " RETURN n";
    }

    private String convertirInsert(Insert insertStmt) {
        String tabla = insertStmt.getTable().getName().toLowerCase();

        // Obtener nombres de columnas y valores
        List<String> columnas = insertStmt.getColumns().stream()
                .map(Object::toString)
                .collect(Collectors.toList());

        List<String> valores = Arrays.asList(insertStmt.getItemsList().toString()
                .replace("(", "").replace(")", "").split(","));

        // Construir el JSON de propiedades en formato Cypher
        StringBuilder atributos = new StringBuilder("{");
        for (int i = 0; i < columnas.size(); i++) {
            atributos.append(columnas.get(i).trim()).append(": ").append(valores.get(i).trim());
            if (i < columnas.size() - 1) {
                atributos.append(", ");
            }
        }
        atributos.append("}");

        return "CREATE (n:" + tabla + " " + atributos + ")";
    }

    private String convertirUpdate(Update updateStmt) {
        String tabla = updateStmt.getTable().getName().toLowerCase();

        // Construir la cláusula SET en Cypher
        String setClause = updateStmt.getColumns().stream()
                .map(col -> "n." + col + " = " + updateStmt.getExpressions().get(updateStmt.getColumns().indexOf(col)))
                .collect(Collectors.joining(", "));

        // Construir la cláusula WHERE correctamente
        String where = "";
        if (updateStmt.getWhere() != null) {
            where = updateStmt.getWhere().toString()
                    .replace("=", " = ")  // Asegurar espacios correctos
                    .replaceAll("(\\b[a-zA-Z_]+\\b)", "n.$1");  // Prefija los campos con 'n.'

            // Asegurar que IdCliente es tratado como un número en Cypher
            where = where.replace("n.idcliente =", "n.idcliente =");
        }

        return "MATCH (n:" + tabla + ") WHERE " + where + " SET " + setClause;
    }

    private String convertirDelete(Delete deleteStmt) {
        String tabla = deleteStmt.getTable().getName().toLowerCase();

        // Obtener condición WHERE si existe
        String where = "";
        if (deleteStmt.getWhere() != null) {
            where = deleteStmt.getWhere().toString().replace("=", ":"); // Reemplazar "=" por ":"
            where = where.replaceAll("(\\w+)\\s*:", "n.$1 ="); // Asegurar que las propiedades estén referenciadas con "n."
        }

        // Construir la consulta Cypher corregida
        if (!where.isEmpty()) {
            return "MATCH (n:" + tabla + ") WHERE " + where + " DELETE n";
        } else {
            return "MATCH (n:" + tabla + ") DELETE n"; // Borra todos los nodos de la tabla (¡cuidado!)
        }
    }
}
