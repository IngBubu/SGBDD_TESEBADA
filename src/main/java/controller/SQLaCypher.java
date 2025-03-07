package controller;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;

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
                return convertirSelect((Select) stmt);
            } else if (stmt instanceof Insert) {
                return convertirInsert((Insert) stmt);
            } else if (stmt instanceof Update) {
                return convertirUpdate((Update) stmt);
            } else if (stmt instanceof Delete) {
                return convertirDelete((Delete) stmt);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    private String convertirSelect(Select selectStmt) {
        PlainSelect select = (PlainSelect) selectStmt.getSelectBody();
        String tabla = ((Table) select.getFromItem()).getName().toLowerCase();
        String columnas = select.getSelectItems().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));

        // Construcción correcta de WHERE
        String where = "";
        if (select.getWhere() != null) {
            where = " WHERE " + transformarCondicionWhere(select.getWhere().toString());
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

        // Construcción del JSON de propiedades en formato Cypher
        StringBuilder atributos = new StringBuilder("{");
        for (int i = 0; i < columnas.size(); i++) {
            String valor = valores.get(i).trim();
            // Agregar comillas a valores de texto
            if (!valor.matches("-?\\d+(\\.\\d+)?")) {
                valor = "'" + valor + "'";
            }
            atributos.append(columnas.get(i).trim()).append(": ").append(valor);
            if (i < columnas.size() - 1) {
                atributos.append(", ");
            }
        }
        atributos.append("}");

        return "CREATE (n:" + tabla + " " + atributos + ")";
    }

    private String convertirUpdate(Update updateStmt) {
        String tabla = updateStmt.getTable().getName().toLowerCase();

        // Construcción de la cláusula SET en Cypher
        String setClause = updateStmt.getColumns().stream()
                .map(col -> "n." + col + " = " + (updateStmt.getExpressions().get(updateStmt.getColumns().indexOf(col)) instanceof StringValue
                        ? "'" + updateStmt.getExpressions().get(updateStmt.getColumns().indexOf(col)).toString().replace("'", "") + "'"
                        : updateStmt.getExpressions().get(updateStmt.getColumns().indexOf(col)).toString()))
                .collect(Collectors.joining(", "));

        // Construcción de la cláusula WHERE correctamente
        String where = "";
        if (updateStmt.getWhere() != null) {
            where = " WHERE " + transformarCondicionWhere(updateStmt.getWhere().toString());
        }

        return "MATCH (n:" + tabla + ")" + where + " SET " + setClause;
    }

    private String convertirDelete(Delete deleteStmt) {
        String tabla = deleteStmt.getTable().getName().toLowerCase();

        // Obtener condición WHERE si existe
        String where = "";
        if (deleteStmt.getWhere() != null) {
            where = " WHERE " + transformarCondicionWhere(deleteStmt.getWhere().toString());
        }

        return "MATCH (n:" + tabla + ")" + where + " DELETE n";
    }

    private String transformarCondicionWhere(String where) {
        return where.replaceAll("(\\b[a-zA-Z_]+\\b)\\s*=\\s*('?\\b[a-zA-Z_0-9]+\\b'?)", "n.$1 = $2");
    }
}
