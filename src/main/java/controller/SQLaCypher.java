package controller;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.schema.Table;
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

        // Asegurar que la condición WHERE referencia el alias 'n'
        String where = "";
        if (select.getWhere() != null) {
            where = " WHERE " + select.getWhere().toString().replaceAll("(\\b[a-zA-Z_]+\\b)", "n.$1");
        }

        return "MATCH (n:" + tabla + ")" + where + " RETURN n";
    }



    private String convertirInsert(Insert insertStmt) {
        String tabla = insertStmt.getTable().getName().toLowerCase();
        String columnas = insertStmt.getColumns().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        String valores = insertStmt.getItemsList().toString().replace("(", "{").replace(")", "}");

        return "CREATE (n:" + tabla + " " + valores + ")";
    }

    private String convertirUpdate(Update updateStmt) {
        String tabla = updateStmt.getTable().getName().toLowerCase();
        String setClause = updateStmt.getColumns().stream()
                .map(col -> "n." + col + " = " + updateStmt.getExpressions().get(updateStmt.getColumns().indexOf(col)))
                .collect(Collectors.joining(", "));
        String where = (updateStmt.getWhere() != null) ? " WHERE " + updateStmt.getWhere().toString() : "";

        return "MATCH (n:" + tabla + ") " + where + " SET " + setClause;
    }

    private String convertirDelete(Delete deleteStmt) {
        String tabla = deleteStmt.getTable().getName().toLowerCase();
        String where = (deleteStmt.getWhere() != null) ? " WHERE " + deleteStmt.getWhere().toString() : "";

        return "MATCH (n:" + tabla + ") " + where + " DELETE n";
    }
}
