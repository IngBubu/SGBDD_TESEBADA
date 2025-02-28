package controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
            // Remove the trailing semicolon if present
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
        String where = (select.getWhere() != null) ? " WHERE " + select.getWhere().toString() : "";
        return "MATCH (n:" + tabla + ") RETURN " + columnas + where;
    }

    private String convertirInsert(Insert insertStmt) {
        String tabla = insertStmt.getTable().getName().toLowerCase();
        List<String> columnas = insertStmt.getColumns().stream()
                .map(Object::toString)
                .collect(Collectors.toList());
        List<String> valores = Arrays.asList(insertStmt.getItemsList().toString().replace("(", "").replace(")", "").split(","));

        String atributos = "{" + columnas.stream()
                .map(c -> c + ": " + valores.get(columnas.indexOf(c)).trim())
                .collect(Collectors.joining(", ")) + "}";

        return "CREATE (n:" + tabla + " " + atributos + ")";
    }

    private String convertirUpdate(Update updateStmt) {
        String tabla = updateStmt.getTable().getName().toLowerCase();
        List<String> setItems = new ArrayList<>();
        for (int i = 0; i < updateStmt.getColumns().size(); i++) {
            setItems.add("n." + updateStmt.getColumns().get(i) + " = " + updateStmt.getExpressions().get(i));
        }
        String where = (updateStmt.getWhere() != null) ? " WHERE " + updateStmt.getWhere().toString() : "";
        return "MATCH (n:" + tabla + ") " + where + " SET " + String.join(", ", setItems);
    }

    private String convertirDelete(Delete deleteStmt) {
        String tabla = deleteStmt.getTable().getName().toLowerCase();
        String where = (deleteStmt.getWhere() != null) ? " WHERE " + deleteStmt.getWhere().toString() : "";
        return "MATCH (n:" + tabla + ") " + where + " DELETE n";
    }
}