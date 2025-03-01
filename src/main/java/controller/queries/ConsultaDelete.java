package controller.queries;

import controller.SQLaCypher;
import org.neo4j.driver.*;
import java.sql.*;

public class ConsultaDelete {
    private SQLaCypher sqlParser;

    public ConsultaDelete() {
        this.sqlParser = new SQLaCypher();
    }

    public boolean ejecutarDeleteSQL(Connection conn, String sql) {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            stmt.executeUpdate();
            conn.commit();
            return true;
        } catch (SQLException e) {
            System.err.println("❌ Error en SQL Server: " + e.getMessage());
            return false;
        }
    }

    public boolean ejecutarDeleteNeo4j(Session session, String sql) {
        try {
            String cypherQuery = sqlParser.convertirSQLaCypher(sql);
            session.run(cypherQuery);
            return true;
        } catch (Exception e) {
            System.err.println("❌ Error en Neo4j: " + e.getMessage());
            return false;
        }
    }
}
