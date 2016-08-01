package com.thoughtworks.tca.setup;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MigrateDB {
    private static final Logger LOG = Logger.getLogger(MigrateDB.class.getName());

    public static void main(String[] args)
    {
        C3P0DataSourceProvider dataSourceProvider = new C3P0DataSourceProvider();
        DataSource dataSource = null;
        try {
            dataSource = dataSourceProvider.getDataSource(new JsonObject()
                    .put("url", System.getenv("JDBC_DATABASE_URL")));
            Flyway flyway = new Flyway();
            flyway.setDataSource(dataSource);
            flyway.clean();
            flyway.migrate();
        } catch (SQLException e) {
            LOG.log(Level.SEVERE, "Unable to perform DB migration", e);
        }
    }
}
