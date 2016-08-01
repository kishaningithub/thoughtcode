package com.thoughtworks;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ThoughtCodeVerticle extends AbstractVerticle {

    private Map<String, JsonObject> products = new HashMap<>();

    private final Logger LOG = Logger.getLogger(ThoughtCodeVerticle.class.getName());

    @Override
    public void start() {

        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());
        router.post("/api/v1/questions").handler(this::handleAddQuestion);
        router.put("/api/v1/questions/:questionID").handler(this::handleUpdateQuestion);
        router.delete("/api/v1/questions/:questionID").handler(this::handleDeleteQuestion);
        router.get("/api/v1/questions/:questionID").handler(this::handleGetQuestion);
        router.get("/api/v1/questions").handler(this::handleListQuestions);


        vertx.createHttpServer(new HttpServerOptions().setCompressionSupported(true))
                .requestHandler(router::accept)
                .listen(Integer.getInteger("http.port"), System.getProperty("http.address", "0.0.0.0"));

    }

    private JDBCClient jdbcClient()
    {
        return JDBCClient.createShared(vertx, new JsonObject()
                .put("url", System.getenv("JDBC_DATABASE_URL")));
    }

    private void handleAddQuestion(RoutingContext routingContext) {
        JsonObject question = routingContext.getBodyAsJson();
        HttpServerResponse response = routingContext.response();
        if(question == null){
            sendError(400, response);
            return;
        }
        jdbcClient().getConnection(conn -> {
            if(conn.failed()){
                LOG.log(Level.SEVERE, "Unable to get DB connection.");
                sendError(500, response);
            }else{
                final SQLConnection connection = conn.result();
                String query = "insert into question(title, description_url, description, is_asked, where_asked, last_updated_dttm) values(?,?,?,?,?,CURRENT_TIMESTAMP)";
                JsonArray params = new JsonArray()
                        .add(question.getValue("title"))
                        .add(question.getValue("descriptionUrl"))
                        .add(question.getValue("description"))
                        .add(question.getValue("isAsked"))
                        .add(question.getValue("whereAsked"));
                connection.updateWithParams(query, params, res -> {
                    if(res.failed()){
                        LOG.log(Level.SEVERE, res.cause().getMessage());
                        sendError(500, response);
                    }else{
                        response.end();
                    }
                    connection.close();
                });
            }
        });

    }

    private void handleUpdateQuestion(RoutingContext routingContext) {
        String questionID = routingContext.request().getParam("questionID");
        HttpServerResponse response = routingContext.response();
        if (questionID == null) {
            sendError(400, response);
        } else {
            JsonObject product = routingContext.getBodyAsJson();
            if (product == null) {
                sendError(400, response);
            } else {
                products.put(questionID, product);
                response.end();
            }
        }
    }

    private void handleDeleteQuestion(RoutingContext routingContext) {

    }

    private void handleListQuestions(RoutingContext routingContext) {
        jdbcClient().getConnection(conn -> {
            if (conn.failed()) {
                LOG.log(Level.SEVERE, "Unable to get DB connection");
            } else {
                final SQLConnection connection = conn.result();
                String query = "select title, description_url descriptionUrl, description description, is_asked isAsked, where_asked whereAsked, last_updated_dttm lastUpdated from question";
                connection.query(query, res -> {
                    ResultSet rs = res.result();
                    List<String> columns = rs.getColumnNames();
                    List<JsonArray> data = rs.getResults();
                    JsonArray arr = new JsonArray(data.stream().map( value ->
                            new JsonObject(IntStream.range(0, columns.size()).boxed().collect(Collectors.toMap(i -> columns.get(i), i -> value.getList().get(i))))
                    ).collect(Collectors.toList()));
                    connection.close();
                    LOG.log(Level.INFO, arr.size() + " questions returned ");
                    routingContext.response().putHeader("content-type", "application/json").end(arr.encodePrettily());
                });

            }
        });
    }

    private void handleGetQuestion(RoutingContext routingContext) {
        String productID = routingContext.request().getParam("productID");
        HttpServerResponse response = routingContext.response();
        if (productID == null) {
            sendError(400, response);
        } else {
            JsonObject product = products.get(productID);
            if (product == null) {
                sendError(404, response);
            } else {
                response.putHeader("content-type", "application/json").end(product.encodePrettily());
            }
        }
    }

    private void sendError(int statusCode, HttpServerResponse response) {
        response.setStatusCode(statusCode).end();
    }

    private void setUpInitialData() {
        addProduct(new JsonObject().put("id", "prod3568").put("name", "Egg Whisk").put("price", 3.99).put("weight", 150));
        addProduct(new JsonObject().put("id", "prod7340").put("name", "Tea Cosy").put("price", 5.99).put("weight", 100));
        addProduct(new JsonObject().put("id", "prod8643").put("name", "Spatula").put("price", 1.00).put("weight", 80));
    }

    private void addProduct(JsonObject product) {
        products.put(product.getString("id"), product);
    }
}
