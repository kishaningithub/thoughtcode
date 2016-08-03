package com.thoughtworks.tca.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
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
import io.vertx.ext.web.handler.CorsHandler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ThoughtCodeVerticle extends AbstractVerticle {

    private final Logger LOG = Logger.getLogger(ThoughtCodeVerticle.class.getName());

    @Override
    public void start() {

        Router router = Router.router(vertx);
        Set<HttpMethod> allowedHTTPMethods = new HashSet<>();
        allowedHTTPMethods.add(HttpMethod.GET);
        allowedHTTPMethods.add(HttpMethod.POST);
        allowedHTTPMethods.add(HttpMethod.PUT);
        allowedHTTPMethods.add(HttpMethod.DELETE);
        router.route().handler(CorsHandler.create("*").allowedMethods(allowedHTTPMethods));

        router.route().handler(BodyHandler.create());
        router.post("/api/v1/questions").handler(this::handleAddQuestion);
        router.patch("/api/v1/questions/:questionID").handler(this::handleUpdateQuestion);
        router.delete("/api/v1/questions/:questionID").handler(this::handleDeleteQuestion);
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
                String query = "insert into question(description_url, coding_round) values(?, ?)";
                JsonArray params = new JsonArray()
                        .add(question.getValue("descriptionUrl"))
                        .add(question.getValue("codingRound"));
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
            JsonObject question = routingContext.getBodyAsJson();
            if (question == null) {
                sendError(400, response);
            } else {
                jdbcClient().getConnection(conn -> {
                    if(conn.failed()){
                        LOG.log(Level.SEVERE, "Unable to get DB connection.");
                        sendError(500, response);
                    }else{
                        final SQLConnection connection = conn.result();
                        String query = "update question set where_asked = ? where qid = ?";
                        JsonArray params = new JsonArray()
                                .add(question.getValue("whereAsked"))
                                .add(Integer.parseInt(questionID));
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
        }
    }

    private void handleDeleteQuestion(RoutingContext routingContext) {
        String questionID = routingContext.request().getParam("questionID");
        HttpServerResponse response = routingContext.response();
        if (questionID == null) {
            sendError(400, response);
        } else {
            jdbcClient().getConnection(conn -> {
                if (conn.failed()) {
                    LOG.log(Level.SEVERE, "Unable to get DB connection.");
                    sendError(500, response);
                } else {
                    final SQLConnection connection = conn.result();
                    String query = "delete from question where qid = ?";
                    JsonArray params = new JsonArray()
                            .add(Integer.parseInt(questionID));
                    connection.updateWithParams(query, params, res -> {
                        if (res.failed()) {
                            LOG.log(Level.SEVERE, res.cause().getMessage());
                            sendError(500, response);
                        } else {
                            response.end();
                        }
                        connection.close();
                    });
                }
            });
        }
    }

    private void handleListQuestions(RoutingContext routingContext) {
        jdbcClient().getConnection(conn -> {
            if (conn.failed()) {
                LOG.log(Level.SEVERE, "Unable to get DB connection");
            } else {
                final SQLConnection connection = conn.result();
                String query = "select description_url descriptionUrl, where_asked whereAsked from question order by where_asked";
                connection.query(query, res -> {
                    if(res.failed()){
                        LOG.log(Level.SEVERE, "Unable to fetch query result");
                        connection.close();
                    }else{
                        ResultSet rs = res.result();
                        List<String> columns = rs.getColumnNames();
                        List<JsonArray> data = rs.getResults();
                        JsonArray arr = new JsonArray(data.stream().map( value ->
                                new JsonObject(IntStream.range(0, columns.size()).boxed().collect(Collectors.toMap(i -> columns.get(i), i -> value.getList().get(i))))
                        ).collect(Collectors.toList()));
                        connection.close();
                        LOG.log(Level.INFO, arr.size() + " questions returned ");
                        routingContext.response().putHeader("content-type", "application/json").end(arr.encodePrettily());
                    }
                });
            }
        });
    }

    private void sendError(int statusCode, HttpServerResponse response) {
        response.setStatusCode(statusCode).end();
    }

}
