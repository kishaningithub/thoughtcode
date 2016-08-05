package com.thoughtworks.tca.verticle;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.TrustOptions;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import okhttp3.*;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ThoughtCodeVerticle extends AbstractVerticle {

    private final Logger LOG = Logger.getLogger(ThoughtCodeVerticle.class.getName());

    private final OkHttpClient client = new OkHttpClient();

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
                        .add(question.getValue("descriptionURL"))
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
                String query = "select description_url, coding_round, where_asked from question order by where_asked";
                connection.query(query, res -> {
                    if(res.failed()){
                        LOG.log(Level.SEVERE, "Unable to fetch query result");
                        connection.close();
                    }else{
                        ResultSet rs = res.result();
                        List<JsonArray> data = rs.getResults();
                        LOG.log(Level.INFO, "Data size " + ((data == null)? "null":data.size()));
                        // Getting from google service
                        List<String> descriptionURLLst = data.stream()
                                .map(jsonArr -> jsonArr.getString(0))
                                .collect(Collectors.toList());

                        JsonArray descriptonURLArr = new JsonArray(descriptionURLLst);
                        String serviceURL = "https://script.google.com/macros/s/AKfycbwRVDKt5ApSadrc04rBUEugnWxNmY6iHpMgLxScBSamPmHmCzxl/exec";

                        Request request = new Request.Builder()
                                .url(serviceURL)
                                .post(RequestBody.create(MediaType.parse("application/json; charset=utf-8"), descriptonURLArr.encode()))
                                .build();

                        Map<String, JsonObject> descriptionURLJSONMap = new HashMap<>();
                        try {
                            Response response = client.newCall(request).execute();
                            if(response.isSuccessful()){
                                String serviceResponse = response.body().string();
                                LOG.log(Level.INFO, "Google response " + serviceResponse);
                                JsonArray serviceRespJA = new JsonArray(serviceResponse);
                                for(int i = 0; i < serviceRespJA.size(); i++){
                                    JsonObject ithObject = new JsonObject(serviceRespJA.getString(i));
                                    descriptionURLJSONMap.put(ithObject.getString("descriptionURL"), ithObject);
                                }
                            }
                        } catch (IOException e) {
                            LOG.log(Level.SEVERE, "Unable to process http request");
                        }

                        JsonArray returnArray = new JsonArray();
                        data.forEach(row -> {
                            JsonObject jsonObject = new JsonObject();
                            jsonObject.put("descriptionURL", row.getValue(0));
                            jsonObject.put("codingRound", row.getValue(1));
                            jsonObject.put("whereAsked", row.getValue(2));
                            jsonObject.mergeIn(descriptionURLJSONMap.get(jsonObject.getString("descriptionURL")));
                            returnArray.add(jsonObject);
                        });
                        connection.close();
                        LOG.log(Level.INFO, returnArray.size() + " questions returned ");
                        routingContext.response().putHeader("content-type", "application/json").end(returnArray.encode());
                    }
                });
            }
        });
    }

    private void sendError(int statusCode, HttpServerResponse response) {
        response.setStatusCode(statusCode).end();
    }

}
