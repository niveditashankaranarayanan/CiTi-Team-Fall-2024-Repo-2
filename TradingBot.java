import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TradingBot {

  private static final Gson gson = new Gson();

  public static Map<String, List<Integer>> aggressiveOrders(
      String sym, String action, int qty, List<String> bookLevels,
      Map<String, Map<String, Map<String, Double>>> marketDictLocal,
      String bookSide, String side, double vwapSym, String strategy,
      Integer internalID, Integer size) {

    List<Integer> orderPrices = new ArrayList<>();
    List<Integer> orderQty = new ArrayList<>();

    String level = bookLevels.remove(0);
    try {
      int levelSize =
          ((Number)marketDictLocal.get(sym).get(level).get(bookSide + "Size"))
              .intValue();
      int levelPrice = ((Number)marketDictLocal.get(sym).get(level).get(
                            bookSide + "Price"))
                           .intValue();

      System.out.println(String.format("Level is: %s, size in the level is: %d",
                                       level, levelSize));
      System.out.println(
          String.format("Price in the level is: %d", levelPrice));

      int sizeLevel =
          ((Number)Math.min(qty - size, marketDictLocal.get(sym).get(level).get(
                                            bookSide + "Size")))
              .intValue();
      size += sizeLevel;

      orderPrices.add(levelPrice);
      orderQty.add(sizeLevel);

      System.out.println("Order created: Symbol=" + sym +
                         ", Price=" + levelPrice + ", Quantity=" + sizeLevel);

    } catch (Exception e) {
      System.err.println("Error Occurred while executing Aggressive Orders.");
      e.printStackTrace();
    }

    Map<String, List<Integer>> response = new HashMap<>();
    response.put("order_prices", orderPrices);
    response.put("order_qty", orderQty);

    List<Integer> sizeList = new ArrayList<>(Collections.singletonList(size));
    response.put("size", sizeList);

    return response;
  }

  public static Map<String, List<Integer>>
  placeOrders(String sym, String action, int qty, int nSlices,
              List<String> bookLevels,
              Map<String, Map<String, Map<String, Double>>> marketDictLocal,
              String bookSide, String side, double vwapSym, double preVwap,
              int n_slices_iterator, String strategy, Integer internalID,
              Integer size, Integer targetQ) {

    List<Integer> orderPrices = new ArrayList<>();
    List<Integer> orderQty = new ArrayList<>();

    String level = bookLevels.remove(0);
    try {
      int levelSize =
          ((Number)marketDictLocal.get(sym).get(level).get(bookSide + "Size"))
              .intValue();
      int levelPrice = ((Number)marketDictLocal.get(sym).get(level).get(
                            bookSide + "Price"))
                           .intValue();

      int sizeLevel = Math.min(targetQ - size, levelSize);
      size += sizeLevel;

      orderPrices.add(levelPrice);
      orderQty.add(sizeLevel);

      System.out.println("Order created: Symbol=" + sym +
                         ", Price=" + levelPrice + ", Quantity=" + sizeLevel);

    } catch (Exception e) {
      System.err.println("Error Occurred while executing placeOrders.");
      e.printStackTrace();
    }

    Map<String, List<Integer>> response = new HashMap<>();
    response.put("order_prices", orderPrices);
    response.put("order_qty", orderQty);

    List<Integer> sizeList = new ArrayList<>(Collections.singletonList(size));
    response.put("size", sizeList);

    return response;
  }

  public static void startServer() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
    server.createContext("/api/placeOrders", new OrderHandler());
    server.createContext("/api/placeAggressiveOrders",
                         new AggressiveOrderHandler());

    server.createContext("/api/test", exchange -> {
      String response = "Test endpoint is working";
      exchange.sendResponseHeaders(200, response.getBytes().length);
      OutputStream os = exchange.getResponseBody();
      os.write(response.getBytes());
      os.close();
    });
    server.createContext("/api/testPost", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        if ("POST".equals(exchange.getRequestMethod())) {
          BufferedReader reader = new BufferedReader(new InputStreamReader(
              exchange.getRequestBody(), StandardCharsets.UTF_8));
          StringBuilder body = new StringBuilder();
          String line;
          while ((line = reader.readLine()) != null) {
            body.append(line);
          }

          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.toString().getBytes().length);
          OutputStream outputStream = exchange.getResponseBody();
          outputStream.write(body.toString().getBytes());
          outputStream.close();
        } else {
          exchange.sendResponseHeaders(405, -1);
        }
      }
    });
    server.setExecutor(null);
    server.start();
    System.out.println("Server started on port 8000");
  }

  static class OrderHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if ("POST".equals(exchange.getRequestMethod())) {

        InputStreamReader reader =
            new InputStreamReader(exchange.getRequestBody());
        Type payloadType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> payload = gson.fromJson(reader, payloadType);

        String sym = (String)payload.get("sym");
        String action = (String)payload.get("action");
        int qty = ((Double)payload.get("qty")).intValue();
        int nSlices = ((Double)payload.get("n_slices")).intValue();
        @SuppressWarnings("unchecked")
        List<String> bookLevels = (List<String>)payload.get("book_levels");
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Map<String, Double>>> marketDictLocal =
            (Map<String, Map<String, Map<String, Double>>>)payload.get(
                "market_dict_local");
        String bookSide = (String)payload.get("book_side");
        String side = (String)payload.get("side");
        double vwapSym = ((Double)payload.get("vwap_sym"));
        double preVwap = ((Double)payload.get("pre_vwap"));
        int n_slices_iterator =
            ((Double)payload.get("n_slices_iterator")).intValue();
        String strategy = (String)payload.get("strategy");
        int internalID = ((Double)payload.get("internalID")).intValue();
        int size = ((Double)payload.get("size")).intValue();
        int targetQ = ((Double)payload.get("targetQ")).intValue();

        System.out.println("###########################");
        System.out.println(sym);
        System.out.println(action);
        System.out.println(nSlices);
        System.out.println(bookLevels);
        System.out.println(marketDictLocal);
        System.out.println(bookSide);
        System.out.println(side);
        System.out.println(vwapSym);
        System.out.println(preVwap);
        System.out.println(n_slices_iterator);
        System.out.println(strategy);
        System.out.println(internalID);
        System.out.println(size);
        System.out.println(targetQ);
        System.out.println("###########################");

        Map<String, List<Integer>> orders =
            placeOrders(sym, action, qty, nSlices, bookLevels, marketDictLocal,
                        bookSide, side, vwapSym, preVwap, n_slices_iterator,
                        strategy, internalID, size, targetQ);

        String response = gson.toJson(orders);
        exchange.sendResponseHeaders(200, response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
      } else if ("GET".equals(exchange.getRequestMethod())) {
        System.out.println("GET IS WORKING");
        String response = "placeOrders GET endpoint is working";
        exchange.sendResponseHeaders(200, response.getBytes().length);
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
      } else {
        exchange.sendResponseHeaders(405, -1);
      }
    }
  }

  static class AggressiveOrderHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      if ("POST".equals(exchange.getRequestMethod())) {

        InputStreamReader reader =
            new InputStreamReader(exchange.getRequestBody());
        Type payloadType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> payload = gson.fromJson(reader, payloadType);

        String sym = (String)payload.get("sym");
        String action = (String)payload.get("action");
        int qty = ((Double)payload.get("qty")).intValue();
        @SuppressWarnings("unchecked")
        List<String> bookLevels = (List<String>)payload.get("book_levels");
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Map<String, Double>>> marketDictLocal =
            (Map<String, Map<String, Map<String, Double>>>)payload.get(
                "market_dict_local");
        String bookSide = (String)payload.get("book_side");
        String side = (String)payload.get("side");
        double vwapSym = ((Double)payload.get("vwap_sym"));
        String strategy = (String)payload.get("strategy");
        int internalID = ((Double)payload.get("internalID")).intValue();
        int size = ((Double)payload.get("size")).intValue();

        System.out.println("###########################");
        System.out.println(sym);
        System.out.println(action);
        System.out.println(bookLevels);
        System.out.println(marketDictLocal);
        System.out.println(bookSide);
        System.out.println(side);
        System.out.println(vwapSym);
        System.out.println(strategy);
        System.out.println(internalID);
        System.err.println(size);
        System.out.println("###########################");

        Map<String, List<Integer>> orders = aggressiveOrders(
            sym, action, qty, bookLevels, marketDictLocal, bookSide, side,
            vwapSym, strategy, internalID, size);

        String response = gson.toJson(orders);
        exchange.sendResponseHeaders(200, response.length());
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
      } else if ("GET".equals(exchange.getRequestMethod())) {
        System.out.println("GET IS WORKING");
        String response = "placeOrders GET endpoint is working";
        exchange.sendResponseHeaders(200, response.getBytes().length);
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
      } else {
        exchange.sendResponseHeaders(405, -1);
      }
    }
  }

  public static void main(String[] args) throws IOException { startServer(); }
}
