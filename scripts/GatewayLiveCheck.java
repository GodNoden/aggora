import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Comprueba de punta a punta que el gateway-ws reparte eventos de verdad.
 *
 * Habla WebSocket con la libreria del JDK (java.net.http.WebSocket), asi que no hace falta
 * instalar nada: ni un cliente de Kafka ni una libreria de websockets. Se conecta, escucha
 * unos segundos y falla si no llega ningun tick.
 *
 *     java scripts/GatewayLiveCheck.java [host] [puerto] [segundos]
 *
 * Sale con codigo 0 solo si el reparto funciona.
 */
public class GatewayLiveCheck {

    private static final Pattern KIND = Pattern.compile("\"kind\"\\s*:\\s*\"([a-z]+)\"");

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int puerto = args.length > 1 ? Integer.parseInt(args[1]) : 8089;
        long milisegundos = args.length > 2 ? Long.parseLong(args[2]) * 1000 : 12_000;

        Map<String, Integer> cuenta = new TreeMap<>();
        Map<String, String> ejemplo = new LinkedHashMap<>();
        CountDownLatch cerrado = new CountDownLatch(1);

        WebSocket.Listener escucha = new WebSocket.Listener() {
            private final StringBuilder trozos = new StringBuilder();

            @Override
            public CompletionStage<?> onText(WebSocket ws, CharSequence datos, boolean ultimo) {
                trozos.append(datos);
                if (ultimo) {
                    String json = trozos.toString();
                    trozos.setLength(0);
                    Matcher m = KIND.matcher(json);
                    String tipo = m.find() ? m.group(1) : "?";
                    cuenta.merge(tipo, 1, Integer::sum);
                    ejemplo.putIfAbsent(tipo, json.length() > 130 ? json.substring(0, 130) + "..." : json);
                }
                ws.request(1);
                return null;
            }

            @Override
            public CompletionStage<?> onClose(WebSocket ws, int codigo, String motivo) {
                System.out.println("el servidor cerro la conexion: " + codigo + " " + motivo);
                cerrado.countDown();
                return null;
            }

            @Override
            public void onError(WebSocket ws, Throwable fallo) {
                System.out.println("error en el websocket: " + fallo);
                cerrado.countDown();
            }
        };

        URI uri = URI.create("ws://" + host + ":" + puerto + "/ws");
        WebSocket ws = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build()
                .newWebSocketBuilder()
                .buildAsync(uri, escucha)
                .get(10, TimeUnit.SECONDS);
        System.out.println("handshake OK con " + uri);

        Thread.sleep(milisegundos);

        System.out.println("--- eventos recibidos en " + (milisegundos / 1000) + " s");
        cuenta.forEach((tipo, veces) ->
                System.out.printf("  %-9s x%-5d ej: %s%n", tipo, veces, ejemplo.get(tipo)));
        if (cuenta.isEmpty()) {
            System.out.println("  (ninguno)");
        }
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "fin del check").get(5, TimeUnit.SECONDS);
        cerrado.await(2, TimeUnit.SECONDS);

        if (!cuenta.containsKey("tick")) {
            System.err.println("FALLO: no llego ningun evento de tipo tick en " + (milisegundos / 1000) + " s");
            System.exit(1);
        }
        System.out.println("OK: el gateway reparte " + cuenta.size() + " tipos de evento");
    }
}
