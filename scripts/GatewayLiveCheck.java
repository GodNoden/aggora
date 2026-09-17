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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Comprueba de punta a punta que el gateway-ws reparte eventos de verdad y con el contrato nuevo.
 *
 * Habla WebSocket con la libreria del JDK (java.net.http.WebSocket), asi que no hace falta
 * instalar nada: ni un cliente de Kafka ni una libreria de websockets. Se conecta, escucha
 * unos segundos, cuenta los tipos de mensaje y FALLA si no llega ningun snapshot valido.
 *
 * El contrato que se comprueba, por mensaje de tipo snapshot:
 *
 *     {"v":1,"kind":"snapshot","stack":"spring","ts":"...","ticksIn":84,"ticksOut":12,
 *      "symbols":{"EUR/USD":{...}}}
 *
 *     java scripts/GatewayLiveCheck.java [host] [puerto] [segundos]
 *
 * Sale con codigo 0 solo si el reparto funciona y el snapshot cumple el contrato.
 */
public class GatewayLiveCheck {

    private static final Pattern KIND = Pattern.compile("\"kind\"\\s*:\\s*\"([a-z]+)\"");
    private static final Pattern VERSION = Pattern.compile("\"v\"\\s*:\\s*1\\b");
    private static final Pattern STACK = Pattern.compile("\"stack\"\\s*:\\s*\"([a-z]+)\"");
    private static final Pattern SYMBOLS = Pattern.compile("\"symbols\"\\s*:\\s*\\{");
    private static final Pattern TICKS_IN = Pattern.compile("\"ticksIn\"\\s*:\\s*\\d+");
    private static final Pattern TICKS_OUT = Pattern.compile("\"ticksOut\"\\s*:\\s*\\d+");
    private static final Pattern PRICE = Pattern.compile("\"price\"\\s*:");

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int puerto = args.length > 1 ? Integer.parseInt(args[1]) : 8089;
        long milisegundos = args.length > 2 ? Long.parseLong(args[2]) * 1000 : 12_000;

        Map<String, Integer> cuenta = new TreeMap<>();
        Map<String, String> ejemplo = new LinkedHashMap<>();
        AtomicInteger snapshotsValidos = new AtomicInteger();
        AtomicInteger simbolos = new AtomicInteger();
        AtomicReference<String> stack = new AtomicReference<>("?");
        AtomicReference<String> fallo = new AtomicReference<>();
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
                    ejemplo.putIfAbsent(tipo, recorte(json));
                    if ("snapshot".equals(tipo)) {
                        String problema = revisarSnapshot(json);
                        if (problema == null) {
                            snapshotsValidos.incrementAndGet();
                            simbolos.set(contarSimbolos(json));
                            Matcher s = STACK.matcher(json);
                            stack.set(s.find() ? s.group(1) : "?");
                        } else {
                            fallo.compareAndSet(null, "snapshot invalido: " + problema);
                        }
                    }
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
            public void onError(WebSocket ws, Throwable problema) {
                System.out.println("error en el websocket: " + problema);
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
        System.out.println("  snapshots validos (v=1, stack, symbols, ticksIn/ticksOut): "
                + snapshotsValidos.get());
        System.out.println("  stack: " + stack.get() + " | simbolos en el ultimo snapshot: " + simbolos.get());
        ws.sendClose(WebSocket.NORMAL_CLOSURE, "fin del check").get(5, TimeUnit.SECONDS);
        cerrado.await(2, TimeUnit.SECONDS);

        if (snapshotsValidos.get() == 0) {
            System.err.println("FALLO: " + (fallo.get() != null ? fallo.get()
                    : "no llego ningun snapshot en " + (milisegundos / 1000) + " s"));
            System.exit(1);
        }
        System.out.println("OK: el gateway reparte " + cuenta.size() + " tipos de evento y el contrato del snapshot se cumple");
    }

    /** Devuelve el motivo por el que el snapshot no cumple el contrato, o null si lo cumple. */
    private static String revisarSnapshot(String json) {
        if (!VERSION.matcher(json).find()) {
            return "falta v=1";
        }
        if (!STACK.matcher(json).find()) {
            return "falta stack";
        }
        if (!SYMBOLS.matcher(json).find()) {
            return "falta symbols";
        }
        if (!TICKS_IN.matcher(json).find() || !TICKS_OUT.matcher(json).find()) {
            return "faltan ticksIn o ticksOut";
        }
        return null;
    }

    /**
     * Cuenta los simbolos del snapshot contando los precios que lleva el objeto symbols (cada
     * simbolo trae exactamente uno). No hay parser de JSON a mano.
     */
    private static int contarSimbolos(String json) {
        int inicio = json.indexOf("\"symbols\"");
        if (inicio < 0) {
            return 0;
        }
        int cuantos = 0;
        Matcher m = PRICE.matcher(json.substring(inicio));
        while (m.find()) {
            cuantos++;
        }
        return cuantos;
    }

    private static String recorte(String json) {
        return json.length() > 160 ? json.substring(0, 160) + "..." : json;
    }
}
