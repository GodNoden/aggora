package com.aggora.normalizer.schema;

import com.aggora.avro.canonical.CanonicalTick;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Laboratorio de evolucion de esquemas (Fase 7), en pequeño y sin broker.
 *
 * <p>Un mensaje Avro no lleva el esquema dentro: lleva el ID del esquema con el que se
 * escribio, y quien lo lee le pide ese esquema al registro. Con los dos esquemas en la mano
 * (el del escritor y el del lector), Avro traduce campo a campo. Que un cambio de esquema
 * sea COMPATIBLE significa, ni mas ni menos, que esa traduccion funciona.
 *
 * <p>Las dos direcciones no son simetricas y de ahi salen todas las reglas:
 * <ul>
 *   <li>Un campo del escritor que el lector no tiene: se ignora. Por eso quitar un campo
 *       no molesta a los consumidores nuevos.</li>
 *   <li>Un campo que el lector pide y el escritor no escribio: solo se puede rellenar si
 *       tiene valor por defecto. Si no lo tiene, la lectura falla.</li>
 * </ul>
 *
 * <p>Estos tests comprueban la traduccion de verdad, con Avro, en vez de fiarse de la
 * teoria. El "consumidor antiguo" es la clase {@link CanonicalTick} que el servicio ya tiene
 * compilada a partir de {@code services/schemas/canonical.avsc}: no se recompila nada.
 *
 * <p>El laboratorio en vivo, contra el Schema Registry de verdad (que es quien rechaza el
 * cambio que rompe y quien decide si un despliegue pasa o no), esta en
 * {@code scripts/schema-evolution-lab.sh}.
 */
class SchemaEvolutionTest {

    /** Los .avsc reales del proyecto: el mismo directorio del que el build genera las clases. */
    private static final Path ESQUEMAS = Path.of("..", "schemas");

    /** El esquema propuesto en la fase 7: canonical.avsc + venueMic (opcional, con default). */
    private static final String PROPUESTA = "/canonical-v2.avsc";

    private static Schema esquema(String fichero) throws IOException {
        return new Schema.Parser().parse(Files.readString(ESQUEMAS.resolve(fichero)));
    }

    private static Schema esquemaPropuesto() throws IOException {
        try (InputStream in = SchemaEvolutionTest.class.getResourceAsStream(PROPUESTA)) {
            return new Schema.Parser().parse(in);
        }
    }

    @Test
    @DisplayName("Un consumidor ANTIGUO sigue leyendo los mensajes escritos con el esquema nuevo")
    void el_consumidor_antiguo_sigue_leyendo() throws IOException {
        Schema propuesto = esquemaPropuesto();
        byte[] mensajeNuevo = escribir(unTick(propuesto), propuesto);

        // Este lector es el consumidor antiguo: la clase que ya esta compilada en el servicio.
        // El escritor usa el esquema nuevo; el lector, el viejo. Avro ignora lo que sobra.
        SpecificDatumReader<CanonicalTick> consumidorAntiguo =
                new SpecificDatumReader<>(propuesto, CanonicalTick.getClassSchema());
        CanonicalTick tick = consumidorAntiguo.read(null, DecoderFactory.get().binaryDecoder(mensajeNuevo, null));

        assertThat(tick.getSymbol()).isEqualTo("EUR/USD");
        assertThat(tick.getSize()).isEqualTo(1000);
        assertThat(tick.getOriginOffset()).isEqualTo(12345L);
        assertThat(tick.getPrice()).isEqualByComparingTo("1.1462");
        assertThat(CanonicalTick.getClassSchema().getField("venueMic"))
                .as("el consumidor antiguo ni siquiera conoce el campo nuevo")
                .isNull();
    }

    @Test
    @DisplayName("Un consumidor NUEVO lee los mensajes ANTIGUOS: el campo nuevo toma su valor por defecto")
    void el_consumidor_nuevo_lee_lo_antiguo() throws IOException {
        Schema antiguo = esquema("canonical.avsc");
        byte[] mensajeAntiguo = escribir(unTick(antiguo), antiguo);

        GenericRecord leido = leer(mensajeAntiguo, antiguo, esquemaPropuesto());

        assertThat(leido.get("venueMic").toString())
                .as("los mensajes viejos no traen el campo: lo pone el valor por defecto")
                .isEmpty();
        assertThat(leido.get("originOffset")).isEqualTo(12345L);
    }

    @Test
    @DisplayName("Un campo nuevo SIN valor por defecto rompe la compatibilidad; con default, no")
    void un_campo_nuevo_necesita_valor_por_defecto() throws IOException {
        Schema v1 = esquema("canonical.avsc");
        byte[] mensaje = escribir(unTick(v1), v1);

        Schema sinDefault = conCampoNuevo(v1, campoNuevo("venueMic", null));
        assertThatThrownBy(() -> leer(mensaje, v1, sinDefault))
                .as("el lector nuevo pide un campo que los mensajes viejos no traen: es el error que da el registro")
                .isInstanceOf(AvroRuntimeException.class)
                .hasMessageContaining("venueMic");

        Schema conDefault = conCampoNuevo(v1, campoNuevo("venueMic", ""));
        assertThat(leer(mensaje, v1, conDefault).get("venueMic").toString())
                .as("el valor por defecto es lo unico que hace compatible el campo nuevo")
                .isEmpty();
    }

    @Test
    @DisplayName("Renombrar un campo rompe la compatibilidad; con un alias, nadie se entera")
    void renombrar_necesita_un_alias() throws IOException {
        Schema v1 = esquema("canonical.avsc");
        byte[] mensaje = escribir(unTick(v1), v1);

        assertThatThrownBy(() -> leer(mensaje, v1, renombrar(v1, "symbol", "ticker")))
                .as("para el lector nuevo, symbol ha desaparecido")
                .isInstanceOf(AvroRuntimeException.class)
                .hasMessageContaining("ticker");

        GenericRecord leido = leer(mensaje, v1, renombrar(v1, "symbol", "ticker", "symbol"));
        assertThat(leido.get("ticker").toString())
                .as("el alias le dice al lector nuevo como se llamaba el campo antes")
                .isEqualTo("EUR/USD");
    }

    @Test
    @DisplayName("Quitar un campo no molesta al consumidor nuevo, pero rompe al antiguo")
    void quitar_un_campo_solo_rompe_hacia_atras() throws IOException {
        Schema v1 = esquema("canonical.avsc");
        Schema sinOriginOffset = sinCampo(v1, "originOffset");

        // El consumidor nuevo (que ya no conoce el campo) lee los mensajes antiguos sin problema.
        GenericRecord conLectorNuevo = leer(escribir(unTick(v1), v1), v1, sinOriginOffset);
        assertThat(conLectorNuevo.get("symbol").toString()).isEqualTo("EUR/USD");
        assertThat(conLectorNuevo.getSchema().getField("originOffset")).isNull();

        // El consumidor antiguo, en cambio, no puede leer los mensajes nuevos: le falta el campo.
        byte[] mensajeNuevo = escribir(unTick(sinOriginOffset), sinOriginOffset);
        assertThatThrownBy(() -> leer(mensajeNuevo, sinOriginOffset, v1))
                .as("no basta con que el registro diga 'compatible': hay que mirar en que direccion")
                .isInstanceOf(AvroRuntimeException.class)
                .hasMessageContaining("originOffset");
    }

    // --- utilidades ---------------------------------------------------------

    /** Escribe un registro con el esquema del escritor, en binario Avro (sin el ID del registro). */
    private static byte[] escribir(GenericRecord registro, Schema escritor) throws IOException {
        ByteArrayOutputStream salida = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(salida, null);
        new GenericDatumWriter<GenericRecord>(escritor).write(registro, encoder);
        encoder.flush();
        return salida.toByteArray();
    }

    /** Lee unos bytes sabiendo quien los escribio y quien los lee: la traduccion de Avro. */
    private static GenericRecord leer(byte[] mensaje, Schema escritor, Schema lector) throws IOException {
        GenericDatumReader<GenericRecord> traductor = new GenericDatumReader<>(escritor, lector);
        return traductor.read(null, DecoderFactory.get().binaryDecoder(mensaje, null));
    }

    private static GenericRecord unTick(Schema esquema) {
        GenericRecord tick = new GenericData.Record(esquema);
        tick.put("eventId", "evt-0001");
        tick.put("symbol", "EUR/USD");
        tick.put("assetClass", new GenericData.EnumSymbol(esquema.getField("assetClass").schema(), "FX"));
        tick.put("exchange", new GenericData.EnumSymbol(esquema.getField("exchange").schema(), "FX"));
        tick.put("currency", "USD");
        tick.put("price", precio());
        tick.put("size", 1000);
        tick.put("eventTime", 1_700_000_000_000L);
        tick.put("source", new GenericData.EnumSymbol(esquema.getField("source").schema(), "REFERENCE"));
        tick.put("sequence", 7L);
        tick.put("normalizedAt", 1_700_000_000_100L);
        tick.put("originPartition", 2);
        poner(tick, esquema, "originOffset", 12345L);
        poner(tick, esquema, "venueMic", "XNAS");
        return tick;
    }

    /** Se pone el campo solo si el esquema lo tiene: asi el registro sirve tambien sin el. */
    private static void poner(GenericRecord tick, Schema esquema, String nombre, Object valor) {
        if (esquema.getField(nombre) != null) {
            tick.put(nombre, valor);
        }
    }

    /** 1.1462 con la escala 4 del esquema: el decimal de Avro viaja como bytes. */
    private static ByteBuffer precio() {
        return ByteBuffer.wrap(new java.math.BigDecimal("1.1462").unscaledValue().toByteArray());
    }

    private static Schema.Field campoNuevo(String nombre, Object valorPorDefecto) {
        return new Schema.Field(nombre, Schema.create(Schema.Type.STRING), "campo del laboratorio", valorPorDefecto);
    }

    private static Schema conCampoNuevo(Schema record, Schema.Field nuevo) {
        List<Schema.Field> campos = new ArrayList<>(copiarCampos(record.getFields()));
        campos.add(nuevo);
        return copiar(record, campos);
    }

    /** El mismo registro sin un campo, que es lo que se lleva por delante a los antiguos. */
    private static Schema sinCampo(Schema record, String nombre) {
        List<Schema.Field> campos = copiarCampos(record.getFields()).stream()
                .filter(f -> !f.name().equals(nombre))
                .toList();
        return copiar(record, campos);
    }

    private static Schema renombrar(Schema record, String viejo, String nuevo, String... alias) {
        List<Schema.Field> campos = copiarCampos(record.getFields()).stream()
                .map(f -> f.name().equals(viejo) ? renombrado(f, nuevo) : f)
                .toList();
        Schema copia = copiar(record, campos);
        if (alias.length > 0) {
            copia.getField(nuevo).addAlias(alias[0]);
        }
        return copia;
    }

    /**
     * Un Schema.Field pertenece al registro que lo creo, asi que para reutilizarlo en otro
     * registro hay que copiarlo. Con los objetos originales, Avro lanza "Field already used".
     */
    private static List<Schema.Field> copiarCampos(List<Schema.Field> campos) {
        return campos.stream()
                .map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
                .toList();
    }

    private static Schema.Field renombrado(Schema.Field campo, String nuevo) {
        return new Schema.Field(nuevo, campo.schema(), campo.doc(), campo.defaultVal(), campo.order());
    }

    private static Schema copiar(Schema record, List<Schema.Field> campos) {
        return Schema.createRecord(record.getName(), record.getDoc(), record.getNamespace(), false, campos);
    }
}
