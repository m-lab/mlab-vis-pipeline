package mlab.dataviz.coder;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * A Coder for NavigableMaps, based on Google's MapCoder
 * https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/coders/MapCoder.java
 */
public class NavigableMapCoder<K, V> extends Coder<NavigableMap<K, V>> {

	private static final long serialVersionUID = 1L;
	
	private MapCoder<K, V> mapCoder;

	NavigableMapCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
		mapCoder = MapCoder.of(keyCoder, valueCoder);
	}

	/**
	 * Produces a MapCoder with the given keyCoder and valueCoder.
	 */
	public static <K, V> NavigableMapCoder<K, V> of(Coder<K> keyCoder, Coder<V> valueCoder) {
		return new NavigableMapCoder<K, V>(keyCoder, valueCoder);
	}

	@JsonCreator
	public static NavigableMapCoder<?, ?> of(
			@JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<Coder<?>> components) {
		Preconditions.checkArgument(components.size() == 2, "Expecting 2 components, got " + components.size());
		return of((Coder<?>) components.get(0), (Coder<?>) components.get(1));
	}

	/**
	 * Returns the key and value for an arbitrary element of this map, if it is
	 * non-empty, otherwise returns {@code null}.
	 */
	public static <K, V> List<Object> getInstanceComponents(Map<K, V> exampleValue) {
		for (Map.Entry<K, V> entry : exampleValue.entrySet()) {
			return Arrays.asList(entry.getKey(), entry.getValue());
		}
		return null;
	}

	public Coder<K> getKeyCoder() {
		return mapCoder.getKeyCoder();
	}

	public Coder<V> getValueCoder() {
		return mapCoder.getValueCoder();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @return a {@link List} containing the key coder at index 0 at the and value
	 *         coder at index 1.
	 */
	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		return mapCoder.getCoderArguments();
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws NonDeterministicException always. Not all maps have a deterministic
	 *                                   encoding. For example, {@code HashMap}
	 *                                   comparison does not depend on element
	 *                                   order, so two {@code HashMap} instances may
	 *                                   be equal but produce different encodings.
	 */
	@Override
	public void verifyDeterministic() throws NonDeterministicException {
		throw new NonDeterministicException(this, "Ordering of entries in a Map may be non-deterministic.");
	}

	@Override
	public void registerByteSizeObserver(NavigableMap<K, V> map, ElementByteSizeObserver observer) throws Exception {
		mapCoder.registerByteSizeObserver(map, observer);
	}

	@Override
	public void encode(NavigableMap<K, V> map, OutputStream outStream) throws CoderException, IOException {
		mapCoder.encode(map, outStream);
	}

	@Override
	public NavigableMap<K, V> decode(InputStream inStream) throws CoderException, IOException {
		DataInputStream dataInStream = new DataInputStream(inStream);
		int size = dataInStream.readInt();
		NavigableMap<K, V> retval = new TreeMap<K, V>();

		for (int i = 0; i < size; ++i) {
			K key = mapCoder.getKeyCoder().decode(inStream);
			V value = mapCoder.getValueCoder().decode(inStream);
			retval.put(key, value);
		}
		return retval;
	}

}