package cs245.as3;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	class Record {
		byte recType;
		byte valSize;
		long txId;
		long key;
		ByteBuffer val;

		public Record(int recType, long txId) {
			this.recType = (byte) recType;
			this.valSize = 0;
			this.txId = txId;
			this.key = 0;
			this.val = null;
		}

		public Record(byte recType, long txId, long key, byte[] val) {
			this.recType = recType;
			this.valSize = (byte) val.length;
			this.txId = txId;
			this.key = key;
			this.val = java.nio.ByteBuffer.wrap(val);
		}

		public Record(ByteBuffer slice) {
			this.recType = slice.get();
			this.valSize = slice.get();
			this.txId = slice.getLong();
			this.key = slice.getLong();
			this.val = slice.slice();
		}

		public byte[] asByteArray() {
			ByteBuffer result = java.nio.ByteBuffer.allocate(18 + this.valSize);
			result.put(this.recType);
			result.put(this.valSize);
			result.putLong(this.txId);
			result.putLong(this.key);
			result.put(this.val.array());
			return result.array();
		}
	}

	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private HashMap<Long, Record> writerecsets;
	private LogManager lm;

	public TransactionManager() {
		writesets = new HashMap<>();
		writerecsets = new HashMap<>();
		lm = null;
		//see initAndRecover
		latestValues = null;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		latestValues = sm.readStoredTable();
		this.lm = lm;
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		lm.appendLogRecord(new Record(0,  txID).asByteArray());
		writerecsets.put(txID, new Record(0, txID));
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	public void write(long txID, long key, byte[] value) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				long tag = 0;
				latestValues.put(x.key, new TaggedValue(tag, x.value));
			}
			writesets.remove(txID);
		}
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		writesets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
	}
}
