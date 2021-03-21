package cs245.as3;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
	static class Record {	// Static only as an ugly hack to support variable header sizes. Nothing is actually
							// stored in this across variable transaction managers
		int recType;	// 0 = Write committed; 1 = Write persisted; 2 = Transaction started; 3 = Transaction committed
		int valSize;
		long txId;
		long key;
		ByteBuffer val;

		public static int hdrSize(int recType) {    // # Bytes of metadata other than val
			final int transRecSize = 9;
			final int writeRecSize = 18;
			return (isTransRec(recType) ? transRecSize : writeRecSize);
		}

		public Record(int recType, long input) {
			if (!isTransRec(recType)) throw new RuntimeException("Created transaction record on non-transaction.");
			this.recType = recType;
			this.valSize = 0;
			this.txId = input;
			this.key = 0;
			this.val = ByteBuffer.wrap(new byte[0]);
		}

		public Record(int recType, long key, long txId, byte[] val) {
			this.recType = recType;
			this.valSize = val.length;
			this.txId = txId;
			this.key = key;
			this.val = ByteBuffer.wrap(val);
		}

		public Record(byte[] input) {
			ByteBuffer slice = ByteBuffer.wrap(input);
			this.recType = slice.get();
			this.valSize = isTransRec(recType) ? 0 : slice.get();
			this.txId = slice.getLong();
			this.key = isTransRec(recType) ? 0 : slice.getLong();
			byte[] valBuf = new byte[valSize];
			slice.get(valBuf);
			this.val = ByteBuffer.wrap(valBuf);
		}

		public byte[] asByteArray() {
			ByteBuffer result = ByteBuffer.allocate(hdrSize(recType) + this.valSize);
			result.put((byte) this.recType);
			if (!isTransRec(recType)) result.put((byte) this.valSize);
			result.putLong(this.txId);
			if (!isTransRec(recType)) {
				result.putLong(this.key);
				result.put(this.val.array());
			}
			return result.array();
		}

		public static boolean isTransRec(int type) {
			return (type == 2 || type == 3);
		}
	}


	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	//private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private HashMap<Long, HashMap<Long, Record>> logwritesets;
	private HashMap<Long, Record> unpersistedcommitsets;
	private TreeMap<Integer, AtomicInteger> unpersistedoffsets;
	private LogManager lm;
	private StorageManager sm;

	public TransactionManager() {
		logwritesets = new HashMap<>();
		lm = null;
		sm = null;
		unpersistedoffsets = new TreeMap<>();
		latestValues = null;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		latestValues = sm.readStoredTable();
		this.lm = lm;
		this.sm = sm;
		logwritesets = new HashMap<>();
		unpersistedoffsets = new TreeMap<>();
		unpersistedcommitsets = getUnpersistedCommits(lm);

		unpersistedcommitsets.forEach((key, pair) -> {
			latestValues.put(key, new TaggedValue(0, pair.val.array()));
			sm.queueWrite(key, -1, pair.val.array());
		});
	}

	private HashMap<Long, Record> getUnpersistedCommits(LogManager lm) {
		HashMap<Long, HashMap<Long, Record>> started = new HashMap<>();
		HashMap<Long, Record> committed = new HashMap<>();

		int relLogSize = lm.getLogEndOffset() - lm.getLogTruncationOffset();
		ByteBuffer relevantLog = optimizedLogReader(lm.getLogTruncationOffset(), relLogSize);

		while (0 < relevantLog.remaining()) {
			byte[] recordInfo = new byte[128];
			relevantLog.get(recordInfo, 0, 2);
			int currRecordSize = recordInfo[1] + Record.hdrSize(recordInfo[0]);
			relevantLog.get(recordInfo, 2, currRecordSize - 2);

			Record currRecord = new Record(recordInfo);

			switch (currRecord.recType) {
				case 0 :
					HashMap<Long, Record> currWrite = started.get(currRecord.txId);
					if (currWrite == null) {
						throw new RuntimeException("Attempted a write for a transaction that never began.");
					}
					currWrite.put(currRecord.key, currRecord);

					break;

				case 1 :
					throw new RuntimeException("Congratulations, you've logged a persisted write that hasn't been implemented");

				case 2 :
					if (started.get(currRecord.txId) != null) {
						throw new RuntimeException("Attempted to start transaction that already began.");
					}
					started.put(currRecord.txId, new HashMap<>());
					break;

				case 3 :
					HashMap<Long, Record> transaction = started.get(currRecord.txId);
					if (transaction == null) {
						throw new RuntimeException("Attempted to finish committing transaction that never began.");
					}
					transaction.forEach((key, record) -> {
						Record toWrite = committed.get(key);
						if (toWrite == null || toWrite.txId < record.txId) {
							committed.put(key, record);
						}
					});
					started.remove(currRecord.txId);
					break;

				default :
					throw new RuntimeException("Record type invalid? Type given: " + currRecord.recType);
			}
		}

		return committed;
	}

	private ByteBuffer optimizedLogReader(int offset, int length) {
		ByteBuffer relLog = ByteBuffer.wrap(new byte[length]);
		int numFullReads = length / 128;
		int leftover = length % 128;
		for (int i = 0; i < numFullReads; i++) {
			relLog.put(lm.readLogRecord(offset + i*128, 128));
		}
		if (leftover > 0) {
			relLog.put(lm.readLogRecord(offset + length - leftover, leftover));
		}
		relLog.position(0);
		return relLog;
	}

	private void optimizedLogAppend(PriorityQueue<byte[]> queue) {
		synchronized (lm) {
			ArrayList<byte[]> toProcess = new ArrayList<>();
			while (!queue.isEmpty()) {
				Collections.sort(toProcess, Comparator.comparing(entry -> 128 - entry.length));
				boolean wasNotPlaced = true;
				byte[] first = queue.remove();
				for (byte[] bin : toProcess) {
					int size = bin.length;
					int newSize = first.length + size;
					if (newSize <= 128) {
						ByteBuffer temp = ByteBuffer.wrap(new byte[newSize]);
						temp.put(bin);
						temp.put(first);
						toProcess.remove(bin);
						toProcess.add(temp.array());
						wasNotPlaced = false;
						break;
					}
				}
				if (wasNotPlaced) {
					toProcess.add(first);
				}
			}

			toProcess.forEach(rec -> {
				lm.appendLogRecord(rec);
			});
		}
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		logwritesets.put(txID, new HashMap<>());
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
		HashMap<Long, Record> lastKeyRecord = logwritesets.get(txID);
		if (lastKeyRecord == null) {
			throw new RuntimeException("Write attempted without starting transaction: " + txID);
		}
		lastKeyRecord.put(key, new Record(0, key, txID, value));
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		HashMap<Long, Record> txRecords = logwritesets.get(txID);
		if (txRecords == null) {
			throw new RuntimeException("Error: Tried committing a transaction " + txID + " that never started");
		}

		int currLogEnd = lm.getLogEndOffset();
		AtomicInteger numUnpersisted = new AtomicInteger();
		synchronized (lm) {
			lm.appendLogRecord(new Record(2, txID).asByteArray());
			PriorityQueue<byte[]> toAppend = new PriorityQueue<>(Comparator.comparing((entry) -> (128-entry.length)));

			txRecords.forEach((key, rec) -> {
				toAppend.add(rec.asByteArray());
				numUnpersisted.getAndIncrement();
			});
			optimizedLogAppend(toAppend);
			lm.appendLogRecord(new Record(3, txID).asByteArray());
			unpersistedoffsets.put(currLogEnd, numUnpersisted);


			/*lm.appendLogRecord(new Record(2, txID).asByteArray());
			txRecords.forEach((key, rec) -> {
				lm.appendLogRecord(rec.asByteArray());
				numUnpersisted.getAndIncrement();
			});
			lm.appendLogRecord(new Record(3, txID).asByteArray());
			unpersistedoffsets.put(currLogEnd, numUnpersisted);*/
		}

		txRecords.forEach((key, rec) -> {
			sm.queueWrite(key, currLogEnd, rec.val.array());
			latestValues.put(key, new TaggedValue(0, rec.val.array()));
		});

		logwritesets.remove(txID);
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		logwritesets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		synchronized (this) {
			AtomicInteger numUnpersisted = unpersistedoffsets.get((int)persisted_tag);
			if (numUnpersisted != null) {
				numUnpersisted.getAndDecrement();
				if (numUnpersisted.get() == 0) {
					if (persisted_tag == unpersistedoffsets.firstKey()) {
						lm.setLogTruncationOffset(Math.toIntExact(persisted_tag));
					}
					unpersistedoffsets.remove((int) persisted_tag);
				} else if (numUnpersisted.get() < 0) {
					throw new RuntimeException("Congratulations, you've managed to persist more writes than were done.");
				}
			}
		}
	}
}
