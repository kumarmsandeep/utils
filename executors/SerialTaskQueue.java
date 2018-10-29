package com.app;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class SerialTaskQueue {

	// all lookups and modifications to the list must be synchronized on the
	// list.
	private final Map<String, GroupTask> taskGroups = new HashMap<>();
	// make lock fair so that adding and removing tasks is balanced.
	private final ReentrantLock lock = new ReentrantLock(true);
	private final ExecutorService executor;

	public SerialTaskQueue(ExecutorService executor) {
		this.executor = executor;
	}

	public boolean add(String groupId, Runnable task) {

		lock.lock();
		GroupTask gt = null;
		try {
			gt = taskGroups.get(groupId);
			if (gt == null) {
				gt = new GroupTask(groupId);
				taskGroups.put(groupId, gt);
			}
			gt.tasks.add(task);
		} finally {
			lock.unlock();
		}
		if (gt != null) {
			runNextTask(gt);
		}
		return true;
	}

	private void runNextTask(GroupTask gt) {

		// critical section that ensures one task is executed.
		lock.lock();
		try {
			if (gt.tasks.isEmpty()) {
				// only cleanup when last task has executed, prevent memory leak
				if (!gt.taskRunning.get()) {
					taskGroups.remove(gt.groupId);
				}
			} else if (!executor.isShutdown() && gt.taskRunning.compareAndSet(false, true)) {
				executor.execute(new WrapperTask(gt.tasks.poll(), new GroupTaskCallback(gt, this)));
			}
		} finally {
			lock.unlock();
		}
	}

	/** Amount of (active) task groups. */
	public int size() {

		int size = 0;
		lock.lock();
		try {
			size = taskGroups.size();
		} finally {
			lock.unlock();
		}
		return size;
	}

	public int size(String groupId) {

		int size = 0;
		lock.lock();
		try {
			GroupTask gt = taskGroups.get(groupId);
			size = (gt == null ? 0 : gt.tasks.size());
		} finally {
			lock.unlock();
		}
		return size;
	}

	/* Helper class for the task-group map. */
	class GroupTask {

		final Queue<Runnable> tasks = new LinkedList<Runnable>();
		// atomic boolean used to ensure only 1 task is executed at any given
		// time
		final AtomicBoolean taskRunning = new AtomicBoolean(false);
		final String groupId;

		GroupTask(String groupId) {
			this.groupId = groupId;
		}
	}

	class GroupTaskCallback implements Runnable {

		private final GroupTask gt;
		private final SerialTaskQueue serialTaskQueues;

		public GroupTaskCallback(GroupTask gt, SerialTaskQueue serialTaskQueues) {
			this.gt = gt;
			this.serialTaskQueues = serialTaskQueues;
		}

		@Override
		public void run() {
			if (!gt.taskRunning.compareAndSet(true, false)) {
				System.out.println("ERROR: programming error, the callback should always run in execute state.");
			}
			serialTaskQueues.runNextTask(gt);
		}
	}

	static class WrapperTask implements Runnable {

		private final Runnable task;
		private final GroupTaskCallback callback;

		public WrapperTask(Runnable task, GroupTaskCallback callback) {
			this.task = task;
			this.callback = callback;
		}

		@Override
		public void run() {

			try {
				task.run();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					callback.run();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
