import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*
    Attempt to write a Task scheduler, Tasks are prioritised based on priority.
    1. When TaskScheduler is instantiated, internally it initialises priority queue and scheduledExecutorService
    with number of Cores.
    2. Any task can be scheduled by passing Task info and its initialDelay params
    3. stop method tries to stop current executing tasks and returns tasks from the queue which are yet to be executed.
 */

public class TaskScheduler {

    static class Task {
        String id;
        int priority;

        Task(String id, int priority) {
            this.id = id;
            this.priority = priority;
        }

        @Override
        public boolean equals(Object o) {
            if (this != o) return false;
            if (o == null || o.getClass() != getClass()) return false;

            Task t = (Task) o;
            return t.id.equals(this.id);
        }
    }

    private static ScheduledExecutorService scheduledExecutorService;
    private Queue<Task> priorityQueue;

    public void scheduleTask(Task taskInfo, long initialDelay) {
        priorityQueue.add(taskInfo);
        scheduledExecutorService.schedule(new FetchTask(), initialDelay, TimeUnit.SECONDS);
    }

    public TaskScheduler() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        scheduledExecutorService = Executors.newScheduledThreadPool(availableProcessors);
        priorityQueue = new PriorityQueue<>(new Comparator<Task>() {
            @Override
            public int compare(Task task1, Task task2) {
                return task1.priority - task2.priority;
            }
        });
    }

    public List<String> stop() {
        List<String> tasks = new ArrayList<>();
        scheduledExecutorService.shutdown();
        while (!priorityQueue.isEmpty()) {
            tasks.add(priorityQueue.poll().id);
        }
        return tasks;
    }

    private class FetchTask implements Runnable {

        @Override
        public void run() {
            if (priorityQueue == null || priorityQueue.isEmpty()) {
                return;
            }
            Task task = priorityQueue.poll();
            System.out.println("Executing task = " + task.id);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        TaskScheduler taskScheduler = new TaskScheduler();
        taskScheduler.scheduleTask(new Task("ABC", 2), 2);
        taskScheduler.scheduleTask(new Task("DEF", 1), 2);

        taskScheduler.scheduleTask(new Task("GHI", 1), 5);
        taskScheduler.scheduleTask(new Task("JKL", 1), 5);

        Thread.sleep(3000);
        List<String> list = taskScheduler.stop();
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
    }
}
