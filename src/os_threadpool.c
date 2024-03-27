// SPDX-License-Identifier: BSD-3-Clause

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

#include "os_threadpool.h"
#include "log/log.h"
#include "utils.h"

/* Create a task that would be executed by a thread. */
os_task_t *create_task(void (*action)(void *), void *arg, void (*destroy_arg)(void *))
{
	os_task_t *t;

	t = malloc(sizeof(*t));
	DIE(t == NULL, "malloc");

	t->action = action;		// the function
	t->argument = arg;		// arguments for the function
	t->destroy_arg = destroy_arg;	// destroy argument function

	return t;
}

/* Destroy task. */
void destroy_task(os_task_t *t)
{
	if (t->destroy_arg != NULL)
		t->destroy_arg(t->argument);

	free(t);
}

/* Put a new task to threadpool task queue. */
void enqueue_task(os_threadpool_t *tp, os_task_t *t)
{
	assert(tp != NULL);
	assert(t != NULL);

	pthread_mutex_lock(&tp->queue_lock);
	list_add_tail(&tp->head, &t->list);

	// if there are threads waiting, wake one up
	pthread_cond_signal(&tp->wait_cond);

	pthread_mutex_unlock(&tp->queue_lock);
}

/*
 * Check if queue is empty.
 * This function should be called in a synchronized manner.
 */
static int queue_is_empty(os_threadpool_t *tp)
{
	return list_empty(&tp->head);
}

// function that waits for a condition
void wait_condition(os_threadpool_t *tp)
{
	// the tp->stop condition is set to 1 when the main thread
	// finishes process_node(0) in wait_for_completion
	if (queue_is_empty(tp) && // if the queue is empty and
		(tp->threads_waiting < tp->num_threads - 1 // there are no threads waiting
		|| tp->stop == 0)) { // and the stop condition is not met
		// the condition tp->stop == 0 is important because before the
		// main thread finishes process_node(0) all the threads would
		// be waiting for a task, but the main thread would not be able to
		// enqueue, so the last thread (5) would reach line 96 and exit
		// we dont want that, we want all the threads to wait for the main

		// incrementing the number of threads waiting
		tp->threads_waiting++;

		pthread_mutex_lock(&tp->queue_lock);
		// waiting for a thread to enqueue a task
		pthread_cond_wait(&tp->wait_cond, &tp->queue_lock);
		pthread_mutex_unlock(&tp->queue_lock);

		// decrementing the number of threads waiting
		tp->threads_waiting--;
	}
}


/*
 * Get a task from threadpool task queue.
 * Block if no task is available.
 * Return NULL if work is complete, i.e. no task will become available,
 * i.e. all threads are going to block.
 */
os_task_t *dequeue_task(os_threadpool_t *tp)
{
	os_task_t *t;

	/* TODO: Dequeue task from the shared task queue. Use synchronization. */
	wait_condition(tp);

	pthread_mutex_lock(&tp->queue_lock);

	// after the broadcast, the last threads would enter here
	if (queue_is_empty(tp)) {
		pthread_mutex_unlock(&tp->queue_lock);
		return NULL;
	}

	// get the first task
	t = list_entry(tp->head.next, os_task_t, list);
	list_del(tp->head.next);

	pthread_mutex_unlock(&tp->queue_lock);

	return t;
}

/* Loop function for threads */
static void *thread_loop_function(void *arg)
{
	os_threadpool_t *tp = (os_threadpool_t *) arg;

	while (1) {
		os_task_t *t;

		t = dequeue_task(tp);
		if (t == NULL)
			break;
		t->action(t->argument);
		destroy_task(t);
	}

	pthread_mutex_lock(&tp->threads_lock);
	tp->exited_threads++;
	pthread_mutex_unlock(&tp->threads_lock);

	return NULL;
}

/* Wait completion of all threads. This is to be called by the main thread. */
void wait_for_completion(os_threadpool_t *tp)
{
	assert(tp != NULL);

	/* TODO: Wait for all worker threads. Use synchronization. */

	pthread_mutex_lock(&tp->wait_lock);
	// setting the stop condition
	tp->stop = 1;
	pthread_mutex_unlock(&tp->wait_lock);

	// broadcast until all threads exit
	while (tp->exited_threads < tp->num_threads)
		pthread_cond_broadcast(&tp->wait_cond);


	/* Join all worker threads. */
	for (unsigned int i = 0; i < tp->num_threads; i++)
		pthread_join(tp->threads[i], NULL);
}

/* Initialize mutexes. */
void init_mutex(os_threadpool_t *tp)
{
	pthread_mutex_init(&tp->queue_lock, NULL);
	pthread_mutex_init(&tp->threads_lock, NULL);
	pthread_mutex_init(&tp->wait_lock, NULL);
	pthread_cond_init(&tp->wait_cond, NULL);
}

/* Create a new threadpool. */
os_threadpool_t *create_threadpool(unsigned int num_threads)
{
	os_threadpool_t *tp = NULL;
	int rc;

	tp = malloc(sizeof(*tp));
	DIE(tp == NULL, "malloc");

	list_init(&tp->head);

	/* TODO: Initialize synchronization data. */
	init_mutex(tp);

	// initialising the stop condition. 0 means no stop
	tp->stop = 0;
	tp->threads_waiting = 0;
	tp->exited_threads = 0;

	tp->num_threads = num_threads;
	tp->threads = malloc(num_threads * sizeof(*tp->threads));
	DIE(tp->threads == NULL, "malloc");
	for (unsigned int i = 0; i < num_threads; ++i) {
		rc = pthread_create(&tp->threads[i], NULL, &thread_loop_function, (void *) tp);
		DIE(rc < 0, "pthread_create");
	}

	return tp;
}

/* Destroy mutexes. */
void destroy_mutex(os_threadpool_t *tp)
{
	pthread_mutex_destroy(&tp->queue_lock);
	pthread_mutex_destroy(&tp->threads_lock);
	pthread_mutex_destroy(&tp->wait_lock);
	pthread_cond_destroy(&tp->wait_cond);
}


/* Destroy a threadpool. Assume all threads have been joined. */
void destroy_threadpool(os_threadpool_t *tp)
{
	os_list_node_t *n, *p;

	/* TODO: Cleanup synchronization data. */
	destroy_mutex(tp);

	list_for_each_safe(n, p, &tp->head) {
		list_del(n);
		destroy_task(list_entry(n, os_task_t, list));
	}

	free(tp->threads);
	free(tp);
}
