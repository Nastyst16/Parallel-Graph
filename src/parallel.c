// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>

#include "os_graph.h"
#include "os_threadpool.h"
#include "log/log.h"
#include "utils.h"

#define NUM_THREADS		4

static int sum;
static os_graph_t *graph;
static os_threadpool_t *tp;
/* TODO: Define graph synchronization mechanisms. */
static pthread_mutex_t sum_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t node_lock = PTHREAD_MUTEX_INITIALIZER;

static void process_node(unsigned int idx);

// function that processes the neighbours of a node
static void process_neighbours(os_threadpool_t *tp, os_graph_t *graph,
								os_node_t *node)
{
	for (unsigned int i = 0; i < node->num_neighbours; i++) {
		pthread_mutex_lock(&node_lock);

		// if the neighbour is not visited
		if (graph->visited[node->neighbours[i]] == NOT_VISITED) {
			// mark it as processing
			graph->visited[node->neighbours[i]] = PROCESSING;

			pthread_mutex_unlock(&node_lock);

			// gettting the index of the neighbour
			long idx = node->neighbours[i];

			// adding the task so it can be processed by a thread
			enqueue_task(tp, create_task((void *)process_node, (void *)idx, NULL));

		} else {
			pthread_mutex_unlock(&node_lock);
		}
	}
}

// function that processes a node
static void process_node(unsigned int idx)
{
	/* TODO: Implement thread-pool based processing of graph. */

	os_node_t *node;

	// getting the node
	node = graph->nodes[idx];

	// to avoid race condition
	// we use lock and unlock
	pthread_mutex_lock(&sum_lock);
	// updating the sum
	sum += node->info;
	pthread_mutex_unlock(&sum_lock);

	// node set visited
	graph->visited[idx] = DONE;

	// going through all the neighbours of the current node
	process_neighbours(tp, graph, node);
}

int main(int argc, char *argv[])
{
	FILE *input_file;

	if (argc != 2) {
		fprintf(stderr, "Usage: %s input_file\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	input_file = fopen(argv[1], "r");
	DIE(input_file == NULL, "fopen");

	graph = create_graph_from_file(input_file);

	/* TODO: Initialize graph synchronization mechanisms. */
	tp = create_threadpool(NUM_THREADS);

	// going to start from node 0
	// going through this conex component
	process_node(0);

	// wait for all threads to finish
	wait_for_completion(tp);

	// destroy graph synchronization mechanisms
	destroy_threadpool(tp);

	pthread_mutex_destroy(&sum_lock);
	pthread_mutex_destroy(&node_lock);

	// printing the result
	printf("%d", sum);

	return 0;
}
