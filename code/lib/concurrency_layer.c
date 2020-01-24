// THIRD ASSIGNMENT - STOCK MARKET
// OPERATING SYSTEMS - 2019
// Rafael Pablos Sarabia 100372175
// Juan Sanchez Esquivel 100383422

#include "../include/concurrency_layer.h"

/* Declarations */

int stats_readers; 		//Number of simultaneous readers.
pthread_mutex_t accesses;	//Mutex to control accesses from readers and writers.
pthread_mutex_t readers_mutex;	//Mutex to control shared variable stats_readers.
pthread_cond_t broker_write;	//Allows broker to execute if queue is not full and 
				//no readers are active.
pthread_cond_t executer_write; 	//Allows executer to execute if queue is not empty and 
				//no readers are active.

/*
 * Initializes the concurrency control mechanisms necessary for the correct operation of the
 * application with multiple threads. It receives no input parameters and has no return value. 
 * It will always be called by the main () function of the program before creating any thread.
 * Initializes global variables, mutexes and condition variables.
 */
void init_concurrency_mechanisms() {
	stats_readers = 0; //No readers initially
	pthread_mutex_init(&accesses, NULL);
	pthread_mutex_init(&readers_mutex, NULL);
	pthread_cond_init(&broker_write, NULL);
	pthread_cond_init(&executer_write, NULL);
}

/*
 * Destroys all concurrency control mechanisms used during program execution. It will be 
 * always called by the main () function after the completion of all threads and before
 * completion of the program. Destroy mutex and condition variables.
 */
void destroy_concurrency_mechanisms() {
	pthread_mutex_destroy(&accesses);
	pthread_mutex_destroy(&readers_mutex);
	pthread_cond_destroy(&broker_write);
	pthread_cond_destroy(&executer_write);
}

/*
 * Implements the functionality of broker threads (concurrency control included).
 * Brokers are responsible for inserting new stock trades on the queue of market
 * operations. They take the operations from a batch file. There may be n concurrent 
 * threads with this role, but only runs one operations at a time to maintain data integrity.
 * Receives broker_info as input parameter that contains:
 *	typedef struct broker_info{
 *		char batch_file[256]; 	//name of the batch file containing transactions.
 *		stock_market * market;	//pointer to the market where broker operates.
 *	}broker_info;
 */
void* broker(void * args) {
  //Extract data received in the pointer
  broker_info param = *(broker_info * ) args; //Cast to type broker_info

  //Create iterator of the batch file
  iterator * my_iterator = new_iterator(param.batch_file);

  //Create variables for the next operation to be added to queue
  char id [ID_LENGTH];
  int type;
  int num_shares;
  int price;

  //Allocate memory for the operation
  operation * op = malloc(sizeof(operation));

  //While there are pending file operations. Positive value if successful read.
  while (next_operation(my_iterator, id, &type, &num_shares, &price)>0) {

    //Create a new operation
    new_operation(op, id, type, num_shares, price);

    //Attempt to enter critical section. No writers or readers must be active. Also, queue
    //must not be full. operations_queue_full returns 1 if full. Wait if broker can not execute.
    pthread_mutex_lock(&accesses);
    while (operations_queue_full((*param.market).stock_operations)==1) {
      pthread_cond_wait(&broker_write, &accesses);
    }

    //Queue the new operation
    enqueue_operation((*param.market).stock_operations, op);

    //Signal that the queue is not empty and executer can operate. 
    pthread_cond_signal(&executer_write);

    //End of critical section. Unlock the mutex.
    pthread_mutex_unlock(&accesses);
  }

  //Free memory allocated for operations, destroy the iterator and finish thread.
  free(op);
  destroy_iterator(my_iterator);
  pthread_exit(NULL);
}

/*
 * Implements the functionality of the thread that processes the operations (including
 * concurrency control). The operation_executer is in charge of processing operations 
 * one by one (operations that were inserted into the market operations queue by the brokers).
 * There will only be a thread running this role. 
 * It receives as input parameter the exec_info type structure containing:
 *	typedef struct exec_info{
 *		int *exit;
 *		stock_market * market;
 *		pthread_mutex_t *exit_mutex;
 *	}exec_info; 
 */
void* operation_executer(void * args) {
  //Extract data received in the pointer
  exec_info param = *(exec_info * ) args; //Cast to type exec_info

  //Store reference to queue in variable for easier access.
  operations_queue * queue = (*param.market).stock_operations;

  //Allocate memory for the operation
  operation * op = malloc(sizeof(operation));

  //While the exit flag is not active or the queue is not empty. Exit flag is active when brokers
  //have finished adding to the queue. Exit flag is shared, so only access if not being used by
  //another thread. Use a temporal varaible for exitFlag, which is updated before each loop.
  int exitFlag;

  //Use mutex when accessing exit flag.
  pthread_mutex_lock(param.exit_mutex);
  exitFlag = *(param.exit);
  pthread_mutex_unlock(param.exit_mutex);
  
  while (exitFlag!=1 || operations_queue_empty(queue)!=1) {
    //Attempt to enter critical section. No writers or readers must be active. Also, queue
    //must not be empty. operations_queue_empty returns 1 if full. Wait if executer can not execute.
    //Finish executer if flag is active and queue is empty.
    pthread_mutex_lock(&accesses);
    while (operations_queue_empty(queue)==1) {
      //Update exitFlag and exit thread if needed
      pthread_mutex_lock(param.exit_mutex);
      exitFlag = *(param.exit);
      pthread_mutex_unlock(param.exit_mutex);
      if (exitFlag==1 && operations_queue_empty(queue)==1) {
        pthread_exit(NULL);
      }
      pthread_cond_wait(&executer_write, &accesses);
    }

    //Dequeue operation from the operations queue
    dequeue_operation(queue, op);

    //Process the operation
    process_operation(param.market, op);

    //Signal that the queue is not full and broker can operate. 
    pthread_cond_signal(&broker_write);

    //End of critical section. Unlock the mutex.
    pthread_mutex_unlock(&accesses);

    //Update exitFlag
    pthread_mutex_lock(param.exit_mutex);
    exitFlag = *(param.exit);
    pthread_mutex_unlock(param.exit_mutex);
  }

  //Free memory allocated for operations and finish thread.
  free(op);
  pthread_exit(NULL);
}

/* Implements the functionality of the threads that evaluate the stock exchange market 
 * (including concurrency control). It is responsible for evaluating the current state of the
 * market. There may be n concurrent readers in the system and all can run simultaneously. 
 * During this read process no task that modifies the market (operation_executer or broker) 
 * can be executed. It receives reader_info as input parameter containing:
 *	typedef struct reader_info{
 *		int *exit;
 *		stock_market * market;
 *		pthread_mutex_t *exit_mutex;
 *		unsigned int frequency;
 *	}reader_info;
 */
void* stats_reader(void * args) {
  //Extract data received in the pointer
  reader_info param = *(reader_info * ) args; //Cast to type reader_info
  
  //While the exit flag is not active. Exit flag is active when brokers have finished adding to the
  //queue. Exit flag is shared, so only access if not being used by another thread. Use a temporal
  //varaible for exitFlag, which is updated before each loop.
  int exitFlag;

  //Use mutex when accessing exit flag.
  pthread_mutex_lock(param.exit_mutex);
  exitFlag = *(param.exit);
  pthread_mutex_unlock(param.exit_mutex);

  while (exitFlag!=1) {
    //Increase the number of current readers by 1. If it is the first reader, lock accesses so
    //brookers and executer are blocked. Perform these actions in critical section controlled by 
    //mutex readers so that only 1 reader is executing this part and no concurrency issues arise.
    pthread_mutex_lock(&readers_mutex);
    stats_readers++;
    if (stats_readers == 1) {
      pthread_mutex_lock(&accesses);
    }
    pthread_mutex_unlock(&readers_mutex);
    
    //View stock market statistics
    print_market_status(param.market);

    //Decrease stats_readers
    pthread_mutex_lock(&readers_mutex);
    stats_readers--;
    //If current readers are 0, unlock mutex accesses and signal conditions
    if (stats_readers == 0) {
      pthread_cond_signal(&broker_write);
      pthread_cond_signal(&executer_write);
      pthread_mutex_unlock(&accesses);
    }
    pthread_mutex_unlock(&readers_mutex);

    //Sleep until the next round of information analysis
    usleep(param.frequency);

    //Update exitFlag
    pthread_mutex_lock(param.exit_mutex);
    exitFlag = *(param.exit);
    pthread_mutex_unlock(param.exit_mutex);
  }

  //Finish thread
  pthread_exit(NULL);
}

