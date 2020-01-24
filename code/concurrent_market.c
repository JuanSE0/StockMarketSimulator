// THIRD ASSIGNMENT - STOCK MARKET
// OPERATING SYSTEMS - 2019
// Rafael Pablos Sarabia 100372175
// Juan Sanchez Esquivel 100383422

#include "include/concurrency_layer.h"

/*
 * Information for the proposed tests
 * TEST 1: testID=1, BROKERS=1, EXECUTERS=1, READERS=0
 * TEST 2: testID=2, BROKERS=1, EXECUTERS=1, READERS=0
 * TEST 3: testID=3, BROKERS=1, EXECUTERS=1, READERS=0
 * TEST 4: testID=4, BROKERS=3, EXECUTERS=1, READERS=0
 * TEST 5: testID=5, BROKERS=1, EXECUTERS=1, READERS=1
 * TEST 6: testID=6, BROKERS=1, EXECUTERS=1, READERS=1
 * TEST 7: testID=7, BROKERS=3, EXECUTERS=1, READERS=2
 */

//Modify these for automatic file input and threads management
#define testID 1
#define BROKERS 1
#define EXECUTERS 1
#define READERS 0

int main(int argc, char * argv[]){
	pthread_t tid[BROKERS+EXECUTERS+READERS];
	stock_market market_madrid;
	int exit = 0;
	pthread_mutex_t exit_mutex;

	// Init market and concurrency mechanisms
	char market_file[20];
	sprintf(market_file, "./tests/%d_stocks.txt", testID);
	init_market(&market_madrid, market_file);
	init_concurrency_mechanisms();
	pthread_mutex_init(&exit_mutex,NULL);
	
	// Init broker_info structure for the broker threads
	broker_info info_b[BROKERS];	
	for (int i=0; i<BROKERS; i++) {
		//Obtain batch file proper file for each broker
		char batch_file[50];
		sprintf(batch_file, "./tests/%d_batch_operations_%d.txt", testID, i+1);
		strcpy(info_b[i].batch_file, batch_file);
		info_b[i].market = &market_madrid;
	}

	// Init exec_info structure for the operation_executer thread
	exec_info info_ex1;
	info_ex1.market = &market_madrid;
	info_ex1.exit = &exit;
	info_ex1.exit_mutex = &exit_mutex;
	
	// Init reader_info for the stats_reader threads
	reader_info info_r[READERS];
	for (int i=0; i<READERS; i++) {
		info_r[i].market = &market_madrid;
		info_r[i].exit = &exit;
		info_r[i].exit_mutex = &exit_mutex;
		info_r[i].frequency = (rand()%10+1)*10000; //Frequency between 10000 and 100000
	}

	// Create brokers threads
	for (int i=0; i<BROKERS; i++) {
		pthread_create(&(tid[i]), NULL, &broker, (void*) &info_b[i]);
	}
	
	//Create executer thread
	pthread_create(&(tid[BROKERS]), NULL, &operation_executer, (void*) &info_ex1);

	//Create readers threads
	for (int i=0; i<READERS; i++) {
		pthread_create(&(tid[BROKERS+EXECUTERS+i]), NULL, &stats_reader, (void*) &info_r[i]);
	}

	// Join broker threads
	void * res;
	for (int i=0; i<BROKERS; i++) {
		pthread_join(tid[i],&res);
	}

	// Put exit flag = 1 after brokers completion
	pthread_mutex_lock(&exit_mutex);
	exit = 1;
	pthread_mutex_unlock(&exit_mutex);
	
	// Join the rest of the threads
	for (int i=BROKERS; i<BROKERS+EXECUTERS+READERS; i++) {
		pthread_join(tid[i],&res);
	}
	
	// Print final statistics of the market
	print_market_status(&market_madrid);
	
	// Destroy market and concurrency mechanisms
	delete_market(&market_madrid);
	destroy_concurrency_mechanisms();
	pthread_mutex_destroy(&exit_mutex);
  
	return 0;
}
