/*
	Albert Szmigielski aszmgie@sfu.ca
	june 20, 2013 created
	june 27, 2013 rewritten in C, added buffer argument option
	june 28, 2013 added an option to read all files in a specified directory
		hoping to saturate the bandwidth of a SATA 3 connected SSD (6Gbits/s)
	june 28, 2013 change timer to only time the read (and not the overhead of thread creation) 
	july 17, 2013 adding an asynchronous IO option
	july 19 2013, adding parameters 
	july 29 2013, adding an -m option where all filesin a directory are divided up among threads
	pthread simple file read	
	times the duration  of read of a specified file (-f option), 
	-t option specifies number of threads,
 	-b option specifies size of elements to be read (2nd arg. to fread())


 gcc  frt_aio.c -o frt_aio  -fopenmp -pthread -lm -fno-stack-protector -lrt
	 -g  :  prepare to use by GDB
	 -lm math library
 	-fopenmp using open mp timers
  	-fno-stack-protector turn off stack protection
 	-lrt Real Time Extension for AI/O
	usage: (see explain_usage function or just run ./frt_aio)
							 ./frt_aio
 	options:		-t  <number of threads> 
							-f  <file to read> 
							-b <buffer reading size>
							-d <directory from which to read all files> 
*/

#include <aio.h>
#include <dirent.h> 
#include <errno.h>
#include <getopt.h>
#include <math.h>
#include <omp.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>






const int max_num_files=64;
long filesizes[64];
double readtimes[64][2];

 typedef struct 
	 {
		long size;
		int id;
		FILE* file;
		long chunk;
		char fname[30];
		char * dir;
	} thread_args;
long sum =0;
int Num_Threads=4; 	//default is 4 can be changed via argument
long buffer_size=1024; //bytes


int ASYNC =0;
int HUMAN=0, DEBUG=0, DEBUG_L2=0,DEBUG_L3=0, BUFFER=0, 	MULTIPLE=0;
int dirBool=0;
int fileBool=0, filesRead=0;

//char file_name[] ="/home/alberts/Programs/pig-0.11.1/files/btc-2010-chunk-000";

/**************************    AIO    ******************************/
 #define errExit(msg) do { perror(msg); exit(EXIT_FAILURE); } while (0)

 #define errMsg(msg)  do { perror(msg); } while (0)

struct ioRequest {      /* Application-defined structure for tracking
                                  I/O requests */
           int           reqNum;
           int           status;
           struct aiocb *aiocbp;
       };

       static volatile sig_atomic_t gotSIGQUIT = 0;
                               /* On delivery of SIGQUIT, we attempt to
                                  cancel all outstanding I/O requests */

       static void             /* Handler for SIGQUIT */
       quitHandler(int sig)
       {
           gotSIGQUIT = 1;
       }

       #define IO_SIGNAL SIGUSR1   /* Signal used to notify I/O completion */

       static void                 /* Handler for I/O completion signal */
       aioSigHandler(int sig, siginfo_t *si, void *ucontext)
       {
          // write(STDOUT_FILENO, "I/O completion signal received\n", 31);
			#pragma omp critical 
				{
				filesRead++;
				}
           /* The corresponding ioRequest structure would be available as
                  struct ioRequest *ioReq = si->si_value.sival_ptr;
              and the file descriptor would then be available via
                  ioReq->aiocbp->aio_fildes */
       }



// errexit *******************************************************
void
errexit (const char *err_str)
{
    fprintf (stderr, "%s", err_str);
    exit (1);
}

// ********************  BARRIER  ****************************************
pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
void
barrier (int expect)
{
    static int arrived = 0;

    pthread_mutex_lock (&mut);	//lock

    arrived++;
    if (arrived < expect)
        pthread_cond_wait (&cond, &mut);
    else {
        arrived = 0;		// reset the barrier before broadcast is important
        pthread_cond_broadcast (&cond);
    }

    pthread_mutex_unlock (&mut);	//unlock
}

// ***********************************

/**************************    PTHREAD READ    ******************************/
void * pthreadRead(void * args){
	thread_args *targs;
	targs = (thread_args *) args; /* type cast to a pointer to thread_args */
	long start_pos,read_pos, size=targs->size;
	int thread_id = targs->id;
	long chunk = targs->chunk;
	char* mem;
	double start, stop, time;
	long num_elements =1;
	FILE *file;

	if (DEBUG_L2) printf("thread %i in pthred read\n", thread_id);

	if (dirBool){
		char filename[20]; 
		strcpy (filename,  targs->fname); 
		char* dir = targs->dir;
		char directory[strlen(dir)];
		strcpy(directory,dir);
		char filepath[strlen(dir)+30];
		sprintf(filepath, "%s%s%s",directory, "/", filename);
		
		file = fopen (filepath, "r");
	 	if (file == NULL )	{
				if( HUMAN==1)
				{
					printf( "Unable to open file \n");
					printf ("Thread %i  filename: %s   \n" ,thread_id ,filename );
					printf ("path %s ", filepath);
				}
		}
		else 
			  {
				fseek(file, 0L, SEEK_END);
				size = ftell(file);
				filesizes[thread_id] = size;
				fseek(file, 0L, SEEK_SET);
				mem= (char *) malloc(size* sizeof(char));

				barrier(Num_Threads);// for timing purposes
				start=omp_get_wtime();
					fread (mem, size, 1, file);
				stop=omp_get_wtime();
				time = stop-start;
				readtimes[thread_id][0] = start;
				readtimes[thread_id][1] = stop;
				if (DEBUG) printf ("Thread %i  path %s  size: %li, time: %f  ...done \n" ,thread_id ,filepath, size,time );
				fclose(file);
				free ( mem);
			  }
	}

	if (fileBool){
		file = targs->file;
		if (thread_id == Num_Threads-1) 
		{chunk=ceil(size/Num_Threads);}
		if (DEBUG) printf( "in pthread_read %i  size:  %li  chunk %li \n", thread_id, size, chunk);
		if(! BUFFER) buffer_size=chunk;
		// read the file into a specified buffer size;
		//for (int i=1024; i<= max_buffer; i=i*2){
		num_elements = chunk/buffer_size;
		mem= (char *) malloc(chunk* sizeof(char));
		start_pos = chunk*thread_id;
		fseek(file, start_pos, SEEK_SET); //put *file to read_pos
		fread (mem, buffer_size, num_elements,file);
		//buffer_size = buffer_size*2;
		if (DEBUG)  printf ("Thread %i buffer: %li chunk: %li elements: %li reading from %li ...done \n" ,thread_id ,buffer_size, chunk, num_elements, start_pos );
		//printf ("buffer: %i chunk: %li elements: %li \n" ,buffer_size, chunk, num_elements);
		free ( mem);

	}


	// BARRIER
	barrier(Num_Threads);

}
/****************** ---------  END    pthread read   --------- *********************/




/**************************    AIO READ    ******************************/
// Code based ni part on the example code in the aio documentation
void  aioRead(int numReqs,char  (* filenames)[max_num_files], char * directory){

		struct ioRequest *ioList;
	   	struct aiocb *aiocbList;
	   	struct sigaction sa;
	

		int openReqs;       /* Number of I/O requests still in progress */
	 	//int numReqs;        /* Total number of queued I/O requests */
		int lastThread =0;
		int slastThread=0; //2nd last thread
		if (numReqs> max_num_files) numReqs =  	max_num_files;
		if (DEBUG) printf("Number of fIles to read: %i \n",numReqs);
		   ioList = calloc(numReqs, sizeof(struct ioRequest));
		   if (ioList == NULL)
		       errExit("calloc");

		   aiocbList = calloc(numReqs, sizeof(struct aiocb));
		   if (aiocbList == NULL)
		       errExit("calloc");

		   /* Establish handlers for SIGQUIT and the I/O completion signal */

		   sa.sa_flags = SA_RESTART;
		   sigemptyset(&sa.sa_mask);

		   sa.sa_handler = quitHandler;
		   if (sigaction(SIGQUIT, &sa, NULL) == -1)
		       errExit("sigaction");

		   sa.sa_flags = SA_RESTART | SA_SIGINFO;
		   sa.sa_sigaction = aioSigHandler;
		   if (sigaction(IO_SIGNAL, &sa, NULL) == -1)
		       errExit("sigaction");

			int local_numReqs= numReqs;
			if (Num_Threads > 1){
				 local_numReqs = (numReqs)/(Num_Threads-1);
				slastThread = Num_Threads-2; 
				lastThread = Num_Threads-1;
				}
			else
				 local_numReqs = (numReqs);

			if (DEBUG_L2) printf( "local_numReqs: %i \n",local_numReqs);
	if (Num_Threads >0){
			#pragma omp parallel num_threads(Num_Threads)
			{
				int thread_id = omp_get_thread_num();
				//READ
				if ( thread_id != lastThread || Num_Threads == 1){	
					int start = thread_id *  local_numReqs;
					int stop = start + local_numReqs-1;
					if (thread_id == slastThread) stop = numReqs-1; //2nd last thread gets the remainder as well 
					//last thread (thread_id-1) will do the monitoring
					if (DEBUG_L2) printf( "omp thread #%i start: %i, stop: %i \n",thread_id, start, stop);
					int s,j;
					for (j = start; j <= stop; j++) { 
		
						///////  GET FILE SIZES  ////////
						FILE * file;
						char  * filename;
						long fsize;
						filename = filenames[j];
						char direct[strlen(directory)];
						strcpy(direct,directory);
						char filepath[strlen(directory)+30];
						sprintf(filepath, "%s%s%s",direct, "/", filename);
						file = fopen (filepath, "r");
						if (file !=NULL){
							fseek(file, 0L, SEEK_END);
							fsize = ftell(file);
							if(fsize ==0) printf("File size is ZERO\n");
							fclose(file);
							if(DEBUG_L2) printf("thread #%i filename: %s file: %s \n", thread_id, filename, filepath);
							if(DEBUG_L2) printf("file: %s \n", filepath);
						}
						/////////////

							ioList[j].reqNum = j;
					   		ioList[j].status = EINPROGRESS;

						   ioList[j].aiocbp = &aiocbList[j];

						   ioList[j].aiocbp->aio_fildes = open(filepath, O_RDONLY);


						   if (ioList[j].aiocbp->aio_fildes == -1)
						       errExit("open");
						   if (DEBUG_L2) printf("thread #%i opened %s on descriptor %d\n", thread_id, filepath,
						           ioList[j].aiocbp->aio_fildes);

						   ioList[j].aiocbp->aio_buf = malloc(fsize+1);//(BUF_SIZE);
						   if (ioList[j].aiocbp->aio_buf == NULL)
						       errExit("malloc");

						   ioList[j].aiocbp->aio_nbytes = (fsize);//BUF_SIZE;
						   ioList[j].aiocbp->aio_reqprio = 0;
						   ioList[j].aiocbp->aio_offset = 0;
						   ioList[j].aiocbp->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
						   ioList[j].aiocbp->aio_sigevent.sigev_signo = IO_SIGNAL;
						   ioList[j].aiocbp->aio_sigevent.sigev_value.sival_ptr = &ioList[j];

							#pragma omp critical 
							{
						  	 s = aio_read(ioList[j].aiocbp);
							}
						   if (s == -1)
						       errExit("aio_read");
					}// end for
				}//end if   READ
					
				// AIO MONITOR******
				if (thread_id == lastThread) {			
					openReqs = numReqs;
					int s,j;
					//last thread (thread_id-1) will do the monitoring
					
						while (openReqs > 0) {
						    if ( DEBUG_L3 || (Num_Threads==1) ) sleep(1);       /* Delay between each monitoring step */

						   if (gotSIGQUIT) {

						       /* On receipt of SIGQUIT, attempt to cancel each of the
						          outstanding I/O requests, and display status returned
						          from the cancellation requests */

						      if (DEBUG_L2)  printf("got SIGQUIT; canceling I/O requests: \n");
							
						       for (j = 0; j < numReqs; j++) {
						           if (ioList[j].status == EINPROGRESS) {
						               if (DEBUG_L2) printf("    Request %d on descriptor %d:", j,
						                       ioList[j].aiocbp->aio_fildes);
						               s = aio_cancel(ioList[j].aiocbp->aio_fildes,
						                       ioList[j].aiocbp);
						               if (s == AIO_CANCELED)
						                   printf("I/O canceled\n");
						               else if (s == AIO_NOTCANCELED)
						                      if (DEBUG_L2)  printf("I/O not canceled\n");
						               else if (s == AIO_ALLDONE)
						                  if (DEBUG_L2)  printf("I/O all done\n");
						               else
						                   errMsg("aio_cancel");
						           }
						       }

						       gotSIGQUIT = 0;
						   }

						   /* Check the status of each I/O request that is still
						      in progress */
						//if (DEBUG_L2) printf("aio_error(): ");
						
							   for (j = 0; j < numReqs; j++) {
								   //if (DEBUG_L2) printf("req # %i status: %i \n", j, ioList[j].status);
								   if (ioList[j].status == EINPROGRESS) {
								      // if (DEBUG_L2) printf("for request %d descriptor %d: ", j, ioList[j].aiocbp->aio_fildes);
									
									
								       ioList[j].status = aio_error(ioList[j].aiocbp);
									
								       switch (ioList[j].status) {
								       case 0:
								           if (DEBUG_L2) printf("I/O succeeded\n");
								           break;
								       case EINPROGRESS:
								          if (DEBUG_L2)  printf("In progress\n");
								           break;
								       case ECANCELED:
								           if (DEBUG) printf("Canceled\n");
								           break;
								       default:
								           if (DEBUG) errMsg("aio_error");
								           break;
								       }

								       if (ioList[j].status != EINPROGRESS)
								           openReqs--;
											if (DEBUG_L2) printf("openReqs %i \n",openReqs);
								   }
							   }
						
					   }

					   if (DEBUG) printf("All I/O requests completed\n");

					   /* Check status return of all I/O requests */

					  
						  if (DEBUG)printf("aio_return(): \n");
						   for (j = 0; j < numReqs; j++) {
							   ssize_t s;

							   s = aio_return(ioList[j].aiocbp);
								sum= sum + (long)s;
								if (s==-1) printf("	file: %s", filenames[numReqs]);
							    if (DEBUG) printf("    for request %d (descriptor %d): %ld\n",
								       j, ioList[j].aiocbp->aio_fildes, (long) s);
							}
							  if (DEBUG) printf("aio_return(): recieved signal for %i files\n", filesRead);
					
						
					}//end monitor

		}// end omp
	}// end if num_thread>1
	else {
		printf("PROBLEM: Num_Threads %i \n", Num_Threads);
	}

// SINGLE THREADED*******************************
if (Num_Threads ==0){
	int s,j;
       // Open each file specified on the command line, and queue
        //  a read request on the resulting file descriptor 

 	for (j = 0; j < numReqs; j++) {

			///////  GET FILE SIZES  ////////
			FILE * file;
			char  * filename;
			long fsize;
			filename = filenames[j];
			char direct[strlen(directory)];
			strcpy(direct,directory);
			char filepath[strlen(directory)+30];
			sprintf(filepath, "%s%s%s",direct, "/", filename);
			file = fopen (filepath, "r");
			fseek(file, 0L, SEEK_END);
			fsize = ftell(file);
			fclose(file);
			if(DEBUG) printf("filename: %s \n", filename);
			if(DEBUG) printf("file: %s \n", filepath);
			/////////////

               ioList[j].reqNum = j;
               ioList[j].status = EINPROGRESS;
               ioList[j].aiocbp = &aiocbList[j];

               ioList[j].aiocbp->aio_fildes = open(filepath, O_RDONLY);
               if (ioList[j].aiocbp->aio_fildes == -1)
                   errExit("open");
               printf("opened %s on descriptor %d\n", filepath,
                       ioList[j].aiocbp->aio_fildes);

               ioList[j].aiocbp->aio_buf = malloc(fsize);//(BUF_SIZE);
               if (ioList[j].aiocbp->aio_buf == NULL)
                   errExit("malloc");

               ioList[j].aiocbp->aio_nbytes = (fsize);//BUF_SIZE;
               ioList[j].aiocbp->aio_reqprio = 0;
               ioList[j].aiocbp->aio_offset = 0;
               ioList[j].aiocbp->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
               ioList[j].aiocbp->aio_sigevent.sigev_signo = IO_SIGNAL;
               ioList[j].aiocbp->aio_sigevent.sigev_value.sival_ptr =
                                       &ioList[j];

               s = aio_read(ioList[j].aiocbp);
               if (s == -1)
                   errExit("aio_read");
           }

           openReqs = numReqs;

           // Loop, monitoring status of I/O requests 
	while (openReqs > 0) {
               sleep(30);       // Delay between each monitoring step 

               if (gotSIGQUIT) {

                   // On receipt of SIGQUIT, attempt to cancel each of the
                  //    outstanding I/O requests, and display status returned
                 //     from the cancellation requests 

                   printf("got SIGQUIT; canceling I/O requests: \n");

                   for (j = 0; j < numReqs; j++) {
                       if (ioList[j].status == EINPROGRESS) {
                           printf("    Request %d on descriptor %d:", j,
                                   ioList[j].aiocbp->aio_fildes);
                           s = aio_cancel(ioList[j].aiocbp->aio_fildes,
                                   ioList[j].aiocbp);
                           if (s == AIO_CANCELED)
                               printf("I/O canceled\n");
                           else if (s == AIO_NOTCANCELED)
                                   printf("I/O not canceled\n");
                           else if (s == AIO_ALLDONE)
                               printf("I/O all done\n");
                           else
                               errMsg("aio_cancel");
                       }
                   }

                   gotSIGQUIT = 0;
               }

               // Check the status of each I/O request that is still
               //   in progress 

			printf("aio_error():\n");
               for (j = 0; j < numReqs; j++) {
                   if (ioList[j].status == EINPROGRESS) {
                       printf("    for request %d (descriptor %d): ",
                               j, ioList[j].aiocbp->aio_fildes);
                       ioList[j].status = aio_error(ioList[j].aiocbp);

                       switch (ioList[j].status) {
                       case 0:
                           printf("I/O succeeded\n");
                           break;
                       case EINPROGRESS:
                           printf("In progress\n");
                           break;
                       case ECANCELED:
                           printf("Canceled\n");
                           break;
                       default:
                           errMsg("aio_error");
                           break;
                       }

                       if (ioList[j].status != EINPROGRESS)
                           openReqs--;
                   }
               }
           }

           printf("All I/O requests completed\n");

           // Check status return of all I/O requests 

           printf("aio_return():\n");
           for (j = 0; j < numReqs; j++) {
               ssize_t s;

               s = aio_return(ioList[j].aiocbp);
               printf("    for request %d (descriptor %d): %ld\n",
                       j, ioList[j].aiocbp->aio_fildes, (long) s);
           }
		}
	

}
/******************* --------  END     aio read    ---------  **************************/


/**************************    PAR READ    ******************************/
void par_read(long size, char  (* filenames)[max_num_files], char * directory, FILE *file) {
	long chunk = floor(size/Num_Threads); 
	// break into num_threads chunks, read each chunk
	pthread_attr_t attr;
    pthread_t *tid;
	int *id;
	long i; 
	void * function;
	
	function = pthreadRead; 

	// create threads
	id = (int *) malloc (sizeof (int) * Num_Threads);

	tid = (pthread_t *) malloc (sizeof (pthread_t) * Num_Threads);
	if (!id || !tid)
	    errexit ("out of shared memory");

	pthread_attr_init (&attr);
	pthread_attr_setscope (&attr, PTHREAD_SCOPE_SYSTEM);
	if (DEBUG) printf( "about to create threads \n"); 

	
	 thread_args  threadArgs[Num_Threads];
	for (i = 1; i < Num_Threads; i++) {
		threadArgs[i].id=i;
		threadArgs[i].size = size;
		threadArgs[i].chunk = chunk;
		if (dirBool) {
			strcpy(threadArgs[i].fname, filenames[i]);
		}
		if (fileBool) threadArgs[i].file = file;
		threadArgs[i].dir = directory;
	    pthread_create (&tid[i], &attr, function,  &threadArgs[i]);
	}

	threadArgs[0].id=0;
	threadArgs[0].size=size;
	threadArgs[0].chunk = chunk;
	if (dirBool) strcpy(threadArgs[0].fname, filenames[0]);
	if (fileBool) threadArgs[0].file= file;
	threadArgs[0].dir = directory;
	id[0]=0;
	pthreadRead(&threadArgs[0]);

	// wait for all threads to finish
	for (i = 1; i < Num_Threads; i++)
	    pthread_join (tid[i], NULL);

}

/*****************************    Multiple Read     *********************************/
void multipleRead(int numReqs,char  (* filenames)[max_num_files], char * directory) {
// read files from directory, dived the files among available number of threads.

		
		int lastThread = Num_Threads-1;
		double start, stop, time;

		
		if (numReqs> max_num_files) numReqs =  	max_num_files;
		if (DEBUG) printf("Number of fIles to read: %i \n",numReqs);
  
		#pragma omp parallel num_threads(Num_Threads)
		{
		int j;
		char* mem;
		FILE * file;
		char  * filename;
		long fsize;
		char filepath[strlen(directory)+30];
		int local_numReqs= numReqs;
		if (Num_Threads > 1) local_numReqs = (numReqs)/(Num_Threads);
		else local_numReqs = numReqs;

		if (DEBUG_L2) printf( "local_numReqs: %i \n",local_numReqs);
		int thread_id = omp_get_thread_num();
		int start_t = thread_id *  local_numReqs;
		int stop_t = start_t + local_numReqs-1;
		if (thread_id == lastThread) stop_t = numReqs-1; // last thread gets the remainder as well 
		if (DEBUG_L2) printf( "omp thread #%i start: %i, stop: %i \n",thread_id, start_t, stop_t);

		for (j = start_t; j <= stop_t; j++) { 
			filename = filenames[j];
			sprintf(filepath, "%s%s%s",directory, "/", filename);
			file = fopen (filepath, "r");
			if (file !=NULL){
				fseek(file, 0L, SEEK_END);
				fsize = ftell(file);
				fseek(file, 0L, SEEK_SET);
				if(fsize ==0) printf("File size is ZERO\n");				
				if(DEBUG_L2) printf("j: %i thread #%i filename: %s filepath: %s \n",j, thread_id, filename, filepath);
				filesizes[j] = fsize;
				mem= (char *) malloc(fsize* sizeof(char));
				start=omp_get_wtime();
					fread (mem, fsize, 1, file);
				stop=omp_get_wtime();
				time = stop-start;
				readtimes[j][0] = start;
				readtimes[j][1] = stop;
				if (DEBUG) printf ("T %i  path %s  size: %li, time: %f  ...done \n" ,thread_id ,filepath, fsize,time );
				fclose(file);
				free ( mem);
			}
			else {
				if( HUMAN) {
				printf( "Unable to open file ... \n");
				printf ("Thread %i  filename: %s   \n" ,thread_id ,filename );
				printf ("path %s ", filepath);
				}
			}
		}// end for
	}

					

}

/*****************************    explain usage     *********************************/
void explain_usage(){
	printf ("This program needs at least one parameter either a directory -d, or a file -f \n");
	printf ("The following is a list of parameters: \n");
	printf ("	-a use Asynchronous read \n");
	printf ("	-b specify buffer size  <buffer size>\n");
	printf ("	-d specify directory from which all files will be read <path to directory>\n");
	printf ("	-f specify the filename to be read <path to file>\n");
	printf ("	-h output information to screen in human readable form \n");
	printf ("	-m files will be divided among threads when this option is used, \n\t   otherwise each thread will only read 1 file. -m is used inly with -d \n");
	printf ("	-p print debug statements \n");
	printf ("	-q print debug statements level 2 \n");
	printf ("	-r to introduce a delay in the aio monitor \n");
	printf ("	-t specify number of threads <number of threads> \n");

}
/*****************************    ARGS     *********************************/
static struct option long_options[] =
  {
	//{"human", 		no_argument, 					&HUMAN, 1},
	{"async", 			no_argument,						0, 'a'},
	{"human", 			no_argument, 					0, 'h'},	
	{"multiple", 		no_argument, 					0, 'm'},
	{"debug", 			no_argument, 					0, 'p'},
	{"ddebug", 		no_argument, 					0, 'q'},
	{"3debug", 		no_argument, 					0, 'r'},
    {"buffer",			required_argument, 		0, 'b'},
	{"dir",					required_argument,		0, 'd'},
    {"file", 					required_argument, 		0, 'f'},
	{"threads", 		required_argument, 		0,  't'},	
    {0, 0,0,0,}
  };

/****************************   MAIN   **********************************/
int main(int argc, char** argv){

	long size;
	double start, stop, time;
	char  * directory = malloc(256*sizeof(char));
	char  * filename = calloc(256,sizeof(char));	
	double time_sum=0;
	double speed =0.0;
	int num_files=0;
	double read_min=2147483648, read_max=-2147483648;
	char filenames[20][max_num_files] ; 
	FILE * file;
	int PROCEED=0;


	// Code based on the example code in the getopt documentation
  while (1) {

    int option_index = 0;    
    int c = getopt_long_only(argc, argv, "ahmpqrb:d:f:t:",
                             long_options, &option_index);
    
    /* Detect the end of the options. */
    if (c == -1)
      break;
    
    switch (c) {
		case 0:
		  /* If this option set a flag, do nothing else now. */
		  break;

		case 'a':		//asynchronous read
		  ASYNC = 1; 
		  if (DEBUG_L2)  printf("case a\n");
		  break;
  
		case 'b':		 //varry buffer size
		 BUFFER=1;
		  buffer_size = (int)(atoi(optarg));
		  break;

		case 'd':	//read all files in a directory
		  strcpy(directory, optarg); 
		  dirBool =1;
		  if (DEBUG_L2) printf("case d\n");
		  break;

		case 'f':
		  strcpy(filename,optarg);
		  fileBool=1; 
		  break;

		case 'h': // output in human readable form
		  HUMAN = 1;
		 if (DEBUG_L2) printf("case h\n");
		  break;

		case 'm': // 
		  MULTIPLE = 1;
		 if (DEBUG_L2) printf("case m\n");
		  break;

		case 'p': // debug
		  DEBUG = 1;
		if (DEBUG_L2) printf("case p\n");
		  break;

		case 'q': // debug level 2
		  	DEBUG = 1;
			DEBUG_L2 = 1;
		 	if (DEBUG_L2) printf("case q\n");
		  break;
		
			case 'r': // debug level 2
		  	DEBUG_L3 = 1;
		  break;
		  
		case 't':
		  Num_Threads = (int)(atoi(optarg));
		  if (DEBUG_L2) printf("case t\n");
		  break;
		
		case '?':
		  /* getopt_long already printed an error message. */
		  exit(1);
		  break;
		  
		default:
		  exit(1);
   	 }// end switch
  	}// end while


	//if (argc>1)  Num_Threads= atoi(argv[1]);
	//if (argc>2)  file_name = argv[2];
	/* PRINT OUT THe DIRECTORY CONTENTS*/
	if (dirBool) {
			
			PROCEED=1;
			char filepath[strlen(directory)+30];
		 	DIR           *d, *f;
		 	 struct dirent *dir;
		 	 d = opendir(directory);
			  if (d)
			  {

					while ((dir = readdir(d)) != NULL)
					{
						
						sprintf(filepath, "%s%s%s",directory, "/", dir->d_name);
						if (DEBUG_L2) {
							printf("filepath: %s\n", filepath);
							printf("directory: %s\n", directory);
						}
						f =opendir(filepath);
						if ( (strcmp(dir->d_name , ".") !=0) &&  (strcmp(dir->d_name ,"..") !=0) && (!f) ){
						 	if (DEBUG_L2)printf("file: %s\n", dir->d_name);
							if (DEBUG_L2)printf("length: %i\n", (int)strlen(dir->d_name));
							//strncpy(filename, dir->d_name, strlen(dir->d_name));
							if (num_files<max_num_files) {
								strcpy(filenames[num_files],  dir->d_name);
								num_files++;
								
							}
						}
						closedir(f);
					}
					if (DEBUG_L2)printf("out of  while\n");
					closedir(d);
					if (Num_Threads > num_files) {
						Num_Threads=num_files;
						if (HUMAN) printf("More threads than files. Limiting number of threads to number of files.\n");
				
					}
			  }
				else {
					printf( "Unable to open dir \n");
					exit(1);
				}
	}//end if dirBool

	if (fileBool){
		file = fopen (filename, "r");
		if (file == NULL)
			printf( "Unable to open file \n");
		else{
			fseek(file, 0L, SEEK_END);
			size = ftell(file);
		 	if (HUMAN) printf(" Size = %li  Number of Threads = %i  \n" ,size, Num_Threads); 
			fseek(file, 0L, SEEK_SET);
			PROCEED=1;
		}
	}

	if (PROCEED){
		start=omp_get_wtime();
			if(ASYNC) aioRead(num_files, filenames, directory);
			else if(MULTIPLE) multipleRead(num_files, filenames, directory);
						else par_read(size, filenames, directory,file); 

		stop=omp_get_wtime();
		time=(stop-start);

		if (dirBool){
			if(!ASYNC){
				int i;
				int number;
				if (MULTIPLE) number = num_files;
				else number = Num_Threads;
				for (i=0; i<number; i++){
					sum+=filesizes[i];
					if (readtimes[i][0] < read_min) {
						read_min = readtimes[i][0];
					}
					if (readtimes[i][1] > read_max) {
						read_max = readtimes[i][1];

					}
				}
				if (DEBUG) {
					 printf("min: %f \n", read_min);
					 printf("max: %f \n", read_max);
				}
				time_sum = read_max-read_min;
				double speed_f;
				sum = sum/(1024*1024);
				speed_f = sum /time_sum;
				speed = sum /time;
				if (DEBUG) printf("time_sum: %f \n", time_sum);
				if (HUMAN) {
					printf("result (excl. overhead): read %li MB in %f sec. for a speed of %f MB/s \n", sum, time_sum, speed_f);
					printf("result (incl. overhead): read %li MB in %f sec. for a speed of %f MB/s \n", sum, time_sum, speed);
				}								
				else printf(" %i,  %f \n", Num_Threads,  speed);
			}
			else {				
				sum = sum/(1024*1024);
				speed = sum /time;
				if (HUMAN){
					printf ("finished \n");
					printf("Incl. overhead read %li MB in %f sec. for a speed of %f MB/s \n", sum, time, speed);
				}
				else 	printf(" %i,  %f \n", Num_Threads,  speed);
			}
			
			if (DEBUG){
				
				printf("num of threads %i \n",Num_Threads);
			}
			else {
				//print information in CSV format, so its easy for a spreadsheet	
					
			}
		}

		if (fileBool){
			fclose(file);
			size= size/(1024*1024);
			speed = size / time;
			if (HUMAN) {
				printf ("the complete file content is in memory \n");
				printf  ("size: %li MB, time : %f sec. , speed %f MB/s\n",size,time,speed) ;
			}
			else printf  ("%li, MB, %f, sec. , %f, MB/s\n",size,time,speed) ;
		}	
	free(directory);
	free(filename);
	
	}
	
	else explain_usage();
	
	

	  



return 0;
}


