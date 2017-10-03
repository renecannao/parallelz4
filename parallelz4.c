#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <semaphore.h>
#include <lz4.h>
#include <errno.h>
#include <zlib.h>
#include <assert.h>
#include <stdint.h>

/* compile:

$ cc parallelz4.c -o parallelz4 -pthread -lz -llz4 -O2

*/


// hardcoded fixed number of threads
// the number of inut threads and output threads can be different
#define NTHS	8

// hardcoded fixed chunk size
// compression and uncompression chunk size MUST match
#define BS	256*1024

sem_t semIN[NTHS];
sem_t semOUT[NTHS];

int do_compress = -1;

int bso = 0;
int cont = 1;

/* Input buffer. */
static char *ibuf[NTHS];

/* Output buffer. */
static char *obuf[NTHS];

/* input buffer size */
uint32_t ibufsize[NTHS];

/* output buffer size */
uint32_t obufsize[NTHS];

/* crc32 */
uint32_t crc[NTHS];

static pthread_t pthd[NTHS];
int indexes[NTHS];


void * worker_compress(void *arg) {
	int index = *(int *)arg;
	while (1) {
		sem_wait(&semIN[index]);
		obufsize[index] = LZ4_compress_default(ibuf[index], obuf[index], ibufsize[index], bso);
		assert (obufsize[index] > 0);
		unsigned long  crcl = 0;
		// generate crc32
		crcl = crc32(crcl, (const unsigned char*)ibuf[index], ibufsize[index]);
		crc[index] = crcl;
		sem_post(&semOUT[index]);
	}
	return NULL;
}


void * worker_decompress(void *arg) {
	int index = *(int *)arg;
	while (1) {
		sem_wait(&semIN[index]);
		obufsize[index] = LZ4_decompress_safe(ibuf[index], obuf[index], ibufsize[index], BS);
		assert (obufsize[index] > 0);
        unsigned long  crcl = 0;
		// generate crc32
        crcl = crc32(crcl, (const unsigned char*)obuf[index], obufsize[index]);
		// compare crc32
        assert(crc[index] = crcl);
		sem_post(&semOUT[index]);
	}
	return NULL;
}


int main(int argc, char *argv[]) {
	int i;
    int opt;

	// opt parsing
	if (argc != 2 ) {
		fprintf(stderr,"Usage: %s ( -c | -d)\n", argv[0]);
		exit(EXIT_FAILURE);
	}
    while ((opt = getopt(argc, argv, "cd")) != -1) {
		switch (opt) {
			case 'c':
				do_compress = 1;
				break;
			case 'd':
				do_compress = 0;
				break;
			default:
				fprintf(stderr,"Usage: %s ( -c | -d)\n", argv[0]);
				exit(EXIT_FAILURE);
		}
	}

	// initialization loop
	bso = LZ4_compressBound(BS);
	for (i=0; i<NTHS; i++) {
		sem_init(&semIN[i],0,0);
		sem_init(&semOUT[i],0,0);
		ibuf[i]=malloc(BS);
		if (ibuf[i]==NULL) {
			perror("malloc()");
			exit(EXIT_FAILURE);
		}
		obuf[i]=malloc(BS);
		if (obuf[i]==NULL) {
			perror("malloc()");
			exit(EXIT_FAILURE);
		}
		indexes[i]=i;
	}

	// create threads
	for (i=0; i<NTHS; i++) {
		pthread_create(&pthd[i], NULL, ( do_compress ? worker_compress : worker_decompress ), &indexes[i]);
	}

	ssize_t nread;
	uint32_t bsize;

	// main loop
	// the main loop is responsible for reading from stdin, passing the data
	// to workers, and wrie to stdout
	// the main loop does all the IO
	if (do_compress) {
		while (cont) {
			i = 0;
			while (cont && i < NTHS) {
				int partial = 0;
				do {
					nread = read (STDIN_FILENO, ibuf[i]+partial, BS-partial);
					if (nread >= 0) {
						partial += nread;
					}
				} while ((nread < 0 && errno == EINTR) || (nread > 0 && partial < BS));
				ibufsize[i] = partial;
				if (partial < BS) {
					cont = 0;
				}
				sem_post(&semIN[i]);
				i++;
			}
			int exp_blocks = i;
			for (i = 0 ; i < exp_blocks; i++) {
				sem_wait(&semOUT[i]);
				int rc;
				// write block size
				rc = write(STDOUT_FILENO, &obufsize[i], sizeof(uint32_t));
				// write checksum
				rc = write(STDOUT_FILENO, &crc[i], sizeof(uint32_t));
				// write the compressed block
				rc = write(STDOUT_FILENO, obuf[i], obufsize[i]);
			}
		}
	} else { // uncompress
		while (cont) {
			i = 0;
			while (cont && i < NTHS) {
				int partial = 0;
				// read the header
				int rc = read (STDIN_FILENO, &bsize, sizeof(uint32_t));
				if (rc > 0) {
					// read the checksum
					rc = read (STDIN_FILENO, &crc[i], sizeof(uint32_t));
					// read the compressed block
					do {
						nread = read (STDIN_FILENO, ibuf[i]+partial, bsize-partial);
						if (nread >= 0) {
							partial += nread;
						}
					} while ((nread < 0 && errno == EINTR) || (nread > 0 && partial < bsize));
					ibufsize[i] = partial;
					sem_post(&semIN[i]);
					i++;
				} else {
					cont = 0;
				}
			}
			int exp_blocks = i;
			for (i = 0 ; i < exp_blocks; i++) {
				sem_wait(&semOUT[i]);
				// write the uncompressed blok
				int rc = write(STDOUT_FILENO, obuf[i], obufsize[i]);
			}
		}
	}
	return 0;

/*
	// we do not wait for graceful threads termination
	for (i=0; i<NTHS; i++) {
		pthread_join(pthd[i], NULL);
	}
	return 0;
*/
}
