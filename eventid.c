/*
 * 2011.04.01 ivan <github-ivan@enfinity.hu>
 *
 * Unique id generator server inspired by
 * twitter's snowflake.
 *
 * Id composed:
 *   41 bit time (millisecond resulution)
 *   10 bits worker id
 *   13 bits counter
 *
 *   10 bits counter id (/<counter id>)
 *
 * Implementation based on Kuzuki Ohta's sample
 *
 */

#define __STDC_LIMIT_MACROS
#define __STDC_FORMAT_MACROS    /* Enable PRIu64 in <inttypes.h> */

#include <stdio.h>
#include <event.h>
#include <evhttp.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sched.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <netdb.h>
#include <inttypes.h>


#define zepoch        1288834974657L
#define timeBits      41
#define workerIdBits  10
#define counterBits   12
#define numCounterId  128

#ifdef DEBUG
#define DPRINT(...) fprintf(__VA_ARGS__);
#else
#define DPRINT(...)
#endif

#ifndef DEFLOGFILE
#define DEFLOGFILE "/var/log/eventid.log"
#endif

#define RET(rv, ...) { fprintf(__VA_ARGS__); return (rv) ? rv : NULL; }

using namespace std;

namespace enfinity {

namespace eventid {

class HTTPServer {

public:
  HTTPServer();
  ~HTTPServer() {}

  int processParams(int argc, char** argv);
  int daemonize();
  int serv();

protected:
  struct thread_arg {
    struct event_base* eventBase;
    int workerId;
  };
  struct counter_item {
    uint64_t lastMillisec;
    uint32_t counter;
  };
  const char* logFile;
  struct sockaddr_in listenAddr;
  int listenPort;
  bool isDaemon;
  int backlogSize;
  static FILE* logfd;
  uint64_t maxWorkerId, timestampLeftShift, maxCounter;
  static uint64_t workerIdShift;
  static int retVal, cpuCount, workerNo, workerOffset;
  static __thread uint64_t workerId;
  static __thread struct counter_item* counters;
  static void* dispatch(void* arg);
  static void genericHandler(struct evhttp_request *req, void *arg);
  inline uint64_t getMillisec();
  void processRequest(struct evhttp_request *request);
  int bindSocket();
  void usage();
};

int HTTPServer::retVal = 0;
FILE* HTTPServer::logfd = NULL;
int HTTPServer::cpuCount = 0;
int HTTPServer::workerNo = 0;
int HTTPServer::workerOffset = 0;
uint64_t HTTPServer::workerIdShift = 0;
__thread uint64_t HTTPServer::workerId = 0;
__thread struct HTTPServer::counter_item* HTTPServer::counters = NULL;


HTTPServer::HTTPServer() {
  retVal = 0;
  maxWorkerId = UINT64_MAX ^ (UINT64_MAX << workerIdBits);
  maxCounter = UINT64_MAX ^ (UINT64_MAX << counterBits);
  workerIdShift = counterBits;
  timestampLeftShift = counterBits + workerIdBits;
  cpuCount = sysconf(_SC_NPROCESSORS_ONLN);
  workerNo = cpuCount;
  isDaemon = false;
  logfd = stderr;
  listenPort = 3843;
  backlogSize = -1;
  logFile = DEFLOGFILE;
  memset(&listenAddr, 0, sizeof(listenAddr));
  listenAddr.sin_family = AF_INET;
  listenAddr.sin_addr.s_addr = INADDR_ANY;
  listenAddr.sin_port = htons(listenPort);
}


void HTTPServer::usage() {
  fprintf(stdout, "Usage: eventid [-d] [-b <backlog_size>] [-t <thread no>] "
                  "[-l <remote address:port>] [-f <logfile path>]\n");
}


int HTTPServer::processParams(int argc, char* argv[]) {
  int c, intval;
  char* colonpos;
  struct hostent* host;

  if (argc == 1)
      return 0;

  while ((c = getopt (argc, argv, "d?l:b:f:t:")) != -1)
    switch (c)
    {
      case 'd':
	isDaemon = true;
	break;
      case 'f':
	logFile = optarg;
	break;
      case 'b':
	intval = atoi(optarg);
	if (intval < 0) RET(-101, logfd, "Invalid backlog_size value: %d.\n", intval);
	backlogSize = intval;
	break;
      case 't':
	intval = atoi(optarg);
	if (intval < 0)
	    RET(-102, logfd, "Invalid worker number value: %d, max value: %"PRIu64".\n",
                            intval, maxWorkerId);
	workerNo = intval;
	break;
      case 'l':
	colonpos = strrchr(optarg, 58);
	if (colonpos != NULL) {
	    *colonpos = 0;
	    intval = atoi(colonpos + 1);
	    if ((intval < 1) || (intval > 65535))
		RET(-103, logfd, "Invalid remote port value: %d.\n", intval);
	    listenPort = intval;
	    listenAddr.sin_port = htons(intval);
	}

        host = gethostbyname(optarg);
	if (! host) RET(-104, logfd, "Invalid remote address.\n");
	memcpy(&(listenAddr.sin_addr.s_addr), host->h_addr, sizeof(host->h_addr));
	listenAddr.sin_family = host->h_addrtype;
	break;
      case '?':
	usage();
	return -100;
	break;
     default:
        break;
    }

  return 0;
}

int HTTPServer::daemonize()
{
  int pid;
  int sid;
  FILE* rv;

  if (! isDaemon) return 0;

  pid = fork();
  if (pid < 0) RET(-201, logfd, "Can't fork.\n");
  if (pid > 0) exit(EXIT_SUCCESS);

  /* Change the file mode mask */
  umask(0);

  /* Create a new SID for the child process */
  sid = setsid();
  if (sid < 0) RET(-202, logfd, "Can't set new session id.\n");

  if ((chdir("/")) < 0) RET(-4, logfd, "Can't chdir.\n");

  close(STDIN_FILENO);
  close(STDOUT_FILENO);

  // TODO: implement better logging
  rv = fopen(logFile, "w+");
  if (rv == NULL) RET(-203, logfd, "Can't open logfile: %s,\n", logFile);
  fclose(logfd);
  logfd = rv;

  return 0;
}


int HTTPServer::bindSocket() {
  int rv;
  int lfd;
  lfd = socket(listenAddr.sin_family, SOCK_STREAM, 0);
  if (lfd < 0) RET(-301, logfd, "Can't create socket.");

  int one = 1;
  rv = setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, (char*) &one, sizeof(int));

  rv = bind(lfd, (struct sockaddr*) &listenAddr, sizeof(listenAddr));
  if (rv < 0) RET(-302, logfd, "Can't bind socket.");
  rv = listen(lfd, backlogSize);
  if (rv < 0) RET(-303, logfd, "Can't listen on socket.");

  int flags;
  if ((flags = fcntl(lfd, F_GETFL, 0)) < 0 || fcntl(lfd, F_SETFL, flags | O_NONBLOCK) < 0)
    RET(-304, logfd, "Can't set non-blocking on socket.");

  return lfd;
}

int HTTPServer::serv() {
  int rv;
  int lfd = bindSocket();
  if (lfd < 0) return lfd;
  pthread_t ths[workerNo];
  struct thread_arg threadArg[workerNo];
  for (int i = 0; i < workerNo; i++) {
    struct event_base *base = event_init();
    if (base == NULL) RET(-401, logfd, "event_init() error.");
    struct evhttp *httpd = evhttp_new(base);
    if (httpd == NULL) RET(-402, logfd, "evhttp_new() error.");
    rv = evhttp_accept_socket(httpd, lfd);
    if (rv != 0) RET(-403, logfd, "evhttp_accept_socket() error (%d).", rv);
    evhttp_set_gencb(httpd, HTTPServer::genericHandler, this);
    threadArg[i].workerId = workerOffset + i;
    threadArg[i].eventBase = base;
    rv = pthread_create(&ths[i], NULL, HTTPServer::dispatch, &threadArg[i]);
    if (rv != 0) RET(-404, logfd, "pthread_create() errori (%d).", rv);
  }
  for (int i = 0; i < workerNo; i++) {
    pthread_join(ths[i], NULL);
  }

  return retVal;
}

void* HTTPServer::dispatch(void *arg) {
  struct thread_arg *threadArg = (struct thread_arg*) arg;
  workerId = threadArg->workerId << workerIdShift;
  cpu_set_t mask;
  if (HTTPServer::cpuCount) {
    int cpu = threadArg->workerId % HTTPServer::cpuCount;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    sched_setaffinity(0, sizeof(mask), &mask);
  }
  counters = (struct counter_item*) calloc(numCounterId,
				 sizeof(struct counter_item));

  if (counters == NULL) {
     retVal = -405;
     RET(NULL, logfd, "Can't allocate memory for counters.");
  }
  
  event_base_dispatch((struct event_base*)threadArg->eventBase);
  return NULL;
}

void HTTPServer::genericHandler(struct evhttp_request *req, void *arg) {
  ((HTTPServer*)arg)->processRequest(req);
}

uint64_t HTTPServer::getMillisec() {
  struct timeval tv;

  // TODO: return value should be checked
  gettimeofday(&tv, NULL);
  return (tv.tv_sec * 1000 - zepoch) + (tv.tv_usec / 1000);
}

void HTTPServer::processRequest(struct evhttp_request *req) {
  uint64_t uniqueId;
  uint64_t curMillisec;
  struct timespec sleepTime, remainingTime;
  struct counter_item* sCounter;
  int rv, counterId;
  const char* uri;

  if (! counters) return;

  uniqueId = 0;

  counterId = 0;
  uri = evhttp_request_uri(req);
  if (uri[1]) {
    int tCounterId = atoi(uri + 1);
    if ((tCounterId > 0) && (tCounterId < numCounterId)) {
      counterId = tCounterId;
    }
  }
  sCounter = &(counters[counterId]);

  curMillisec = getMillisec();

  // time goes backward
  if (sCounter->lastMillisec > curMillisec) {
    fprintf(logfd, "%"PRIu64" - Time goes backward. Waiting for %"PRIu64" msec.\n"
            "Last msec: %"PRIu64", cur msec: %"PRIu64", workerId: %"PRIu64", counter: %d\n",
            curMillisec, (sCounter->lastMillisec - curMillisec),
	    sCounter->lastMillisec, curMillisec, workerId, sCounter->counter);
    sleepTime.tv_sec = sCounter->lastMillisec / 1000;
    sleepTime.tv_nsec = ((sCounter->lastMillisec % 1000) + 1) * 1000000;
    while ((rv = nanosleep(&sleepTime, &remainingTime))) {
      if (rv != EINTR) {
        evhttp_send_reply(req, HTTP_SERVUNAVAIL, "Service unavaible", NULL);
	retVal = -501;
        fprintf(logfd, "nanosleep() error.");
      }
      sleepTime.tv_sec = remainingTime.tv_sec;
      sleepTime.tv_nsec = remainingTime.tv_nsec;
    }
  }

  if (sCounter->lastMillisec == curMillisec) {
    sCounter->counter++;
    if (sCounter->counter > maxCounter) {
      do {
        curMillisec = getMillisec();
      } while (curMillisec <= sCounter->lastMillisec);
    }
  }
  if (sCounter->lastMillisec < curMillisec) {
    sCounter->counter = 0;
    sCounter->lastMillisec = curMillisec;
  }

  uniqueId = (curMillisec << timestampLeftShift) |
             workerId | sCounter->counter;

  struct evbuffer *buf = evbuffer_new();
  if (buf == NULL) {
    evhttp_send_reply(req, HTTP_SERVUNAVAIL, "Service unavaible", NULL);
    retVal = -502;
    fprintf(logfd, "evbuffer_new() error.");
  }
  evbuffer_add_printf(buf, "%"PRIu64, uniqueId);
  evhttp_send_reply(req, HTTP_OK, "OK", buf);
  evbuffer_free(buf);
}

}} // namespace enfinity::eventid

int main(int argc, char** argv) {
  int rv;

  enfinity::eventid::HTTPServer eserver;
  rv = eserver.processParams(argc, argv);
  if (rv) {
    if (rv == -100) return 0;
    return rv;
  }
  rv = eserver.daemonize();
  if (rv) return rv;

  eserver.serv();
}

