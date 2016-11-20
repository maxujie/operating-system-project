#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#define MAX_CUSTOMER_NUM 100
#define MAX_CLERK_NUM 10
#define INF 10000000

enum ClerkStatus { CLERK_WAITING, SERVING };

clock_t START_TIME;

int Time() { return int(double(clock() - START_TIME) / 1000 + 0.5); }
void Sleep(int n) { usleep(n * 1e6); }

struct Customer {
  int id;
  int enter_time;
  int service_time;

  int ticket;
  int clerk_id;
  int service_start_time;
  int service_done_time;

  pthread_cond_t cond;
  pthread_mutex_t mutex;

  void Print() {
    printf("Customer %d: (%d %d %d %d %d %d)\n", id, enter_time, service_time,
           ticket, clerk_id, service_start_time, service_done_time);
  }
} Customers[MAX_CUSTOMER_NUM];
int customer_num = 0;

struct Clerk {
  int id;
  ClerkStatus status;
  pthread_cond_t cond;
  pthread_mutex_t mutex;
} Clerks[MAX_CLERK_NUM];
int clerk_num = 0;

void ReadInputFile(const char *filepath) {
  FILE *fin;
  if (!(fin = fopen(filepath, "r"))) {
    printf("can't read '%s'\n", filepath);
    exit(0);
  }

  int id, etime, stime;
  while (fscanf(fin, "%d %d %d", &id, &etime, &stime) != EOF) {
    Customers[customer_num] = {id, etime, stime, -1, -1, -1, -1};
    pthread_cond_init(&Customers[customer_num].cond, NULL);
    pthread_mutex_init(&Customers[customer_num].mutex, NULL);
    ++customer_num;
  }
  fclose(fin);
}

void WriteOutputFile(const char *filepath) {
  FILE *fout;
  if (!(fout = fopen(filepath, "w"))) {
    printf("can't write %s\n", filepath);
    exit(0);
  }
  for (int i = 0; i != customer_num; ++i) {
    fprintf(fout, "%d %d %d %d %d\n", Customers[i].id, Customers[i].enter_time,
            Customers[i].service_start_time, Customers[i].service_done_time,
            Customers[i].clerk_id);
  }
}

void InitClerk() {
  for (int i = 0; i != clerk_num; ++i) {
    Clerks[i] = {i, CLERK_WAITING};
    pthread_cond_init(&Clerks[i].cond, NULL);
    pthread_mutex_init(&Clerks[i].mutex, NULL);
  }
}

pthread_mutex_t ticket_mutex;
int ticket = 0;
int GetTicket() {
  int t = 0;
  pthread_mutex_lock(&ticket_mutex);
  t = ticket;
  ++ticket;
  pthread_mutex_unlock(&ticket_mutex);
  return t;
}

sem_t waiting_customers;
pthread_mutex_t waiting_customers_mutex;

void ClerkThread(Clerk *clerk) {
  printf("clerk %d start\n", clerk->id);
  while (true) {
    sem_wait(&waiting_customers);
    pthread_mutex_lock(&waiting_customers_mutex);
    int min_uncalled_ticket = INF;
    int call_customer = -1;
    for (int i = 0; i != customer_num; ++i) {
      Customer *customer = &Customers[i];
      if (customer->ticket >= 0 && customer->clerk_id == -1 &&
          customer->ticket < min_uncalled_ticket) {
        min_uncalled_ticket = customer->ticket;
        call_customer = i;
      }
    }
    Customer *customer = &Customers[call_customer];
    printf("clerk %d serve customer %d\n", clerk->id, customer->id);
    pthread_mutex_lock(&customer->mutex);
    customer->clerk_id = clerk->id;
    pthread_cond_broadcast(&customer->cond);
    pthread_mutex_unlock(&customer->mutex);
    pthread_mutex_unlock(&waiting_customers_mutex);

    do {
      pthread_cond_wait(&clerk->cond, &clerk->mutex);
    } while (clerk->status != CLERK_WAITING);
    pthread_mutex_unlock(&clerk->mutex);
  }
}

void CustomerThread(Customer *customer) {
  /* customer start */
  Sleep(customer->enter_time);

  printf("Customer %d enter bank at %d\n", customer->id, Time());

  /* enter bank */
  pthread_mutex_lock(&customer->mutex);
  customer->ticket = GetTicket();
  pthread_mutex_unlock(&customer->mutex);

  printf("Customer %d get ticket %d\n", customer->id, customer->ticket);

  sem_post(&waiting_customers);
  /* start waiting */
  printf("Customer %d waiting\n", customer->id);
  do {
    pthread_cond_wait(&customer->cond, &customer->mutex);
  } while (customer->clerk_id == -1);
  pthread_mutex_unlock(&customer->mutex);
  printf("Customer %d waiting done, clerk_id = %d\n", customer->id,
         customer->clerk_id);

  /* being served */
  Clerk *clerk = &Clerks[customer->clerk_id];

  pthread_mutex_lock(&clerk->mutex);
  clerk->status = SERVING;
  pthread_cond_broadcast(&clerk->cond);
  pthread_mutex_unlock(&clerk->mutex);

  printf("customer %d being served by clerk %d\n", customer->id, clerk->id);

  customer->service_start_time = Time();
  Sleep(customer->service_time);
  customer->service_done_time = Time();

  pthread_mutex_lock(&clerk->mutex);
  clerk->status = CLERK_WAITING;
  pthread_cond_broadcast(&clerk->cond);

  pthread_mutex_unlock(&clerk->mutex);

  printf("customer %d exit bank\n", customer->id);
  pthread_exit(0);
}

pthread_t CustomerThreads[MAX_CUSTOMER_NUM];
pthread_t ClerkThreads[MAX_CLERK_NUM];

int main(const int argc, const char *argv[]) {
  if (argc != 4) {
    printf("invalid arguments\nUsage: bank CLERK_NUM INPUT_FILE OUTPUT_FILE\n");
  }

  sscanf(argv[1], "%d", &clerk_num);
  ReadInputFile(argv[2]);
  InitClerk();

  printf("clerk num = %d, customer num = %d\n", clerk_num, customer_num);

  /* init*/
  sem_init(&waiting_customers, 10, 0);
  pthread_mutex_init(&ticket_mutex, NULL);

  /* bank start */
  START_TIME = clock();

  for (int i = 0; i != clerk_num; ++i) {
    pthread_create(&ClerkThreads[i], NULL, (void *(*)(void *))ClerkThread,
                   &Clerks[i]);
  }

  for (int i = 0; i != customer_num; ++i) {
    pthread_create(&CustomerThreads[i], NULL, (void *(*)(void *))CustomerThread,
                   &Customers[i]);
  }

  Sleep(20);
  WriteOutputFile(argv[3]);
}
