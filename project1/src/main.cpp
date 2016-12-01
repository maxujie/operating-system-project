#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>

#define MAX_CUSTOMER_NUM 100
#define MAX_CLERK_NUM 10
#define INF 10000000


// 用于获取顾客进入和离开银行的时间
timeval START_TIME;
int Time() {
  timeval t;
  gettimeofday(&t, NULL);
  return t.tv_sec - START_TIME.tv_sec;
}
void Sleep(int n) { usleep(n * 1e6); }


// 顾客
struct Customer {
  // 输入参数
  int id;
  int enter_time;
  int service_time;  // 顾客所需的服务时间

  int ticket;  // 顾客拿到的号
  int clerk_id;  // 服务顾客的柜台号
  int service_start_time;
  int service_end_time;

  pthread_cond_t cond;  // 每个顾客有一个条件变量，用于在叫号时唤醒这个顾客
  pthread_mutex_t mutex;  // 每个顾客有一个锁，防止有多个柜员同时叫到该顾客

} Customers[MAX_CUSTOMER_NUM];  // 所有顾客存在一个数组中
int customer_num = 0;  // 顾客总数

// 银行柜员有两个状态：等待、正在服务
enum ClerkStatus { CLERK_WAITING, SERVING };
struct Clerk {
  int id;
  ClerkStatus status;
  pthread_cond_t cond;  // 每个柜员有一个条件变量和锁，用于在顾客到来时唤醒柜员
  pthread_mutex_t mutex;
} Clerks[MAX_CLERK_NUM];
int clerk_num = 0;

void InitClerk() {  // 创建柜员时初始化其条件变量和锁
  for (int i = 0; i != clerk_num; ++i) {
    Clerks[i] = {i, CLERK_WAITING};
    pthread_cond_init(&Clerks[i].cond, NULL);
    pthread_mutex_init(&Clerks[i].mutex, NULL);
  }
}

// 取号系统
pthread_mutex_t ticket_mutex;
int ticket = 0;
int GetTicket() {
  int t = 0;
  pthread_mutex_lock(&ticket_mutex);  // 取号时加锁，防止号码重复
  t = ticket;
  ++ticket;
  pthread_mutex_unlock(&ticket_mutex);
  return t;
}

sem_t waiting_customers;  // 信号量，表示正在等待的顾客数量
pthread_mutex_t waiting_customers_mutex;  // 信号量，表示正在等待的顾客数量

pthread_t CustomerThreads[MAX_CUSTOMER_NUM];
pthread_t ClerkThreads[MAX_CLERK_NUM];

// 柜员线程的主函数
void ClerkThread(Clerk *clerk) {
  printf("clerk %d start\n", clerk->id);
  while (true) {  // 主循环
    sem_wait(&waiting_customers);  // P 操作，阻塞到 waiting customer > 0
    pthread_mutex_lock(&waiting_customers_mutex);  // 开始叫号前先拿锁
    int min_uncalled_ticket = INF;
    int call_customer = -1;
    for (int i = 0; i != customer_num; ++i) {  // 检查列表中的所有顾客
      Customer *customer = &Customers[i];
      if (customer->ticket >= 0   // 如果顾客拿到了号
        && customer->clerk_id == -1  // 并且还没有被任何柜员叫过
        && customer->ticket < min_uncalled_ticket) {  // 并且其拿到的号码尽可能小
        min_uncalled_ticket = customer->ticket;
        call_customer = i;
      }
    }
    Customer *customer = &Customers[call_customer];  // 决定叫这个顾客
    printf("clerk %d is calling ticket %d\n", clerk->id, customer->ticket);

    pthread_mutex_lock(&customer->mutex);
    customer->clerk_id = clerk->id;  // 将该顾客的服务柜员设置为自己
    pthread_cond_broadcast(&customer->cond);  // 并且通过该顾客的条件变量唤醒她
    pthread_mutex_unlock(&customer->mutex);

    pthread_mutex_unlock(&waiting_customers_mutex);  // 结束一次叫号过程后释放锁

    do {
      pthread_cond_wait(&clerk->cond, &clerk->mutex);  // 然后等待该顾客表示服务结束
    } while (clerk->status != CLERK_WAITING);
    pthread_mutex_unlock(&clerk->mutex);
  }
}

void CustomerThread(Customer *customer) {
  Sleep(customer->enter_time);  // 在 enter time 时进入银行
  printf("customer %d enter bank at %d\n", customer->id, Time());

  /* enter bank */
  pthread_mutex_lock(&customer->mutex);
  customer->ticket = GetTicket();  // 进入银行后先拿号
  pthread_mutex_unlock(&customer->mutex);

  printf("customer %d get ticket %d\n", customer->id, customer->ticket);

  sem_post(&waiting_customers);  // V 操作，等待的顾客数量增加 1

  /* start waiting */
  printf("customer %d is waiting\n", customer->id);
  do {
    pthread_cond_wait(&customer->cond, &customer->mutex);  // 等待柜员叫自己的号
  } while (customer->clerk_id == -1);
  pthread_mutex_unlock(&customer->mutex);

  /* being served */
  Clerk *clerk = &Clerks[customer->clerk_id];  // 被 clerk 叫到
  printf("customer %d is called by clerk %d\n", customer->id, clerk->id);

  pthread_mutex_lock(&clerk->mutex);
  clerk->status = SERVING;
  pthread_cond_broadcast(&clerk->cond);  // 通知 clerk 开始给自己服务
  pthread_mutex_unlock(&clerk->mutex);

  printf("customer %d being served by clerk %d\n", customer->id, clerk->id);

  customer->service_start_time = Time();  // 记录服务开始的时间
  Sleep(customer->service_time);  // 然后顾客进程阻塞一段时间
  customer->service_end_time = Time();  // 记录服务结束的时间

  pthread_mutex_lock(&clerk->mutex);
  clerk->status = CLERK_WAITING;  // 结束服务后释放柜员
  pthread_mutex_unlock(&clerk->mutex);
  pthread_cond_broadcast(&clerk->cond);  // 并告知柜员结束服务

  printf("customer %d finish and leave bank at %d\n", customer->id, Time());
  pthread_exit(0);  // 最后结束顾客线程
}


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
            Customers[i].service_start_time, Customers[i].service_end_time,
            Customers[i].clerk_id);
  }
}


int main(const int argc, const char *argv[]) {
  if (argc != 4) {
    printf("invalid arguments\nUsage: ./main CLERK_NUM INPUT_FILE OUTPUT_FILE\n");
  }

  // 读取输入
  sscanf(argv[1], "%d", &clerk_num);
  ReadInputFile(argv[2]);
  InitClerk();
  printf("clerk num = %d, customer num = %d\n", clerk_num, customer_num);

  // 初始化锁和信号量
  sem_init(&waiting_customers, 10, 0);
  pthread_mutex_init(&ticket_mutex, NULL);

  // 开始营业
  gettimeofday(&START_TIME, NULL);

  // 启动所有柜员
  for (int i = 0; i != clerk_num; ++i) {
    pthread_create(&ClerkThreads[i], NULL, (void *(*)(void *))ClerkThread,
                   &Clerks[i]);
  }

  // 启动所有顾客
  for (int i = 0; i != customer_num; ++i) {
    pthread_create(&CustomerThreads[i], NULL, (void *(*)(void *))CustomerThread,
                   &Customers[i]);
  }

  Sleep(20);  // 等待一段时间（需要大于最后一个顾客离开的时间）
  WriteOutputFile(argv[3]);  // 输出结果
}
