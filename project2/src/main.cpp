#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <random>
#define DATA_NUM 1000000
#define QUEUE_SIZE 100000
#define BLOCK_SIZE 1000
#define MAX_THREADS_NUM 20

// Data

int numbers[DATA_NUM];

// Thread Manager

struct SortParam {
  int st;
  int ed;
};

SortParam task_queue[QUEUE_SIZE];
int queue_size = 0;
int queue_tail = 0;
int queue_head = 0;

void Push(int st, int ed) {
  task_queue[queue_tail].st = st;
  task_queue[queue_tail].ed = ed;
  ++queue_tail;
  ++queue_size;
}
SortParam *Pop() {
  SortParam *ret = &task_queue[queue_head++];
  --queue_size;
  return ret;
}

pthread_t thread_manager;
pthread_mutex_t mutex;
pthread_cond_t thread_manager_cond;
int working_threads = 0;

// Functions For Loading Data

void ReadFile(char *filepath) {
  FILE *fin;
  if (!(fin = fopen(filepath, "r"))) {
    printf("can't open file %s\n", filepath);
    exit(0);
  }

  for (int i = 0; i != DATA_NUM; ++i) {
    fscanf(fin, "%d", &numbers[i]);
  }

  fclose(fin);
}

void WriteFile(char *filepath) {
  FILE *fout;
  if (!(fout = fopen(filepath, "w"))) {
    printf("can't open file %s\n", filepath);
    exit(0);
  }

  for (int i = 0; i != DATA_NUM; ++i) {
    fprintf(fout, "%d\n", numbers[i]);
  }

  fclose(fout);
}

void QSort(int st, int ed) {
  printf("qsort (%d, %d)\n", st, ed);
  while (ed - st > 1) {
    int mid = numbers[ed - 1];
    int i = 0, j = 0;
    while (i != ed - 1) {
      if (numbers[i] < mid) {
        int t = numbers[j];
        numbers[j++] = numbers[i];
        numbers[i] = t;
      }
      ++i;
    }
    numbers[ed - 1] = numbers[j];
    numbers[j] = mid;
    if (ed > j + 2) {
      QSort(j + 1, ed);
    }
    ed = j;
  }
}

void InsertSort(int st, int ed) {
  for (int i = 1; i != ed; ++i) {
    int j = i;
    while (j > 0 && numbers[j] < numbers[j - 1]) {
      int t = numbers[j];
      numbers[j] = numbers[j - 1];
      numbers[j - 1] = t;
      --j;
    }
  }
}

void CalcThread(void *argv) {
  SortParam *param = (SortParam *)argv;
  int st = param->st;
  int ed = param->ed;
  if (ed - st < BLOCK_SIZE) { // do not need quick sort
    InsertSort(st, ed);
  } else if (ed - st > BLOCK_SIZE) {
    // random pick an element as middle value
    int rand_ind = st + rand() % (ed - st);
    int mid = numbers[rand_ind];
    numbers[rand_ind] = numbers[ed - 1];

    int i = 0, j = 0;
    while (i != ed - 1) {
      if (numbers[i] < mid) {
        int t = numbers[j];
        numbers[j++] = numbers[i];
        numbers[i] = t;
      }
      ++i;
    }
    numbers[ed - 1] = numbers[j];
    numbers[j] = mid;

    int st1 = st, ed1 = j;
    int st2 = j + 1, ed2 = ed;
    pthread_mutex_lock(&mutex);
    Push(st1, ed1);
    Push(st2, ed2);
    pthread_mutex_unlock(&mutex);
  }
  pthread_mutex_lock(&mutex);
  --working_threads;
  pthread_mutex_unlock(&mutex);
  pthread_cond_broadcast(&thread_manager_cond);
}

void ThreadManager() {
  while (true) {
    pthread_mutex_lock(&mutex); // check status
    if (queue_size > 0 && working_threads < MAX_THREADS_NUM) {
      ++working_threads;
      pthread_t thread;
      SortParam *task = Pop();
      printf("new task (%d, %d), working_threads = %d, queue_size = %d\n",
             task->st, task->ed, working_threads, queue_size);
      pthread_create(&thread, NULL, (void *(*)(void *))CalcThread, task);
      pthread_mutex_unlock(&mutex);
    } else if (queue_size == 0 && working_threads == 0) {
      pthread_mutex_unlock(&mutex);
      return;
    } else {
      pthread_mutex_unlock(&mutex);
      pthread_cond_wait(&thread_manager_cond, &mutex);
      pthread_mutex_unlock(&mutex);
    }
  }
}

int main(int argc, char *argv[]) {
  if (argc != 3) {
    printf("error: invalid arguments.\neg: ./main in.txt out.txt\n");
    return -1;
  }

  ReadFile(argv[1]);

  // initialize

  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&thread_manager_cond, NULL);
  Push(0, DATA_NUM);

  // create threads
  pthread_create(&thread_manager, NULL, (void *(*)(void *))ThreadManager, NULL);

  // waiting threads done
  pthread_join(thread_manager, NULL);

  // output
  WriteFile(argv[2]);
}
