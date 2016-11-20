#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#define NUM_SIZE 1000000
#define STACK_SIZE 1000
#define MAX_THREADS_SIZE 20
#define BLOCK_SIZE 1000

int numbers[NUM_SIZE];

pthread_t threads[MAX_THREADS_SIZE];
int threads_status[MAX_THREADS_SIZE];
int threads_size = 0;
pthread_mutex_t threads_mutex;
pthread_cond_t threads_cond;

int stack_st[STACK_SIZE];
int stack_ed[STACK_SIZE];
int stack_size = 0;
pthread_mutex_t stack_mutex;

void StackPush(int st, int ed) {
  pthread_mutex_lock(&stack_mutex);
  stack_st[stack_size] = st;
  stack_ed[stack_size] = ed;
  ++stack_size;
  pthread_mutex_unlock(&stack_mutex);
}

void StackPop(int &st, int &ed) {
  pthread_mutex_lock(&stack_mutex);
  --stack_size;
  st = stack_st[stack_size];
  ed = stack_ed[stack_size];
  pthread_mutex_unlock(&stack_mutex);
}

void ReadFile(char *filepath) {
  FILE *fin;
  if (!(fin = fopen(filepath, "r"))) {
    printf("can't open file %s\n", filepath);
    exit(0);
  }

  for (int i = 0; i != NUM_SIZE; ++i) {
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

  for (int i = 0; i != NUM_SIZE; ++i) {
    fprintf(fout, "%d\n", numbers[i]);
  }

  fclose(fout);
}

void QSort(int st, int ed) {
  while (ed - st) {
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
    QSort(j + 1, ed);
    ed = j;
  }
}

void CalcThread(int st, int ed, int tid) {
  while (ed - st > BLOCK_SIZE) {
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
    StackPush(j + 1, ed);
    ed = j;
  }
  QSort(st, ed);

  pthread_mutex_lock(&threads_mutex);
  --threads_size;
  threads_status[tid] = 0;
  pthread_mutex_unlock(&threads_mutex);

  pthread_cond_broadcast(&threads_cond);
  pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
  while (threads_size > 0 || stack_size > 0) {
    if (threads_size < MAX_THREADS_SIZE && stack_size > 0) {
      pthread_mutex_lock(&threads_mutex);
      for (int i = 0; i != MAX_THREADS_SIZE; ++i) {
        if (threads_status[i] == 0) {
          threads_status[i] = 1;
          pthread_create(&threads[i], NULL, (void *)CalcThread, NULL);
        }
      }
      pthread_mutex_unlock(&threads_mutex);
    } else if (stack_size > 0) {
      while (threads_size >= MAX_THREADS_SIZE) {
        pthread_cond_wait(&threads_cond, &threads_mutex);
      }
      ++threads_size;
      for (int i = 0; i != MAX_THREADS_SIZE; ++i) {
        if (threads_status[i] == 0) {
          threads_status[i] = 1;
          pthread_create(&threads[i], NULL, (void *)CalcThread, NULL);
        }
      }
      pthread_mutex_unlock(&threads_mutex);
    }
  }
}
