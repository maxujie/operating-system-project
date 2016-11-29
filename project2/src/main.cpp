#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#define NUM_SIZE 10000
#define STACK_SIZE 1000
#define MAX_THREADS_SIZE 5
#define BLOCK_SIZE 1000

int numbers[NUM_SIZE];

pthread_t threads[MAX_THREADS_SIZE];
int working_threads = 0;

// a simple queue implements with 2 stacks
int queue_size = 0;
int in_stack_st[STACK_SIZE];
int in_stack_ed[STACK_SIZE];
int in_stack_size = 0;
int out_stack_st[STACK_SIZE];
int out_stack_ed[STACK_SIZE];
int out_stack_size = 0;
pthread_mutex_t queue_mutex;
pthread_cond_t queue_cond;

void Push(int st, int ed) {
  in_stack_st[in_stack_size] = st;
  in_stack_ed[in_stack_size] = ed;
  ++in_stack_size;
  ++queue_size;
  printf("queue size = %d, push(%d, %d)\n", queue_size, st, ed);
}

void Pop(int &st, int &ed) {
  if (out_stack_size == 0) {
    for (int i = 0; i != in_stack_size; ++i) {
      out_stack_st[i] = in_stack_st[in_stack_size - i - 1];
      out_stack_ed[i] = in_stack_ed[in_stack_size - i - 1];
    }
    out_stack_size = in_stack_size;
    in_stack_size = 0;
  }
  --queue_size;
  --out_stack_size;
  st = out_stack_st[out_stack_size];
  ed = out_stack_ed[out_stack_size];
  printf("queue size = %d, pop(%d, %d)\n", queue_size, st, ed);
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

void CalcThread() {
  int st, ed;
  while (true) {
    while (true) { // get next task
      if (queue_size == 0 && working_threads > 0) {
        pthread_cond_wait(&queue_cond, &queue_mutex);
      } else {
        pthread_mutex_lock(&queue_mutex);
      }

      if (queue_size > 0) {
        Pop(st, ed);
        ++working_threads;
        pthread_mutex_unlock(&queue_mutex);
        break;
      } else if (working_threads == 0) {
        pthread_mutex_unlock(&queue_mutex);
        return;
      }
      pthread_mutex_unlock(&queue_mutex);
    }
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

      if (ed - (j + 1) > 1) {
        pthread_mutex_lock(&queue_mutex);
        Push(j + 1, ed);
        pthread_mutex_unlock(&queue_mutex);
        pthread_cond_broadcast(&queue_cond);
      }
      ed = j;
    }
    pthread_mutex_lock(&queue_mutex);
    --working_threads;
    pthread_mutex_unlock(&queue_mutex);
    QSort(st, ed);
  }
}

int main(int argc, char *argv[]) {
  if (argc != 3) {
    printf("error: invalid arguments.\neg: ./main in.txt out.txt\n");
    return -1;
  }

  ReadFile(argv[1]);

  // initialize
  pthread_mutex_init(&queue_mutex, NULL);
  pthread_cond_init(&queue_cond, NULL);

  // create threads
  for (int i = 0; i != MAX_THREADS_SIZE; ++i) {
    pthread_create(&threads[i], NULL, (void *(*)(void *))CalcThread, NULL);
  }

  Push(0, NUM_SIZE);
  pthread_cond_broadcast(&queue_cond);

  // waiting threads done
  for (int i = 0; i != MAX_THREADS_SIZE; ++i) {
    pthread_join(threads[i], NULL);
  }

  // output
  WriteFile(argv[2]);
}
