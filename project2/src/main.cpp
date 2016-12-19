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

struct SortParam {  // 创建计算线程时的传入参数
  int st;
  int ed;
};

SortParam task_queue[QUEUE_SIZE];  // 任务队列
int queue_size = 0;
int queue_tail = 0;
int queue_head = 0;

void Push(int st, int ed) {  // 向队列中添加任务
  task_queue[queue_tail].st = st;
  task_queue[queue_tail].ed = ed;
  ++queue_tail;
  ++queue_size;
}
SortParam *Pop() {  // 从队列中取出任务
  SortParam *ret = &task_queue[queue_head++];
  --queue_size;
  return ret;
}

pthread_t thread_manager;  // 管理线程
pthread_mutex_t mutex;  // 访问任务队列时需要加锁
pthread_cond_t thread_manager_cond;  // 通过条件变量唤醒管理线程
int working_threads = 0;  // 正在运行的计算线程计数器

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

void InsertSort(int st, int ed) {  // 在需要排序的数据量很小时采用插入排序进行加速
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

void CalcThread(void *argv) {  // 计算线程
  SortParam *param = (SortParam *)argv;
  int st = param->st;  // 快速排序起始位置
  int ed = param->ed;  // 快速排序结束位置
  if (ed - st < BLOCK_SIZE) {  // 如果区间很小的话，不采用快排，而是直接进行插入排序
    InsertSort(st, ed);
  } else {  // 否则需要快排
    // 为避免快排效率恶化，随机取一个元素作为中值
    int rand_ind = st + rand() % (ed - st);
    int mid = numbers[rand_ind];
    numbers[rand_ind] = numbers[ed - 1];

    // 快排的主过程，将小于 mid 的元素移动到区间的前一半
    int i = 0, j = 0;
    while (i != ed - 1) {
      if (numbers[i] < mid) {
        int t = numbers[j];
        numbers[j++] = numbers[i];
        numbers[i] = t;
      }
      ++i;
    }

    // 划分结束
    numbers[ed - 1] = numbers[j];
    numbers[j] = mid;

    // 然后将子任务加入任务队列
    int st1 = st, ed1 = j;
    int st2 = j + 1, ed2 = ed;
    pthread_mutex_lock(&mutex);
    Push(st1, ed1);  // 任务1 (左半边)：[起始点, 中间点)
    Push(st2, ed2);  // 任务2 (右半边)：[中间点+1, 终点)
    pthread_mutex_unlock(&mutex);
  }
  pthread_mutex_lock(&mutex);
  --working_threads;  // 在结束计算线程的时候将当前工作线程计数器减 1
  pthread_mutex_unlock(&mutex);
  pthread_cond_broadcast(&thread_manager_cond);  // 最后告知管理线程检查任务队列
  // 完成一个子任务的计算后线程自然退出
}

void ThreadManager() {  // 管理线程主函数
  while (true) {
    pthread_mutex_lock(&mutex);  // 为了检查任务队列和工作线程数，先取得锁
    if (queue_size > 0  // 如果任务队列非空
      && working_threads < MAX_THREADS_NUM) {  // 并且正在工作的线程数未达到上限
      ++working_threads;
      pthread_t thread;
      SortParam *task = Pop();  // 则从任务队列中取出一个任务
      printf("new task (%d, %d), working_threads = %d, queue_size = %d\n",
             task->st, task->ed, working_threads, queue_size);
      // 然后创建一个新的线程处理这个任务
      pthread_create(&thread, NULL, (void *(*)(void *))CalcThread, task);
      pthread_mutex_unlock(&mutex);
    } else if (queue_size == 0 && working_threads == 0) {
      // 否则，如果 (任务队列空 && 没有正在工作的线程)
      // 这种情况说明计算已经结束，不可能新的子任务再产生了
      pthread_mutex_unlock(&mutex);
      return;  // 管理线程退出，向主函数表明计算已经结束
    } else {
      // 否则，（任务队列空 || 工作线程数达到上限 || 有正在工作的线程）
      pthread_mutex_unlock(&mutex);
      // 阻塞地等待一个工作线程退出后唤醒管理线程
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
