#include <stdio.h>
#include <mpi.h>
#include <pthread.h>
#include <iostream>
#include <algorithm> 
#include <vector>
#include <unistd.h>
#include <time.h>
#include <limits.h>
//tags
#define REQ 100 //request
#define REP 110  //reply
#define CONF 120  //confirm
#define K 20
#define D 40

using namespace std;

typedef struct queue_str{
	int lamport;
	int proc_nr;
}qs;
int lamport=0;
int size;
pthread_mutex_t	lamport_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t	ac_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t	queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t	array_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t	send_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t	myrank_mutex = PTHREAD_MUTEX_INITIALIZER;

vector<queue_str> queue_proc;

int dearray[K]; //liczba zatrudnionych debili w Kampanii
int accept=0;
int wait_cs=0;
int myrank;

bool my_compare(queue_str str_A,  queue_str str_B){
	return str_A.lamport < str_B.lamport;
	
}
void print_queue(){
    pthread_mutex_lock(&queue_mutex);
    
    for(unsigned int i = 0; i < queue_proc.size(); i++ )
    {
        
        cout << "Kolejka Lamport: " << queue_proc[ i ].lamport << " nr proc:  " << queue_proc[ i ].proc_nr  << endl;
    }
    pthread_mutex_unlock(&queue_mutex);
}

int mymin()
{
     int min=INT_MAX;
     int index=0;
     for(int i = 0; i < K; i++)
     {
           if(dearray[i] < min){
                min = dearray[i];
                index=i;
        }
     }
     return index;
}

void increment_lamport(int l){
    pthread_mutex_lock(&lamport_mutex);
    lamport=max(l,lamport)+1;
    pthread_mutex_unlock(&lamport_mutex);
}

void init_array(){
        for(int j=0;j<K;j++){
            dearray[j]=0;
    }
}

void *receive_loop(void * arg) {
    int msg[2]; //msg[0] - timestamp msg[1] - id Kampanii 
    int msgS[2]; 
    while(1){
        
        MPI_Status status;
        
        MPI_Recv(msg, 2, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        
        switch(status.MPI_TAG){
            
            case REQ:
                
                increment_lamport(msg[0]);
                msgS[0]=lamport;
                msgS[1]=50;
                
                pthread_mutex_lock(&myrank_mutex);
                printf("Lamport: %d . Odebrałem REQ od %d Moj nr to %d \n",msg[0], status.MPI_SOURCE, myrank );
                pthread_mutex_unlock(&myrank_mutex);
                
                queue_str temp_queue;
                temp_queue.lamport = msg[0];
                temp_queue.proc_nr=status.MPI_SOURCE;
                
                pthread_mutex_lock(&queue_mutex);
                queue_proc.push_back(temp_queue);
                sort(queue_proc.begin(), queue_proc.end(), my_compare);
                pthread_mutex_unlock(&queue_mutex);
                
                pthread_mutex_lock(&myrank_mutex);
                printf("Lamport: %d . Wpisałem do kolejki:%d Moj nr: %d\n",lamport , status.MPI_SOURCE,myrank);
                pthread_mutex_unlock(&myrank_mutex);
                
                print_queue();
                
                pthread_mutex_lock(&send_mutex);
                MPI_Send(&msgS, 2, MPI_INT, status.MPI_SOURCE, REP, MPI_COMM_WORLD);
                pthread_mutex_unlock(&send_mutex);
                
                pthread_mutex_lock(&myrank_mutex);
                printf("Lamport: %d . Odpisałem ok: %d Moj nr: %d  \n",lamport,status.MPI_SOURCE,myrank);
                pthread_mutex_unlock(&myrank_mutex);
                
                break;
                
            case REP:
                
                if(lamport < msg[0]){
                    pthread_mutex_lock(&ac_mutex);
                    accept+=1;
                    pthread_mutex_unlock(&ac_mutex);
                    }
                    
                pthread_mutex_lock(&myrank_mutex);
                printf("Lamport: %d. My Lamport: %d.Odebrałem REP od %d Moj nr: %d \n",msg[0],lamport, status.MPI_SOURCE,myrank);
                pthread_mutex_unlock(&myrank_mutex);
                
                break;
                
            case CONF:
                
                pthread_mutex_lock(&array_mutex);
                dearray[msg[1]]+=1;
                pthread_mutex_unlock(&array_mutex);
                
                pthread_mutex_lock(&queue_mutex);
                queue_proc.erase(queue_proc.begin());
                pthread_mutex_unlock(&queue_mutex);
                
                pthread_mutex_lock(&myrank_mutex);
                printf("Lamport: %d . Odebrałem CONF od %d Moj nr: %d . \n",msg[0], status.MPI_SOURCE,myrank);
                pthread_mutex_unlock(&myrank_mutex);
                break;
            default:
                break;
}
}
}
bool wer_accept(){
        pthread_mutex_lock(&ac_mutex);
        if(accept<size){
            pthread_mutex_unlock(&ac_mutex);
            return false;}
            else{
                pthread_mutex_unlock(&ac_mutex);
                return true;}
}

bool wer_queue(){
    
        pthread_mutex_lock(&queue_mutex);
        pthread_mutex_lock(&myrank_mutex);
        
        if(queue_proc[0].proc_nr!=myrank){
            pthread_mutex_unlock(&queue_mutex);
            pthread_mutex_unlock(&myrank_mutex);
            return false;
        }
            else{
                pthread_mutex_unlock(&queue_mutex);
                return true;
        }
}



int main(int argc, char **argv)
{
    int provided=0;
    MPI_Init_thread(&argc, &argv,MPI_THREAD_MULTIPLE, &provided);
    if (provided!=MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "Brak wystarczającego wsparcia dla wątków - wychodzę!\n");
        MPI_Finalize();
        exit(-1);
    }
 
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    
    pthread_t receive_thread;
    pthread_create(&receive_thread, NULL, receive_loop, 0);
    
    int msg[2];
    
    while(1){
        //wysyłam req najpierw
        if(wait_cs==0){
            
            increment_lamport(lamport);
            msg[0]=lamport;
            msg[1]=50;
            pthread_mutex_lock(&send_mutex);
            for (int i = 0; i < size; i++)
            {
                MPI_Send(&msg, 2, MPI_INT, i, REQ, MPI_COMM_WORLD);
                pthread_mutex_lock(&myrank_mutex);
                printf("Lamport: %d . Wysłałem REQ do %d .  Moj nr:%d \n",lamport,i,myrank);
                pthread_mutex_unlock(&myrank_mutex);
            }
            pthread_mutex_unlock(&send_mutex);
            wait_cs=1;
            }
            
        else if(wer_accept() && wer_queue()){
            //dopoki wszyscy nie odesla ok 
            int index=mymin();
            
            increment_lamport(lamport);
            msg[0]=lamport;
            msg[1]=index;
            
            pthread_mutex_lock(&send_mutex);
            for (int i = 0; i < size; i++)
            {
                MPI_Send(&msg, 2, MPI_INT, i, CONF, MPI_COMM_WORLD);
                
                pthread_mutex_lock(&myrank_mutex);
                printf("Lamport: %d . Wysłałem CONF.  Moj nr:%d \n",lamport,myrank);
                pthread_mutex_unlock(&myrank_mutex);
            }
            pthread_mutex_unlock(&send_mutex);
            
            pthread_mutex_lock(&ac_mutex);
            accept=0;
            pthread_mutex_unlock(&ac_mutex);
            
            pthread_mutex_lock(&myrank_mutex);
            printf("Lamport: %d . ZATRUDNILEM W FIRMIE NR  %d. Teraz się drzemne. Moj nr: %d \n",lamport, index,myrank);
            pthread_mutex_unlock(&myrank_mutex);
            //sleep(3);
            wait_cs=0;
        }
    }
    MPI_Finalize();
    return 0;
}
