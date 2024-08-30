#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }


int HT_CreateFile(char *fileName,  int buckets){
	printf("**CreateHtFile function**\n");
	int fd1;
	void* data;
	HT_info* file = malloc(sizeof(HT_info));
	BF_ErrorCode code;
	
	BF_Block *block;
	BF_Block_Init(&block);
	
	printf("\nCREATING NEW FILE \n");
	code = BF_CreateFile(fileName);
	if (code != BF_OK)
		BF_PrintError(code);
	
	printf("\nOPENING NEW FILE \n");
	code = BF_OpenFile(fileName,&fd1);
	
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
	
	for(int i=0; i <= buckets; i++)
	{
		code = BF_AllocateBlock(fd1, block);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
			
		if(i==0)
		{
			HT_info* file = malloc(sizeof(HT_info));
			data = BF_Block_GetData(block);
			file->buckets = buckets;
			file->max_num_of_records_in_block = (int)((BF_BLOCK_SIZE-sizeof(HT_block_info))/ sizeof(Record));
			printf("max num of records %d", file->max_num_of_records_in_block);
			printf("Last block bucket array: \n");
			for(int j=1; j<=buckets; j++)
			{
				file->last_block_bucket_num[j] = j;
				printf("%d ",file->last_block_bucket_num[j]); // we dont use position 0
				
			}
			printf("\n");
			file->method = 2;
			memcpy(data,file,sizeof(HT_info));
			BF_Block_SetDirty(block);
			code = BF_UnpinBlock(block);
			if (code != BF_OK){
				BF_PrintError(code);
				return -1;}
			free(file);	
		}	
		else
		{
			HT_block_info* file1= malloc(sizeof(HT_block_info));
			data = BF_Block_GetData(block);	
			file1->num_of_records = 0 ;
			file1->next_bucket_block = -1;
			data = data + (BF_BLOCK_SIZE-sizeof(HT_block_info));
			memcpy(data,file1,sizeof(HT_block_info));
			BF_Block_SetDirty(block);
			code = BF_UnpinBlock(block);
			if (code != BF_OK){
				BF_PrintError(code);
				return -1;}
			free(file1);
		}
			
	}
	BF_Block_Destroy(&block);
	code = BF_CloseFile(fd1);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}	
	
    return 0;
}

HT_info* HT_OpenFile(char *fileName){
	printf("\n**OpenHtFile function**\n");
	BF_ErrorCode code;
	int fd1;
	void* data;
	BF_Block *block;
	BF_Block_Init(&block);
	HT_info* file = malloc(sizeof(HT_info));
	
	printf("\nOPENING FILE\n");
	code = BF_OpenFile(fileName ,&fd1);
	if (code != BF_OK){
		BF_PrintError(code);
		return NULL;}
		
		printf("\n%d",fd1);
	
	printf("\nGETTING BLOCK 0\n");
 	code = BF_GetBlock(fd1,0,block);
	if (code != BF_OK){
		BF_PrintError(code);
		return NULL;}
		
	data = BF_Block_GetData(block);
	memcpy(file, data, sizeof(HT_info));

	if (file->method != 2){
		printf ("%s \n", "NoHashFile");
		return NULL;}

		
	file->fileDesc = fd1;
	code = BF_UnpinBlock(block);
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return NULL;}
    return file;
}


int HT_CloseFile( HT_info* HT_info ){
	printf("\n**CloseFile function**\n");
	BF_ErrorCode code;
	void* data;
	BF_Block *block;
	BF_Block_Init(&block);
	
	
	code = BF_GetBlock(HT_info->fileDesc,0,block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	data = BF_Block_GetData(block);
	memcpy(data, HT_info, sizeof(HT_info));
	
	BF_Block_SetDirty(block);
	code = BF_UnpinBlock(block);
	
	printf("CLOSING FILE \n");
	code = BF_CloseFile(HT_info->fileDesc);
	if(code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
	free(HT_info); 
	
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	printf("Getting out of HP_CloseFIle... \n");
    return 0;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
	printf("\n**Ht_InsertEntry function**\n");
	BF_ErrorCode code;
	int blocks_num,bl,BucketNum;
	void* data;
	void* data1;
	void* data2;
	BF_Block *block;
	BF_Block_Init(&block);
	
	BucketNum = (record.id % ht_info->buckets) + 1; //Hash function
	
	code = BF_GetBlock(ht_info->fileDesc,ht_info->last_block_bucket_num[BucketNum],block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	data = BF_Block_GetData(block);
	HT_block_info* file1 = malloc(sizeof(HT_block_info));
	data1 = data + (BF_BLOCK_SIZE-sizeof(HT_block_info));
	memcpy(file1,data1,sizeof(HT_block_info));
	printf("records into block%d: %d\n",ht_info->last_block_bucket_num[BucketNum] ,file1->num_of_records);
	if(file1->num_of_records < ht_info->max_num_of_records_in_block)
	{
		for(int i = 1; i<= file1->num_of_records; i++)
		    data+=sizeof(Record);
		
		memcpy(data,&record,sizeof(Record));
		file1->num_of_records++;
		memcpy(data1,file1,sizeof(HT_block_info));
		bl = ht_info->last_block_bucket_num[BucketNum];
		printf("\n bl %d" , bl);
	}
	else // if last block is full, create another block
	{
		BF_Block *block1;
		BF_Block_Init(&block1);
		code = BF_AllocateBlock(ht_info->fileDesc, block1);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}	
		
		//initiation of new block in bucket	
		data2 = BF_Block_GetData(block1);
		memcpy(data2,&record,sizeof(Record));
		HT_block_info* file2 = malloc(sizeof(HT_block_info));
		file2->num_of_records = 1;
		file2->next_bucket_block = -1;
		data2 = data2 + (BF_BLOCK_SIZE-sizeof(HT_block_info));
		memcpy(data2,file2,sizeof(HT_block_info));
		
		//change of last block in bucket
		code = BF_GetBlockCounter(ht_info->fileDesc, &blocks_num);
		file1->next_bucket_block = blocks_num - 1;
		printf("Next bucktet block %d \n",file1->next_bucket_block);
		memcpy(data1,file1,sizeof(HT_block_info));
		
		//change of file's data
		ht_info->last_block_bucket_num[BucketNum] = blocks_num-1;
		printf("New ht_info array: \n");
		for(int j=1; j<=ht_info->buckets; j++)
			{
				printf("%d ",ht_info->last_block_bucket_num[j]);
				
			}
		printf("\n");
		
		free(file2);
		bl = blocks_num-1;		
		BF_Block_SetDirty(block1);
		code = BF_UnpinBlock(block1);
		BF_Block_Destroy(&block1);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
	}	
	
	free(file1);
	BF_Block_SetDirty(block);
	code = BF_UnpinBlock(block);
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
    return bl;
}

int HT_GetAllEntries(HT_info* ht_info, void *value ){
	printf("\nGetAllEntries");
	int temp,BucketNum,count = 0;
	int* id = value;
	void* data;
	void* data2;
	BF_Block *block;
	BF_Block_Init(&block);
	BF_ErrorCode code;
	BucketNum = ((*id) % ht_info->buckets) + 1;
	printf("\nSearching for id %d\n",*id);
	printf("\n BucketNum(first block): %d\n", BucketNum);
	Record record;
	HT_block_info* file1 = malloc(sizeof(HT_block_info));
	
	do{
		code = BF_GetBlock(ht_info->fileDesc,BucketNum,block);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
			
		data  = BF_Block_GetData(block);
		data2 = data +	(BF_BLOCK_SIZE-sizeof(HT_block_info));
		
		memcpy(file1,data2,sizeof(HT_block_info));
		printf("Searching in block%d\n",BucketNum);
		for(int i=1; i <= file1->num_of_records; i++)
		{
			memcpy(&record,data,sizeof(record));
			//printf("\n record.id %d : ",record.id);
			if(record.id == (*id)){
				printf("FOUND -> ");
				printRecord(record);}	
			data+=sizeof(Record);
		}	
		count++;
		code = BF_UnpinBlock(block);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
		BucketNum = file1->next_bucket_block;
	}while(BucketNum != -1);
	
	free(file1);
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
    return count;
}

int HashStatistics(char* filename  )
{
	
	int blocks_count;
	void* data;
	BF_ErrorCode code;
	HT_block_info* file1 = malloc(sizeof(HT_block_info));
	HT_info* file = HT_OpenFile(filename);
	code = BF_GetBlockCounter(file->fileDesc, &blocks_count);
	if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
	int Bl_Rec_in_Buckets[file->buckets][2];	
	for(int i = 1; i <= file->buckets; i++) // for every bucket
	{
		int count_of_bl = 0;
		int count_of_rc = 0;
		int end = i;
		do{									// for every block in bucket
			//Block
			BF_Block *block;
			BF_Block_Init(&block);
			
			code = BF_GetBlock(file->fileDesc,end,block);
			if (code != BF_OK){
				BF_PrintError(code);
				return -1;}
			
			//get ht_block_info	
			data=data+(BF_BLOCK_SIZE-sizeof(HT_block_info));
			memcpy(file1,data,sizeof(HT_block_info));
			count_of_bl++;
			count_of_rc = count_of_rc + file1->num_of_records;
			end = file1->next_bucket_block;
			code = BF_UnpinBlock(block);
			if (code != BF_OK){
				BF_PrintError(code);
				return -1;}
			BF_Block_Destroy(&block);
			if (code != BF_OK){
				BF_PrintError(code);
			return -1;}	
		}while(end!=-1);
		Bl_Rec_in_Buckets[i-1][0] = count_of_bl;
		Bl_Rec_in_Buckets[i-1][1] = count_of_rc;	
		
	}	
	
	int min = 1000000;
	int max = -1;
	int minBucket = 0;
	int maxBucket = 0;
	int count=0,count1=0,count2=0;
	
	//question 1
	printf("\nNumber of blocks in  file are: %d",blocks_count);
	for(int i=0; i < file->buckets;i++)
	{
		if(Bl_Rec_in_Buckets[i][1] <= min)
		{
			minBucket = i + 1;
			min = Bl_Rec_in_Buckets[i][1];
		}
		if(Bl_Rec_in_Buckets[i][1] >= max)
		{
			maxBucket = i + 1;
			max = Bl_Rec_in_Buckets[i][1];
		}
		count+= Bl_Rec_in_Buckets[i][1];
		count1+=Bl_Rec_in_Buckets[i][0];
		if(Bl_Rec_in_Buckets[i][0] > 1 )
			count2++;
	}	
	
	
	double MO = (double)(count/file->buckets);
	
	//question 2
	printf("\nMinumum records in bucket: %d are %d",minBucket, min);
	printf("\nMaximum records in bucket: %d are %d",maxBucket, max);
	printf("\nAverage of records for  buckets: %f", MO);
	
	//question 3
	double MO1 = (double)(count1/file->buckets);
	printf("\nAverage of blocks for  buckets: %f", MO1);
	
	
	//question 4
	printf("\n Number of bucker with more than one blocks: %d",count2);
	
	for(int i=0; i < file->buckets; i++)
	{
		if(Bl_Rec_in_Buckets[i][0] > 1)
		{
			printf("\n for bucket no:%d we have %d blocks ",i+1,Bl_Rec_in_Buckets[i][0]-1);
		}	
	}	
	HT_CloseFile(file);
	free(file1);
	return 0;
}

int HashStatistics1(HT_info* file )
{
	
	int blocks_count;
	void* data;
	BF_ErrorCode code;
	HT_block_info* file1 = malloc(sizeof(HT_block_info));
	code = BF_GetBlockCounter(file->fileDesc, &blocks_count);
	if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
	int Bl_Rec_in_Buckets[file->buckets][2];	
	for(int i = 1; i <= file->buckets; i++) // for every bucket
	{
		int count_of_bl = 0;
		int count_of_rc = 0;
		int end = i;
		do{									// for every block in bucket
			//Block
			BF_Block *block;
			BF_Block_Init(&block);
			
			code = BF_GetBlock(file->fileDesc,end,block);
			if (code != BF_OK){
				BF_PrintError(code);
				return -1;}
			
			//get ht_block_info	
			data=data+(BF_BLOCK_SIZE-sizeof(HT_block_info));
			memcpy(file1,data,sizeof(HT_block_info));
			count_of_bl++;
			count_of_rc = count_of_rc + file1->num_of_records;
			end = file1->next_bucket_block;
			code = BF_UnpinBlock(block);
			if (code != BF_OK){
				BF_PrintError(code);
				return -1;}
			BF_Block_Destroy(&block);
			if (code != BF_OK){
				BF_PrintError(code);
			return -1;}	
		}while(end!=-1);
		Bl_Rec_in_Buckets[i-1][0] = count_of_bl;
		Bl_Rec_in_Buckets[i-1][1] = count_of_rc;	
		
	}	
	
	int min = 1000000;
	int max = -1;
	int minBucket = 0;
	int maxBucket = 0;
	int count=0,count1=0,count2=0;
	
	//question 1
	printf("\nNumber of blocks in  file are: %d",blocks_count);
	for(int i=0; i < file->buckets;i++)
	{
		if(Bl_Rec_in_Buckets[i][1] <= min)
		{
			minBucket = i + 1;
			min = Bl_Rec_in_Buckets[i][1];
		}
		if(Bl_Rec_in_Buckets[i][1] >= max)
		{
			maxBucket = i + 1;
			max = Bl_Rec_in_Buckets[i][1];
		}
		count+= Bl_Rec_in_Buckets[i][1];
		count1+=Bl_Rec_in_Buckets[i][0];
		if(Bl_Rec_in_Buckets[i][0] > 1 )
			count2++;
	}	
	
	
	double MO = (double)(count/file->buckets);
	
	//question 2
	printf("\nMinumum records in bucket: %d are %d",minBucket, min);
	printf("\nMaximum records in bucket: %d are %d",maxBucket, max);
	printf("\nAverage of records for  buckets: %f", MO);
	
	//question 3
	double MO1 = (double)(count1/file->buckets);
	printf("\nAverage of blocks for  buckets: %f", MO1);
	
	
	//question 4
	printf("\n Number of bucker with more than one blocks: %d",count2);
	
	for(int i=0; i < file->buckets; i++)
	{
		if(Bl_Rec_in_Buckets[i][0] > 1)
		{
			printf("\n for bucket no:%d we have %d blocks ",i+1,Bl_Rec_in_Buckets[i][0]-1);
		}	
	}	
	
	free(file1);
	return 0;
}