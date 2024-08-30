#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
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



int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
	printf("**CreateHtFile function**\n");
	int fd1;
	void* data;
	HT_info* file = malloc(sizeof(HT_info));
	BF_ErrorCode code;
	
	BF_Block *block;
	BF_Block_Init(&block);
	
	printf("\nCREATING NEW FILE \n");
	code = BF_CreateFile(sfileName);
	if (code != BF_OK)
		BF_PrintError(code);
	
	printf("\nOPENING NEW FILE \n");
	code = BF_OpenFile(sfileName,&fd1);
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
			
			SHT_info* file = malloc(sizeof(SHT_info));
			data = BF_Block_GetData(block);
			file->buckets = buckets;
			file->max_num_of_records_in_block = (int)((BF_BLOCK_SIZE-sizeof(SHT_block_info))/ sizeof(ShtRecord));
			printf("max num of records %d", file->max_num_of_records_in_block);
			printf("Last block bucket array: \n");
			for(int j=1; j<=buckets; j++)
			{
				file->last_block_bucket_num[j] = j;
				printf("%d ",file->last_block_bucket_num[j]); // we dont use position 0
				
			}
			printf("\n");
			memcpy(data,file,sizeof(SHT_info));
			BF_Block_SetDirty(block);
			code = BF_UnpinBlock(block);
			if (code != BF_OK){
				BF_PrintError(code);
				return -1;}
			free(file);	
		}
		else
		{
			HT_block_info* file1= malloc(sizeof(SHT_block_info));
			data = BF_Block_GetData(block);	
			file1->num_of_records = 0 ;
			file1->next_bucket_block = -1;
			data = data + (BF_BLOCK_SIZE-sizeof(SHT_block_info));
			memcpy(data,file1,sizeof(SHT_block_info));
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

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
	printf("\n**OpenHtFile function**\n");
	BF_ErrorCode code;
	int fd1;
	void* data;
	BF_Block *block;
	BF_Block_Init(&block);
	SHT_info* file = malloc(sizeof(SHT_info));
	
	printf("\nOPENING FILE\n");
	code = BF_OpenFile(indexName ,&fd1);
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
	memcpy(file, data, sizeof(SHT_info));
		
	file->fileDesc = fd1;
	code = BF_UnpinBlock(block);
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return NULL;}
    return file;
}


int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
	printf("\n**CloseFile function**\n");
	BF_ErrorCode code;
	void* data;
	BF_Block *block;
	BF_Block_Init(&block);
	
	
	code = BF_GetBlock(SHT_info->fileDesc,0,block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	data = BF_Block_GetData(block);
	memcpy(data, SHT_info, sizeof(SHT_info));
	
	BF_Block_SetDirty(block);
	code = BF_UnpinBlock(block);
	
	printf("CLOSING FILE \n");
	code = BF_CloseFile(SHT_info->fileDesc);
	if(code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
	free(SHT_info); 
	
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	printf("Getting out of HP_CloseFIle... \n");
    return 0;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
	printf("\n**SHt_InsertEntry function**\n");
	BF_ErrorCode code;
	int blocks_num,bl,BucketNum;
	void* data;
	void* data1;
	void* data2;
	
	//initiation of shtrecord
	ShtRecord shtrecord;
	strcpy(shtrecord.name, record.name);
	shtrecord.block = block_id;
	if(strcmp(record.name, "Vagelis") == 0)
		printf("(%d , %s ) \n", shtrecord.block,shtrecord.name);
	
	BF_Block *block;
	BF_Block_Init(&block);
	
	
	BucketNum = (hash(record.name)%sht_info->buckets)+1;
	
	printf("\n **Insert in to Bucket no: %d\n", BucketNum);
	code = BF_GetBlock(sht_info->fileDesc,sht_info->last_block_bucket_num[BucketNum],block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	data = BF_Block_GetData(block);
	SHT_block_info* file1 = malloc(sizeof(SHT_block_info));
	data1 = data + (BF_BLOCK_SIZE-sizeof(SHT_block_info));
	memcpy(file1,data1,sizeof(SHT_block_info));
	printf("records into block%d: %d\n",sht_info->last_block_bucket_num[BucketNum] ,file1->num_of_records);
	if(file1->num_of_records < sht_info->max_num_of_records_in_block)
	{
		for(int i = 1; i<= file1->num_of_records; i++)
		    data+=sizeof(ShtRecord);
		
		memcpy(data,&shtrecord,sizeof(ShtRecord));
		file1->num_of_records++;
		memcpy(data1,file1,sizeof(SHT_block_info));
	}
	else // if last block is full, create another block
	{	
		BF_Block *block1;
		BF_Block_Init(&block1);
		code = BF_AllocateBlock(sht_info->fileDesc, block1);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}	
		
		//initiation of new block in bucket	
		data2 = BF_Block_GetData(block1);
		memcpy(data2,&shtrecord,sizeof(ShtRecord));
		SHT_block_info* file2 = malloc(sizeof(SHT_block_info));
		file2->num_of_records = 1;
		file2->next_bucket_block = -1;
		data2 = data2 + (BF_BLOCK_SIZE-sizeof(SHT_block_info));
		memcpy(data2,file2,sizeof(SHT_block_info));
		
		//change of last block in bucket
		code = BF_GetBlockCounter(sht_info->fileDesc, &blocks_num);
		file1->next_bucket_block = blocks_num - 1;
		printf("Next bucktet block %d \n",file1->next_bucket_block);
		memcpy(data1,file1,sizeof(SHT_block_info));
		
		//change of file's data
		sht_info->last_block_bucket_num[BucketNum] = blocks_num-1;
		printf("New sht_info array: \n");
		for(int j=1; j<=sht_info->buckets; j++)
			{
				printf("%d ",sht_info->last_block_bucket_num[j]);
				
			}
		printf("\n");
		free(file2);
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
	
	return 0;	
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){
	printf("\n**ShtGetAllEntries\n");
	void* data;
	void* data2;
	void* data4;
	void* data5;
	BF_Block *block;
	BF_Block_Init(&block);
	BF_Block *block4;
	BF_Block_Init(&block4);
	int CountOfBlocks;
	BF_ErrorCode code;
	int BucketNum = (hash(name)%sht_info->buckets)+1;
	printf("BucketNum : %d",BucketNum);
	ShtRecord shtrecord;
	Record record;
	int count = 0;
	SHT_block_info* file1 = malloc(sizeof(SHT_block_info));
	HT_block_info* file4 = malloc(sizeof(HT_block_info));
	printf("max_num_of_records_in_block : %d", sht_info->max_num_of_records_in_block);
	do{
		code = BF_GetBlock(sht_info->fileDesc, BucketNum,block);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
		data  = BF_Block_GetData(block);
		data2 = data +	(BF_BLOCK_SIZE-sizeof(SHT_block_info));
		memcpy(file1,data2,sizeof(SHT_block_info));
		printf("Searching in block:%d in SHT_File\n",BucketNum);
		for(int i=1; i <= file1->num_of_records; i++)
		{
			
			memcpy(&shtrecord,data,sizeof(ShtRecord));
			printShtRecord(shtrecord);
			if (strcmp(name,shtrecord.name) == 0){
				printf("FOUND sht -> ");
				printShtRecord(shtrecord);
				
				code = BF_GetBlock(ht_info->fileDesc,shtrecord.block,block4);
				if (code != BF_OK){
					BF_PrintError(code);
					return -1;}
				data4  = BF_Block_GetData(block4);
				data5 = data4 +(BF_BLOCK_SIZE-sizeof(HT_block_info));	
				memcpy(file4,data5,sizeof(HT_block_info));
				for(int i=1; i <= file4->num_of_records; i++) // prwteuon
				{
					memcpy(&record,data4,sizeof(Record));
					//printf("\n record.id %d : ",record.id);
					if(strcmp(name,record.name) == 0){
						printf("FOUND ht -> ");
						printRecord(record);}
					if(i<file4->num_of_records)		
						data4+=sizeof(Record);
				}	
				count++;
				printf("count %d",count);
				code = BF_UnpinBlock(block4);
				if (code != BF_OK){
					BF_PrintError(code);
					return -1;}
				
	
			}
					
			data+=sizeof(ShtRecord);
			
		}
		code = BF_UnpinBlock(block);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
		count ++;
		BucketNum = file1->next_bucket_block;
		printf("\n**BucketNum: %d",BucketNum);
	}while(BucketNum != -1);
	
	
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	BF_Block_Destroy(&block4);
		if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	free(file4);
	
	free(file1);
	
	return count;
}	



 int hash(char name[15]) {
  int sum = 0;
  int i = 0;
 while(name[i] != '\0'){
	 sum+=name[i];
	 i++;
  }
  return sum;

 }
 
 int hash1(char name[15]){
	if(strcmp(name,"Yannis") == 0 )
		 return 1;
	if(strcmp(name,"Christofos") == 0 )
		 return 2;
	if(strcmp(name,"Sofia") == 0 )
		 return 3; 
	if(strcmp(name,"Marianna") == 0 )
		 return 4;  
	if(strcmp(name,"Vagelis") == 0 )
		 return 5;   
	if(strcmp(name,"Maria") == 0 )
		 return 4;  
	if(strcmp(name,"Iosif") == 0 )
		 return 7;  
	if(strcmp(name,"Dionisis") == 0 )
		 return 8;
	if(strcmp(name,"Konstantina") == 0 )
		 return 9; 
	if(strcmp(name,"Giorgos") == 0 )
		 return 10; 
	if(strcmp(name,"Dimitris") == 0 )
		 return 8; 
	if(strcmp(name,"Theofilos") == 0 )
		 return 6; 
 }
 
 int HashStatisticsSht(char* filename  )
{
	
	int blocks_count;
	void* data;
	BF_ErrorCode code;
	SHT_block_info* file1 = malloc(sizeof(SHT_block_info));
	SHT_info* file = SHT_OpenSecondaryIndex(filename);
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
			
			//get sht_block_info	
			data=data+(BF_BLOCK_SIZE-sizeof(SHT_block_info));
			memcpy(file1,data,sizeof(SHT_block_info));
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
	SHT_CloseSecondaryIndex(file);
	free(file1);
	return 0;
}



int HashStatisticsSht1(SHT_info* file )
{
	printf("\nskata");
	int blocks_count;
	void* data;
	BF_ErrorCode code;
	SHT_block_info* file1 = malloc(sizeof(SHT_block_info));
	code = BF_GetBlockCounter(file->fileDesc, &blocks_count);
	if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
	int Bl_Rec_in_Buckets[file->buckets][2];	
	for(int i = 1; i <= file->buckets; i++) // for every bucket
	{
		printf("\nskata1");
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
			
			//get sht_block_info	
			data=data+(BF_BLOCK_SIZE-sizeof(SHT_block_info));
			memcpy(file1,data,sizeof(SHT_block_info));
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