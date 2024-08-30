#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}                        

int HP_CreateFile(char *fileName){
	printf("**CreateFile function**\n");;
	int fd1,offset;
	int blocks_num;
	void* data;
	int mthd = 1; // 1 for heapfile
	HP_info* file = malloc(sizeof(HP_info));
	BF_ErrorCode code;
	
	//Initiation of the block
	BF_Block *block;
	BF_Block_Init(&block);
	
	
	
	//Create - Open File - Allocate
	
	//Create
	printf("\nCREATING NEW FILE \n");
	code = BF_CreateFile(fileName);
	if (code != BF_OK)
		BF_PrintError(code);
	
	//Open
	printf("\nOPENING NEW FILE \n");
	code = BF_OpenFile(fileName,&fd1);
	printf("fileName: %s, fileDesc: %d \n", fileName, fd1);
	
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
	//Allocate Block 0
	printf("\nALLOCATING FIRST BLOCK\n");
	code = BF_AllocateBlock(fd1, block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
	//Initiation of HP_info
	data = BF_Block_GetData(block);
	file->NumberOfRecords = (int)(BF_BLOCK_SIZE/ sizeof(Record));
	file->LastBlockRecords = 0;
	file->Method = 1;
	
	memcpy(data,file,sizeof(HP_info));
	
	
	//Set dirty and unpinn the first block
	printf("\nSETTING DIRTY AND UNPINNING THE FIRST BLOCK\n");
	BF_Block_SetDirty(block);
	code = BF_UnpinBlock(block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
	//free the allocated memory, close file
	free(file);
	printf("\nBLOCK_DESTROY\n");
	BF_Block_Destroy(&block);
	code = BF_CloseFile(fd1);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
    return 0;
}

HP_info* HP_OpenFile(char *fileName){
	int blocks_num;
	printf("\n**OpenFile function**\n");
	BF_ErrorCode code;
	int fd1;
	void* data;
	BF_Block *block;
	BF_Block_Init(&block);
	HP_info* file1 = malloc(sizeof(HP_info));
	
	// Open file
	printf("\nOPENING FILE\n");
	code = BF_OpenFile(fileName ,&fd1);
	if (code != BF_OK){
		BF_PrintError(code);
		return NULL;}
	
	//Get Block 0
	printf("\nGETTING BLOCK 0\n");
 	code = BF_GetBlock(fd1,0,block);
	if (code != BF_OK){
		BF_PrintError(code);
		return NULL;}
	
	data = BF_Block_GetData(block);
	memcpy(file1, data, sizeof(HP_info));
	
	
	
	
	if (file1->Method != 1){
		printf ("%s \n", "NoHeapFile");
		return NULL;}
	
		
	file1->fileDesc = fd1;
	code = BF_UnpinBlock(block);
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return NULL;}
	
	printf("skata\n");
	return file1;
	
}

int HP_CloseFile(HP_info* hp_info ){
	printf("\n**CloseFile function**\n");
	BF_ErrorCode code;
	void* data;
	BF_Block *block;
	BF_Block_Init(&block);
	
	
	code = BF_GetBlock(hp_info->fileDesc,0,block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
	data = BF_Block_GetData(block);
	memcpy(data, hp_info, sizeof(HP_info)); // save the new hp_info!!!
	
	BF_Block_SetDirty(block);
	code = BF_UnpinBlock(block);
	
	printf("CLOSING FILE \n");
	code = BF_CloseFile(hp_info->fileDesc);
	if(code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
	free(hp_info); 
	
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	printf("Getting out of HP_CloseFIle... \n");
    return 0;
}

int HP_InsertEntry(HP_info* hp_info, Record record){
	printf("HP_InsertEntry\n");
	BF_ErrorCode code;
	int blocks_num;
	void* data;
	BF_Block *block;
	BF_Block_Init(&block);
	
	
	code = BF_GetBlockCounter(hp_info->fileDesc, &blocks_num);
		if(code != BF_OK){
			BF_PrintError(code);
			return -1;}
		
	if((hp_info->NumberOfRecords > hp_info->LastBlockRecords) && (hp_info->LastBlockRecords != 0))
	{
		printf("HP_InsertEntry first if \n");
		code = BF_GetBlock(hp_info->fileDesc, blocks_num-1 , block);
		if(code != BF_OK){
			BF_PrintError(code);
			return -1;}
		
		data = BF_Block_GetData(block);
		for(int i = 1; i <= hp_info->LastBlockRecords; i++)
		{
			data+=sizeof(Record);
		}
		memcpy(data,&record,sizeof(Record));	
		hp_info->LastBlockRecords++;
		printf("blocks_num = :%d \n",blocks_num);
	}
	
	if((hp_info->NumberOfRecords == hp_info->LastBlockRecords) || (hp_info->LastBlockRecords == 0))
	{
		printf("HP_InsertEntry second if \n");
		code = BF_AllocateBlock(hp_info->fileDesc,block);
		if(code != BF_OK){
			BF_PrintError(code);
			return -1;}
		
		data = BF_Block_GetData(block);
		memcpy(data,&record,sizeof(Record));	
		hp_info->LastBlockRecords = 1;
		blocks_num++;
		printf("blocks_num = :%d \n",blocks_num);
	}
	BF_Block_SetDirty(block);
	code = BF_UnpinBlock(block);
	BF_Block_Destroy(&block);
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
    return blocks_num-1;	
}

int HP_GetAllEntries(HP_info* hp_info, int value){
	void* data;
	int blocks_count;
	BF_ErrorCode code;
	BF_Block *block;
	BF_Block_Init(&block);
	Record record;
	code = BF_GetBlockCounter(hp_info->fileDesc, &blocks_count);
	int  i;
	if (code != BF_OK){
		BF_PrintError(code);
		return -1;}
	
	for( i = 1; i <= blocks_count - 1; i++) // file
	{
		code = BF_GetBlock(hp_info->fileDesc, i , block);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
		data = BF_Block_GetData(block);
		if(i == blocks_count-1)
		{
			for(int j=1; j <= hp_info->LastBlockRecords; j++) // LastBlock
			{
				
				for(int k = 1; k <= hp_info->LastBlockRecords;k++)
				{
					Record record;

					memcpy(&record ,data, sizeof(Record));
					if(record.id == value)
					{
						printf("RECORD FOUND!:  ");
						printRecord(record);
					}
					if(j != hp_info->LastBlockRecords)
						data+=sizeof(Record);
				}	
			}
		}
		else
		{
			for(int j=1; j <= hp_info->NumberOfRecords; j++)
			{
				for(int k = 1; k <= hp_info->LastBlockRecords;k++)
				{
					Record record;
					memcpy(&record ,data, sizeof(Record));
					if(record.id == value)
					{
						printf("RECORD FOUND!:  ");
						printRecord(record);
					}	
					data+=sizeof(Record);
			
				}
			}
		
		}
		code = BF_UnpinBlock(block);
		if (code != BF_OK){
			BF_PrintError(code);
			return -1;}
	
		
	}
	BF_Block_Destroy(&block);
	
	
	
	return blocks_count;
}

