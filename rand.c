#include<stdio.h>
#include<stdlib.h>
#include<time.h>

int main()
{
   int count = 99999;
   int id = 0;
   int random; 
   srand(time(NULL));
   while(count > 0)
   {
	id++;
	do
	{
		random = rand() % 100;
	} while(random == 0);
	printf("%d ", random);
	if(id == 10)
	{
		printf("\n");
		id = 0;
	}
	count--;
   }
}
