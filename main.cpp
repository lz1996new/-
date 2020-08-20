#include <iostream>
#include <string>
#include "MyRedis.h"
using namespace std;

CRedisClient g_TmcRedisc;

int main()
{
	string strKey1 = "1";  
	string strValue1 = "OK";
	g_TmcRedisc.Init("127.0.0.1", 6379);
	g_TmcRedisc.UpdateCurrentDataFlow("s", 10, 1);
	int ret = g_TmcRedisc.QueryDataInSpecificTime("s", 0, 0);
	if(ret > 0)
	{
		cout << "这个时间段有数据流入" << endl;
	}

    ret = g_TmcRedisc.QueryDataInSpecificTime("s");
	if(ret > 0)
	{
		cout << "这个时间段有数据流入2" << endl;
	}

	//cout << g_TmcRedisc.QueryCurrentDataFlow("12", 10) << endl;	

}
