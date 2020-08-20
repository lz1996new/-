#include "MyRedis.h"
#include <stdio.h>
#include <iostream>

#define REDIS_CLIENT_EXPECTING_INT_1    "expecting int reply of 1"
#define REDIS_CLIENT_KEY_NOT_EXIST      "**nonexistent-key**"

#define REDIS_KYE_EX    "UsrId_"



CRedisClient::CRedisClient()
    : m_predisClient(NULL), m_port(-1), m_tLast(0)
{
}

CRedisClient::~CRedisClient()
{
    disConnect();
}

void CRedisClient::Init(const std::string& host, int port)
{
    m_host = host;
    m_port = port;
    //checkIdle();
    ReConnectRedis();
    //m_predisClient->setbit("1234", "1", "1");
    //int ncount = m_predisClient->bitcount("s", 1, 7);
    //printf("bitcount : %d", ncount);
}

void CRedisClient::checkIdle()
{
    time_t t = time(NULL);
    //if (t - m_tLast > 120 || m_predisClient == NULL)
    if(m_predisClient == NULL)
    {
        //disConnect();
        if (ReConnectRedis())
            m_tLast = t;
    }
    else
        m_tLast = t;
}


void CRedisClient::disConnect()
{
    if (m_predisClient)
    {
        delete m_predisClient;
        m_predisClient = NULL;
    }
}

bool CRedisClient::ReConnectRedis()
{
    if (m_predisClient)
        return true;

    try
    {
        //MYLOG_INFO("ReConnectRedis");
        m_predisClient = new redis::client(m_host, m_port);
        if(m_predisClient)
        {
            //MYLOG_INFO( "Connect to redis %s:%d OK!", m_host.c_str(), m_port);
            printf( "Connect to redis %s:%d OK!\n", m_host.c_str(), m_port);
            return true;
        }
        else
        {
            //MYLOG_INFO( "Connect to redis %s:%d failed!", m_host.c_str(), m_port);
            printf("New redis %s:%d fail!", m_host.c_str(), m_port);
            return false;
        }
    }
    catch (redis::redis_error & e)
    {
        if(m_predisClient)
        {
            delete m_predisClient;
        }
        m_predisClient = NULL;
        printf( "Connect to redis %s fail! error:%s", m_host.c_str(), e.what());
        return false;
    }

    return true;
}

int  CRedisClient::QueryDataInSpecificTime(const std::string & key, int start, int end)
{
    int counter = 0;
    int ret = 0;

    const std::string strKey = REDIS_KYE_EX + key;

    while(counter < 2)
    {
        ReConnectRedis();
        if(NULL == m_predisClient)
        {
            printf( "redis client is null\n");
            return false;
        }

        try
        {
            ++counter;
            ret = m_predisClient->bitcount(strKey, start, end);
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
            printf("error info:%s", e.what());
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                ret = -1;
                return ret;
            }
            else
            {
                continue;
            }
        }
        
        return ret;
    }
}

int CRedisClient::QueryDataInSpecificTime(const std::string & key)
{
    int counter = 0;
    int ret = 0;

    const std::string strKey = REDIS_KYE_EX + key;

    while(counter < 2)
    {
        ReConnectRedis();
        if(NULL == m_predisClient)
        {
            printf( "redis client is null\n");
            return false;
        }

        try
        {
            ++counter;
            ret = m_predisClient->bitcount(strKey);
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
            printf("error info:%s", e.what());
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                ret = -1;
                return ret;
            }
            else
            {
                continue;
            }
        }
        
        return ret;
    }
}

bool CRedisClient::UpdateCurrentDataFlow(const std::string& key, time_t value, bool nFlag)
{
    int counter = 0;

    const std::string strKey = REDIS_KYE_EX + key;

    while (counter < 2)
    {
        ReConnectRedis();
        //checkIdle();
        if (NULL == m_predisClient)
        {
           // MYLOG_INFO("redis client is null");
            printf( "redis client is null\n");
            return false;
        }

        try
        {
            ++counter;
            
            m_predisClient->setbit(strKey, value, nFlag);
            printf("setbit successful\n");

        }
        catch (redis::redis_error & e)
        {
            printf("error info:%s", e.what());
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return false;
            }
            else
            {
                continue;
            }
        }
        //MYLOG_INFO("Data stored successfully in redis");
        return true;
    }

    return true; 
}

int CRedisClient::QueryCurrentDataFlow(const std::string& key, time_t value)
{
    int ret = 0;
    int counter = 0;

    while(counter < 2)
    {
        ReConnectRedis();
        //checkIdle();
        if (NULL == m_predisClient)
        {
            printf("redis client is null");
            return ret;
        }

        try
        {
            ++counter;
             ret = m_predisClient->getbit(key, value);
             printf("getbit successful\n");
            //if(ret == "**nonexistent-key**")
            //    return "";
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                ret = 0;
                return ret;
            }
            else
            {
                continue;
            }
        }
        return ret;
    }
    return ret;
    
}

bool CRedisClient::Set(const std::string& key, const std::string& value)
{
    int counter = 1;

    while (counter < 2)
    {
        ReConnectRedis();
        //checkIdle();
        if (NULL == m_predisClient)
        {
           // MYLOG_INFO("redis client is null");
            printf( "redis client is null\n");
            return false;
        }

        try
        {
            printf("enter redis set\n");
            ++counter;
            
            m_predisClient->set(key, value);
	        printf("redis save successful\n");

        }
        catch (redis::redis_error & e)
        {
            printf("error info:%s", e.what());
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return false;
            }
            else
            {
                continue;
            }
        }
        //MYLOG_INFO("Data stored successfully in redis");
        return true;
    }

    return true;
}

bool CRedisClient::SAdd(const std::string& key, const std::string& element)
{
    int counter = 0;

    while(counter < 2)
    {
        //ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            //printf("redis client is null");
            return false;
        }

        try
        {
            ++counter;
            m_predisClient->sadd(key, element);
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return false;
            }
            else
            {
                continue;
            }
        }
        return true;
    }

    return true;
}


bool CRedisClient::HSet(const std::string& key, const std::string&field, const std::string& value)
{
    int counter = 0;

    while(counter < 2)
    {
        //ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            printf("redis client is null");
            return false;
        }

        try
        {
            ++counter;
            m_predisClient->hset(key, field, value);
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return false;
            }
            else
            {
                continue;
            }
        }
        return true;
    }

    return true;
}

void CRedisClient::HMGet(const std::string& key,std::vector<std::string> &fields,std::vector<std::string> &out)
{
   int counter = 0;

    while(counter < 2)
    {
        //ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            printf("redis client is null");
            return;
        }

        try
        {
            ++counter;
            m_predisClient->hmget(key, fields, out);
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return;
            }
            else
            {
                continue;
            }
        }
        return;
    }

}

void CRedisClient::HMSet(const std::string& key,const std::vector<std::string> &fields,const std::vector<std::string> &out)
{
   int counter = 0;

    while(counter < 2)
    {
        //ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            printf("redis client is null");
            return;
        }

        try
        {
            ++counter;
            m_predisClient->hmset(key, fields, out);
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return;
            }
            else
            {
                continue;
            }
        }
        return;
    }

}


void CRedisClient::HGetAll(const std::string& key, std::vector<std::pair<std::string, std::string> > &out)
{
   int counter = 0;

    while(counter < 2)
    {
        //ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            printf("redis client is null");
            return;
        }

        try
        {
            ++counter;
            m_predisClient->hgetall(key, out);
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return;
            }
            else
            {
                continue;
            }
        }
        return;
    }

}


std::string CRedisClient::Get( const std::string& key )
{
    std::string ret = "";

    int counter = 0;

    while(counter < 2)
    {
        ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            printf("redis client is null");
            return "";
        }

        try
        {
            ++counter;
            ret = m_predisClient->get(key);
            if(ret == "**nonexistent-key**")
                return "";
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                ret= "";
                return ret;
            }
            else
            {
                continue;
            }
        }
        return ret;
    }
    printf("ger data successful:%s\n", ret.c_str());
    return ret;
}

long CRedisClient::SMembers( const std::string& key, std::set<std::string> &out )
{
    long ret = -1;

    int counter = 0;

    while(counter < 2)
    {
        //ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            //printf("redis client is null");
            return ret;
        }

        try
        {
            ++counter;
            ret = m_predisClient->smembers(key, out);
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                ret= -1;
                return ret;
            }
            else
            {
                continue;
            }
        }
        return ret;
    }

    return ret;
}

std::string CRedisClient::HGet( const std::string& key, const std::string& fieid )
{
    std::string ret = "";
    int counter = 0;

    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
        {
            //printf("redis client is null");
            return ret;
        }

        try
        {
            ++counter;
            ret = m_predisClient->hget(key, fieid);
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                ret= "";
                return ret;
            }
            else
            {
                continue;
            }
        }

        if (REDIS_CLIENT_KEY_NOT_EXIST == ret)
            return "";

        return ret;
    }

    return ret;
}

bool CRedisClient::SRem( const std::string& key, const std::string& element )
{
    int counter = 0;

    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            m_predisClient->srem(key, element);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;
            else
                continue;
        }
    }

    return true;
}

// list
bool CRedisClient::Lpush(const std::string& key, const std::string& element)
{
    int counter = 0;

    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            m_predisClient->lpush(key, element);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;
            else
                continue;
        }
    }

    return true;
}

// list
bool CRedisClient::Lrem(const std::string& key, int count, const std::string& element)
{
    int counter = 0;

    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            m_predisClient->lrem(key, count, element);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;
            else
                continue;
        }
    }

    return true;
}

bool CRedisClient::Lrange(const std::string& key, int start, int end, std::vector<std::string> &out)
{
    int counter = 0;

    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            m_predisClient->lrange(key, start, end, out);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;
            else
                continue;
        }
    }

    return true;
}


bool CRedisClient::Del( const std::string& key )
{
    int counter = 1;
    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
        {
            //printf("redis client is null");
            return false;
        }

        try
        {
            ++counter;
            m_predisClient->del(key);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                printf("redis exception %s!\n", e.what());
                return false;
            }
            else
            {
                continue;
            }
        }
    }

    //MYLOG_INFO("Delete data Successful from Redis");
    return true;
}

void CRedisClient::MGet(const std::vector<std::string> & keys, std::vector<std::string> & out)
{
    int counter = 0;
    while(counter < 2)
    {
        //ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            //printf("redis client is null");
            return;
        }

        try
        {
            ++counter;
            m_predisClient->mget(keys,out);
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return;
            }
            else
            {
                continue;
            }
        }
        return;
    }

    return;
}

bool CRedisClient::ZRange(const std::string& key, int start, int end, std::vector<std::string> & out)
{
    bool ret = false;
    int counter = 0;
    while(counter < 2)
    {
        //ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            //printf("redis client is null");
            return false;
        }

        try
        {
            ++counter;
            m_predisClient->zrange(key,start,end,out);
            ret = true;
        }
        catch (redis::redis_error & e)
        {
            ret = false;
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return ret;
            }
            else
            {
                continue;
            }
        }
        return ret;
    }

    return ret;
}

bool CRedisClient::ZAdd(const std::string& key, double fScore, const std::string& val)
{
    int counter = 0;
    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            m_predisClient->zadd(key, fScore, val);
            return true;
        }
        catch (redis::redis_error & e)
        {
            if (REDIS_CLIENT_EXPECTING_INT_1 == std::string(e.what()))
                return true;

            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;

            continue;
        }
    }

    return true;
}

bool CRedisClient::ZRem(const std::string& key, const std::string& val)
{
    int counter = 0;
    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            m_predisClient->zrem(key, val);
            return true;
        }
        catch (redis::redis_error & e)
        {
            if (REDIS_CLIENT_EXPECTING_INT_1 == std::string(e.what()))
                return true;

            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;

            continue;
        }
    }

    return true;
}

bool CRedisClient::ZCard(const std::string& key, int& count)
{
    int counter = 0;
    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            count = m_predisClient->zcard(key);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;

            continue;
        }
    }

    return true;
}

bool CRedisClient::ZremLastRecord(const std::string& key)
{
    int counter = 0;
    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            m_predisClient->zremrangebyrank(key, 0, 0);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;

            continue;
        }
    }

    return true;
}

bool CRedisClient::Expire(const std::string& key, long time)
{
    int counter = 0;
    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            m_predisClient->expire(key, time);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;

            continue;
        }
    }

    return true;
}

bool CRedisClient::Exists(const std::string& key, bool& isExist)
{
    int counter = 0;
    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            isExist = m_predisClient->exists(key);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;

            continue;
        }
    }

    return true;
}

bool CRedisClient::Ttl(const std::string& key, long& ttl)
{
    int counter = 0;
    while(counter < 2)
    {
        checkIdle();
        if (NULL == m_predisClient)
            return false;

        try
        {
            ++counter;
            ttl = m_predisClient->ttl(key);
            return true;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
                return false;

            continue;
        }
    }

    return true;
}

bool CRedisClient::SISMEMBER(const std::string& key, const std::string& value)
{
    bool ret = false;
    int counter = 0;
    while(counter < 2)
    {
        //ReConnectRedis();
        checkIdle();
        if (NULL == m_predisClient)
        {
            return false;
        }

        try
        {
            ++counter;
            ret = m_predisClient->sismember(key,value);
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            if(counter > 1)
            {
                return ret;
            }
            else
            {
                continue;
            }
        }
        return ret;
    }

    return ret;
}

void CRedisClient::Exec(std::vector<redis::command> & commands)
{
    int counter = 0;
    while(counter < 2)
    {
        if (NULL == m_predisClient)
        {
            checkIdle();
        }	
        if (NULL == m_predisClient)
        {
            return ;  
     	}
		
        try
        {
            m_predisClient->exec(commands);
            break;
        }
        catch (redis::redis_error & e)
        {
            delete m_predisClient;
            m_predisClient = NULL;
            ++counter;
        }
    }
}

