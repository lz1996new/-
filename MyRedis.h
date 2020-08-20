#ifndef CLASS_REDIS_CLIENT_H_H_H
#define CLASS_REDIS_CLIENT_H_H_H

#include <string>
#include <time.h>
#include "redisclient.h"

class CRedisClient
{
public:
    CRedisClient();
    virtual ~CRedisClient();

	void Init(const std::string& host, int port);

    bool ReConnectRedis();
    void disConnect();
    void checkIdle();

    //Update whether there is current data inflow
    bool UpdateCurrentDataFlow(const std::string& key, time_t value, bool nFlag);
    int QueryCurrentDataFlow(const std::string& key, time_t value);

    //if return == 0  no data inflow  > 0  exist data inflow
    int QueryDataInSpecificTime(const std::string & key, int start, int end); //Query whether this is data flow in range
    int QueryDataInSpecificTime(const std::string & key); //Query whether there is data flow all day

    // string
    std::string Get(const std::string& key);
    bool Set(const std::string& key, const std::string& value);

    // set
    bool SAdd(const std::string& key, const std::string&element);
    long SMembers(const std::string& key, std::set<std::string> &out);
    bool SRem(const std::string& key, const std::string& element);

    // list
    bool Lpush(const std::string& key, const std::string&element);
    bool Lrem(const std::string& key, int count, const std::string&element);
    bool Lrange(const std::string& key, int start, int end, std::vector<std::string> &out);

    // hash
    bool HSet(const std::string& key, const std::string& field, const std::string& value);//hashes
    std::string HGet(const std::string& key, const std::string& fieid);
    void HGetAll(const std::string& key, std::vector<std::pair<std::string, std::string> > &out);
    void HMGet(const std::string& key,std::vector<std::string> &fields,std::vector<std::string> &out);
    void HMSet(const std::string& key, const std::vector<std::string> &fields, const std::vector<std::string> &out);

    bool Del(const std::string& key);
    bool Expire(const std::string& key, long time);
    bool Exists(const std::string& key, bool& isExist);
    bool Ttl(const std::string& key, long& ttl);

    void MGet(const std::vector<std::string> & keys, std::vector<std::string> & out);

    // zset
    bool ZRange(const std::string& key, int start, int end, std::vector<std::string> & out);
    bool ZAdd(const std::string& key, double fScore, const std::string& val);
    bool ZRem(const std::string& key, const std::string& val);
    bool ZCard(const std::string& key, int& count);
    bool ZremLastRecord(const std::string& key);

    bool SISMEMBER(const std::string& key, const std::string& value);
    void Exec(std::vector<redis::command> & commands);

private:
    redis::client*	m_predisClient;
    std::string m_host;
    int m_port;
    time_t m_tLast;
};

#endif



