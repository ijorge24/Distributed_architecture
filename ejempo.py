import redis
r=redis.Redis('localhost')
print('aaaaaaaaaaa')
r.mset({"croacia": "Zabreb","mah": "nas" })
print(r.get("croacia"))