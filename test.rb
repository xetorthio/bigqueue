
require 'rubygems'
require 'redis'

r = Redis.new
shuffle_list = <<LUA
local type = redis('type',KEYS[1])
if type.ok ~= 'list' then return {err = "Key is not a list"} end
local len = redis('llen',KEYS[1])
for i=0,len-1 do
local r = (math.random(len))-1
local a = redis("lindex",KEYS[1],i)
local b = redis("lindex",KEYS[1],r)
redis("lset",KEYS[1],i,b)
redis("lset",KEYS[1],r,a)
end
return len
LUA

r.del(:mylist)
(0..10).each{|x| r.lpush(:mylist,x)}
r.eval(shuffle_list,1,:mylist)
r.lrange(:mylist,0,-1).each{|x|
    puts(x)
}
