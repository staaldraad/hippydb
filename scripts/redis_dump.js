/*
   Dump data from open Redis instance. 
   Usage: node redis_dump.js 10.10.0.1
          node redis_dump.js 10  #dumps the first 10 keys from the instance
          node redis_dump.js keyname #dump the value of a specific key
   Author: etienne@sensepost.com
   Version: 1.0 12 February 2015
*/

var redis = require("redis")
var argv = require('minimist')(process.argv.slice(2));

if(process.argv.length < 2){
    console.error("Usage: \n --help display this messaage")
    console.error("-h <ip-address> the host to check")
    console.error("-p <port>  the port to use, default 6379")
    console.error("-k <key> dump values for specific keyname")
    console.error("-n dump first 'n' values")
    return
}

var port = argv.p? argv.p:6379
var host = argv.h
var client = redis.createClient(port,host);

client.on("error", function (err) {
        console.log("Error " + err);
        client.end()
});
if(argv.h&&!argv.k&&!argv.n)//not dumping a specific key, get list of keys
{
    client.on("ready",function(err){
        console.info("[+] Connected! \n[+] Server version:")
        console.info(client.server_info.redis_version);


        client.hkeys("*",function(err,hkeys){
            if(err){
             console.error(err)
             client.end()
            }
            else{
               console.log("[*] HKeys: "+hkeys);
               client.keys("*",function(err,keys){
             if(err){
                 console.error(err)
                 client.end()
             }
             else{
                 console.log("[*] Keys: "+keys);
                 client.end()
             }
              })

            }
        })
    })
}
if(argv.k||argv.n)
{
   var key = '' //if we are just getting the first x number of keys
   var num = 0 //default we get the value of one key (the specified key)
   if(argv.k)
       key = argv.k
   if(argv.n)
       num = argv.n

    client.on("ready",function(err){
        console.info("[*] Connected!")
            if(num == 0 ){
            console.info("[+] Getting key") 
        client.get(key,function(err,value){
                        if(err) console.log(err)
                        else
                            console.log("[*] value: "+value);
                        client.end()
                });
               }
               else{
                     console.info("[+] Getting "+num+" keys")
                     client.keys("*",function(err,keys){
             if(err){
                 console.error(err)
                 client.end()
             }
             else{
                 console.log("[*] Keys: "+keys);
                             keys.some(function(key,index){
                                  if(index == num){
                                       return true;
                                  }

                                  client.get(key,function(err,value){
                                       console.log("[*] Key: "+key)
                                       if(err) console.log(err)
                                       else
                                       console.log(value);
                                  })
                             })
                            client.quit();
             }
                     })
                }
    })
}

