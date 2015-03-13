/* Connect to and extract values from Riak database. Default port 8087

   Author: etienne@sensepost.com
   Version: 1.0 26 February 2015
*/

var argv = require('minimist')(process.argv.slice(2));
var riak = require('riak-pb');

if(process.argv.length < 2){
    console.error("Usage: \n --help display this messaage")
    console.error("-h <ip-address> the host to check")
    console.error("-p <port>  the port to use, default 8087")
    console.error("-b <bucket> dump a list of keys for the bucket ")
    console.error("-k <key> dump values for specific bucket:key")
    console.error("-n dump first 'n' values in a bucket")
    console.error("-s list server info")
    return
}

var d_keys = false
var n_keys = 10
var bucket = ""

if(argv.b){
     d_keys = !argv.k 
     bucket = argv.b
}
if(argv.n)
     n_keys = argv.n

var host = argv.h
var port = argv.p ? argv.p:8087
var options = {
  nodes: [
    {
      host: host,
      port: port }],
  maxPool: 5,     // Maximum number of connections in the connection pool - default is 5
  maxRetries: 1, // maximum times the client tries to reconnect before failing - default is 10
  maxDelay: 2000, // maximum time (ms) between reconnections - reconnections have an exponential backoff, but limited to this value - default is 2000
  innactivityTimeout: 1000
};

var client = riak(options);
console.log("[*] Checking: "+host)

if(argv.s)
{
    client.getServerInfo(function(err,info){
        if(err)console.error("[X] "+err)
        console.info("[+] Server info:");
        console.info("[+] Node: "+info.node+" Version: "+info.server_version);
        client.disconnect() 
    });
}

if(!argv.k&&!d_keys){
    client.getBuckets(function(err,buckets){
        console.log("[+] Extracting Buckets ")
        buckets.forEach(function(bucket){
            console.log("[*] Bucket: "+bucket)
            
        })
            client.disconnect();
    })
        
}
if(argv.n&&argv.b){
      console.log("[*] Keys for: "+bucket)
      client.getKeys(bucket, function (err, keys) {
            keys.some(function(value,index){
                  if(index==argv.n){
                      client.disconnect()
                      return true;
                  }
                  client.get({bucket:bucket,key:value},function(err,res){
                          console.log("[*] Key: "+value)
                          console.log(res)
                  });
            });
       });
}
if(d_keys&&!argv.n){
      console.log("[*] Keys for: "+bucket)
      client.getKeys(bucket, function (err, keys) {
         console.log(keys)
       });
}
if(argv.k||argv.key)
{
   if(!argv.b){
      console.log("[X] Requires bucket id and key. Please include -b <bucket> and -k <key>")
      return
   }
   var key = ""
   if(argv.k) key = argv.k

   client.get({bucket:bucket,key:key},function(err,res){
            //console.log(res)
              console.log(res.content[0].value.toString());
   });
 
}
