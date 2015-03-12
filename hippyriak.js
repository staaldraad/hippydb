var util = require('util')
var riak = require("riak-pb")

var options = {host:'127.0.0.1',port:8087,limit:10}

var help = function(callback){
       var helpm = "Riak dumper -- Help\n"
           helpm += "Commands: status, buckets, keys, dump, dumpk\n"
           helpm += "> riak status                       //Check if we can connect to the database\n"
           helpm += "> riak buckets                      //list all buckets on the host\n"
           helpm += "> riak keys  <bucket>               //list all keys in a bucket\n"
           helpm += "> riak dumpk <bucket> <key>         //dump value of key\n"
           helpm += "> riak dump  <bucket>               //show 'limit' number of values in a bucket\n"

       return callback(null,helpm)
}

var getStatus = function(callback)
{
    connector(function(err,client){
	    client.getServerInfo(function(err,info){
		if(err||!info)
		   return callback(err,'Error connecting to Riak')
	
   	        return callback(null,"Connected! Node is ["+info.node+"]");
	    });
   })
}

var listBuckets = function(client,callback){
    client.getBuckets(function(err,buckets){
        if(err)
             return callback(err,'Error getting buckets');
        var message = "Found ["+buckets.length+"] Buckets\n"
        buckets.forEach(function(bucket){
            message+= bucket+"\n"
            
        })
        client.disconnect();
        return callback(null,message)
    })
}

var listKeys = function(client,bucket,callback)
{
  client.getKeys(bucket, function (err, keys) {
 	if(err) 
            return callback(err,'Error getting key');

        var message = "Found ["+keys.length+"] keys\n"
        keys.forEach(function(key){
             message += key+"\n"
        })
        client.disconnect();
        return callback(null,message);
  });
}

var dumpKey = function(client,bucket,key,callback)
{
   client.get({bucket:bucket,key:key},function(err,res){
        if(err)
             return callback(err,'Error getting value')
             return callback(null,(res.content[0].value.toString()));
   });
}

var dump = function(client,bucket,callback)
{
    client.getKeys(bucket, function (err, keys) {
            var message = ""
            keys.some(function(key,index){
                  if(index==options.limit){
                      return true;
                  }
                  client.get({bucket:bucket,key:key},function(err,res){
                       if(err) return true;
                       message += res+'\n'
                  });
            });
            client.disconnect()
            return callback(null,message)
       });
}

var connector = function(callback){
	var opts = {
	  nodes: [
	    {
	      host: options.host,
	      port: options.port }],
	  maxPool: 5,     // Maximum number of connections in the connection pool - default is 5
	  maxRetries: 1, // maximum times the client tries to reconnect before failing - default is 10
	  maxDelay: 2000, // maximum time (ms) between reconnections - reconnections have an exponential backoff, but limited to this value - default is 2000
	  innactivityTimeout: 1000
	};
     var client = riak(opts);
     return callback(null,client)
}

exports.setOptions=function(noptions){
      options.host = noptions.host? noptions.host:options.host
      options.port = noptions.port? noptions.port:options.port
      options.limit = noptions.limit? noptions.limit:options.limit
}

exports.commandParse=function(command,callback)
{
    switch(command[1]){
       case 'help': 
                    help(callback); 
		    break;
       case 'status': 
                     getStatus(callback);
                      break;
       case 'buckets':  connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Riak')
                         else
                             listBuckets(con,callback)
                    })
                    break;
       case 'keys':
                      if(!command[2]) 
                              return(callback(true,'Missing bucket name'))
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Riak')
                         else
                             listKeys(con,command[2],callback)
                    })
                    break;
       case 'dump': 
                      if(!command[2])
                           return callback(true,'Missing bucket name') 
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Riak')
                         else
                             dump(con,command[2],callback)
                    })
                    break;
        case 'dumpk': 
                      if(!command[2])
                           return callback(true,'Missing bucket name and keyname')
                      if(!command[3]) 
                           return callback(true,'Missing keyname')
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Riak')
                         else
                             dumpKey(con,command[2],command[3],callback)
                    })
                    break;
      default: return callback(true,'Command not found');
    }
}
