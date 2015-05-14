var util = require('util')
var redis = require("redis")

var options = {host:'127.0.0.1',port:6379,limit:10}
exports.commands = ['help','status','key','keys','hkey','dump']

var help = function(callback){
       var helpm = "Redis dumper -- Help\n"
           helpm += "Commands: \n"
           helpm += "> redis status                       //Check if we can connect to the database\n"
           helpm += "> redis keys                         //list all [h]keys on the host\n"
           helpm += "> redis key  <key>                   //dump value of key\n"
           helpm += "> redis hkey <hkey>                  //dump value of hkey\n"
           helpm += "> redis dump                        //show 'limit' number of values.\n"

       return callback(null,helpm)
}

var getStatus = function(callback)
{
    connector(function(err,client){
         if(err)	 
           return callback(err,'Error connecting to Redis')
         
         client.end()
         return callback(null,'Connected! - Connection is good to go - '+client.server_info.redis_version)
    })
}

var listKeys = function(client,callback){
     client.hkeys("*",function(err,hkeys){
            if(err){
              client.end()
              return callback(err,'Error getting hkeys');
            }
            else{
               var message = "HKeys: "+hkeys+'\n';
               client.keys("*",function(err,keys){
		     if(err){
			 client.end()
                         return callback(err,'Error getting keys');
		     }
		     else{
			 message += "[*] Keys: "+keys+"\n";
			 client.end()
                         return callback(null,message)
		     }
	      })
            }
        })
}
var dumpKey = function(client,key,callback)
{
   client.get(key,function(err,value){
	if(err) 
            return callback(err,'Error getting key');
	else
            return callback(null,value);
   });
}
var dumpHkey = function(client,hkey,callback)
{
   client.get(key,function(err,value){
	if(err) 
            return callback(err,'Error getting key');
	else
            return callback(null,value);
   });
}
var dump = function(client,callback)
{
    client.keys("*",function(err,keys){
	     if(err){
		 client.end()
                 return callback(err,'Error getting key');
	     }
	     else{
                    var message = ""
		    keys.some(function(key,index){
			  if(index == options.limit){
			       return true;
			  }

			  client.get(key,function(err,value){
			       if(err) 
				   return callback(err,'Error getting key');
			       else
				   message += "Key: "+key+" == "+value+"\n" 
			  })
		     })
		     client.quit();
                     return callback(null,message)
	    }
   })
}

var connector = function(callback){
     var client = redis.createClient(options.port,options.host);
     client.on('error',function(err){
             client.end()
	     return callback(err,'Error connecting to Redis')
     })
      client.on('ready',function(err){
             return callback(null,client)
     })
}

exports.setOptions=function(noptions){
      options.host = noptions.host? noptions.host:options.host
      options.port = noptions.port? noptions.port:options.port
      options.limit = noptions.limit? noptions.limit:options.limit
}

exports.commandParse=function(command,callback)
{
    var commands = ['help','collections','dbs','docs','']
    switch(command[1]){
       case 'help': 
                    help(callback); 
		    break;
       case 'status': 
                     getStatus(callback);
                      break;
       case 'keys':  connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Redis')
                         else
                             listKeys(con,callback)
                    })
                    break;
       case 'key':
                      if(!command[2]) 
                              return(callback(true,'Missing key name'))
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Redis')
                         else
                             dumpKey(con,command[2],callback)
                    })
                    break;
       case 'hkey':
                      if(!command[2]) 
                              return(callback(true,'Missing hkey name'))
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Redis')
                         else
                             dumpHkey(con,command[2],callback)
                    })
                    break;
       case 'dump':  
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Redis')
                         else
                             dump(con,callback)
                    })
                    break;
       default: return callback(true,'Command not found');
    }
}
