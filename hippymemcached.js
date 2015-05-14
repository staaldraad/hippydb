/*
   NodeJS interface to memcache. Allows for extracting keys and specific values.
   Author: etienne@sensepost.com
   Version: 02/02/2015 v0.1
*/
var Memcached = require('memcached');
var options = {host:'127.0.0.1',port:11211,limit:10}
exports.commands = ['help','status','keys','slabs','dump']

var help = function(callback){
       var helpm = "Memcached DB dumper -- Help\n"
           helpm += "Commands: \n"
           helpm += "> memcached status                        //Check if we can connect to the database\n"
           helpm += "> memcached keys                          //this will list all databases on the host\n"
           helpm += "> memcached slabs                         //show all collections in a database\n"
           helpm += "> memcached dump                          //show 'x' number of values.\n"

       return callback(null,helpm)
}

var getStatus = function(callback)
{
    connector(function(con){
	 if(!con)
	     return callback(true,'Error connecting to memcached')
	 else
             return callback(null,'Connected! - Connection is good to go')
    })
}

var connector = function(callback){
    var memcached = new Memcached(options.host+':'+options.port);
    return callback(memcached)

}
exports.setOptions=function(noptions){
      options.host = noptions.host? noptions.host:options.host
      options.port = noptions.port? noptions.port:options.port
      options.limit = noptions.limit? noptions.limit:options.limit
}

var listKeys = function(memcached,callback)
{
     memcached.items(function(err, data){
            if(err) 
                return callback(err,'Error gettings keys'); 
            var message = ""
            data.forEach(function(itemSet){
                 var keys = Object.keys(itemSet);
                 keys.pop()
                 keys.forEach(function(stats){
                       memcached.cachedump(itemSet.server, parseInt(stats,10), itemSet[stats].number,function(err,result){                        
                             if(result&&result.length){
                                   result.some(function(res,index)
                                   {
                                       if(index==options.limit)
                                       {
                                          return true
                                       }
                                       message += res.key+'\n'
                                   })
                             }
                             else if(result)
                             {
                                  message += result.key+'\n'
                             }
                       })
                  })
            })
            return callback(null,message)
       })
}
var listSlabs= function(memcached,callback)
{
     memcached.items(function(err, data){
            if(err) 
                return callback(err,'Error gettings slabs'); 
            var message = ""
            data.forEach(function(itemSet){
                 var keys = Object.keys(itemSet);
                 keys.pop()
                 keys.forEach(function(stats){
                      message += itemSet[stats].number+'\n'
                  })
            })
            return callback(null,message)
       })
}

var dump = function(memcached,callback)
{
     memcached.items(function(err, data){
            if(err) 
                return callback(err,'Error gettings keys'); 
            var message = ""
            data.forEach(function(itemSet){
                 var keys = Object.keys(itemSet);
                 keys.pop()
                 keys.some(function(stats,index){
                       if(index>options.limit)
                                return true
                       memcached.cachedump(itemSet.server, parseInt(stats,10), itemSet[stats].number,function(err,result){                        
                             if(result&&result.length){
                                   result.some(function(res,index)
                                   {
                                       if(index==options.limit)
                                       {
                                          return true
                                       }
                                       memcached.get(res.key,function(err,value){
                                             if(value) 
                                                  message += value+'\n'
                                       })
                                   })
                             }
                             else if(result)
                             {
                                   memcached.get(result.key,function(err,value){
                                       if(value) 
                                           message += value+'\n'
                                  })
                             }
                       })
                  })
            })
            return callback(null,message)
       })
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
       case 'keys':  connector(function(con){
                         if(!con)
                             return callback(true,'Error connecting to memcached')
                         else
                             listKeys(con,callback)
                    })
                    break;
       case 'slabs':
                      connector(function(con){
                         if(!con)
                             return callback(true,'Error connecting to memcached')
                         else
                             listSlabs(con,callback)
                    })
                    break;
       case 'dump':  
                      connector(function(con){
                         if(!con)
                             return callback('Error connecting to memcached')
                         else
                             dump(con,callback)
                    })
                    break;
       default: return callback(true,'Command not found');
    }
}



