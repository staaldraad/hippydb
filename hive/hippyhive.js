var thrift = require('thrift');
var HiveBase = require('./gen-nodejs/ThriftHive');
var FacebookBase = require('./gen-nodejs/FacebookService');

var options = {host:'127.0.0.1',port:10000,limit:10,type:0,timeout:1000}
exports.commands = ['help','status','dbs','tables','key','hkey','dump']

var connection = null
//type=1 -- FramedTransport
//type=0 -- BufferedTransport

var help = function(callback){
       var helpm = "Hive dumper -- Help\n"
           helpm += "Commands: \n"
           helpm += "> hive status                       //Check if we can connect to the database\n"
           helpm += "> hive dbs                          //list all tables on the host\n"
           helpm += "> hive tables                       //list all tables on the host\n"
           helpm += "> hive key  <key>                   //dump value of key\n"
           helpm += "> hive hkey <hkey>                  //dump value of hkey\n"
           helpm += "> hive dump                        //show 'limit' number of values.\n"

       return callback(null,helpm)
}

var getStatus = function(callback)
{
    connector(function(err,client){
         if(err)	 
           return callback(err,'Error connecting to Hive')
        
         client.getName(function(err,name){
                if(err)
                      return callback(null,'Connected! - Connection is good to go')
                
                return callback(null,'Connected! - Connection is good to go - Server name: '+name)
   	  });
 
    })
}
                       
var listDatabases= function(client,callback){
        client.get_all_databases(function(err,dbs){
                   if(err) 
		      return callback(err,'getdatabases error:');
                   var message = "Found ["+dbs.length+"] Databases\n"
                   dbs.forEach(function(db){
                           message += db+'\n'                        
                   })
                   connection.end()
                   return callback(null,message)
        })
}

var listTables= function(client,database,callback){
       client.get_all_tables(database,function(err,tables){
	   if(err) 
	      return callback(err,'gettablenames error:');
	   var message = "Found ["+tables.length+"] tables\n"
	   tables.forEach(function(table){
		   message += table+'\n'                        
	   })
	   connection.end()
	   return callback(null,message)
       })
}

var connector = function(callback){
     if(options.type==0)
        connection = thrift.createConnection(options.host, options.port, { transport: thrift.TBufferedTransport, protocol:thrift.TBinaryProtocol,timeout:options.timeout });
     else
        connection = thrift.createConnection(options.host, options.port, { transport: thrift.TFramedTransport, protocol:thrift.TBinaryProtocol,timeout:options.timeout });

     connection.on('error',function(err){
	     return callback(err,'Error connecting to Hive')
     })
     connection.on('connect',function(){
             var client = thrift.createClient(HiveBase, connection);
             return callback(null,client)
     })
}

exports.setOptions=function(noptions){
      options.host = noptions.host? noptions.host:options.host
      options.port = noptions.port? noptions.port:options.port
      options.limit = noptions.limit? noptions.limit:options.limit
      options.type = noptions.type? noptions.type:options.type
      options.timeout= noptions.timeout? noptions.timeout:options.timeout
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
       case 'dbs':  connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Hive')
                         else
                             listDatabases(con,callback)
                    })
                    break;
       case 'tables':
                      if(!command[2]) 
                              return(callback(true,'Missing database name'))
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Hive')
                         else
                             listTables(con,command[2],callback)
                    })
                    break;
       case 'dump':  
                      connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Hive')
                         else
                             dump(con,callback)
                    })
                    break;
       default: return callback(true,'Command not found');
    }
}

