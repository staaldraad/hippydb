var thrift = require('thrift');
var util = require('util')
var HBase = require('./gen-nodejs/Hbase');
var HBaseTypes = require('./gen-nodejs/Hbase_types');

var options = {host:'127.0.0.1',port:9090,limit:10,type:0}
var connection = null
//type=1 -- FramedTransport
//type=0 -- BufferedTransport

var help = function(callback){
       var helpm = "HBase dumper -- Help\n"
           helpm += "Commands: dbs, collections, docs, status\n"
           helpm += "> hbase status                       //Check if we can connect to the database\n"
           helpm += "> hbase tables                       //list all tables on the host\n"
           helpm += "> hbase key  <key>                   //dump value of key\n"
           helpm += "> hbase hkey <hkey>                  //dump value of hkey\n"
           helpm += "> hbase dump                        //show 'limit' number of values.\n"

       return callback(null,helpm)
}

var getStatus = function(callback)
{
    connector(function(err,client){
         if(err)	 
           return callback(err,'Error connecting to Hive')
         
         return callback(null,'Connected! - Connection is good to go - ')
    })
}
var listTables = function(client,callback){
        client.getTableNames(function(err,data) {
		if (err) {
		      return callback(err,'gettablenames error:');
		} else {
		      var message = "Found ["+data.length+"] Tables\n"
                      data.forEach(function(table){
		          message += table.toString()+'\n'
                      });
                      connection.end()                 
                      return callback(null,message)
		}
	});
}

var connector = function(callback){
     if(options.type==0)
        connection = thrift.createConnection(options.host, options.port, { transport: thrift.TBufferedTransport, protocol:thrift.TBinaryProtocol });
     else
        connection = thrift.createConnection(options.host, options.port, { transport: thrift.TFramedTransport, protocol:thrift.TBinaryProtocol });

     connection.on('error',function(err){
	     return callback(err,'Error connecting to Redis')
     })
     connection.on('connect',function(){
             var client = thrift.createClient(HBase, connection);
             return callback(null,client)
     })
}

exports.setOptions=function(noptions){
      options.host = noptions.host? noptions.host:options.host
      options.port = noptions.port? noptions.port:options.port
      options.limit = noptions.limit? noptions.limit:options.limit
      options.type = noptions.type? noptions.type:options.type
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
       case 'tables':  connector(function(err,con){
                         if(err)
                             return callback(err,'Error connecting to Redis')
                         else
                             listTables(con,callback)
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

